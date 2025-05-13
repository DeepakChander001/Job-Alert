import asyncio
from playwright.async_api import async_playwright
import json, csv
import time 
import os
import logging
import subprocess
from supabase import create_client, Client
from dotenv import load_dotenv
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
import uvicorn
from openai import OpenAI

# Load environment variables
load_dotenv()

# Load credentials
linkedin_email = os.getenv('LINKEDIN_EMAIL')
linkedin_password = os.getenv('LINKEDIN_PASSWORD')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize Supabase client
def init_supabase():
    try:
        supabase_url = os.getenv('SUPABASE_URL')
        supabase_key = os.getenv('SUPABASE_KEY')
        if not supabase_url or not supabase_key:
            raise ValueError("Missing Supabase credentials in .env file")
        
        supabase: Client = create_client(supabase_url, supabase_key)
        logger.info("✅ Supabase client initialized successfully")
        return supabase
    except Exception as e:
        logger.error(f"❌ Error initializing Supabase client: {str(e)}")
        raise

# Store jobs in Supabase
async def store_jobs_in_supabase(supabase: Client, jobs_data):
    try:
        # Clear the table before inserting new data
        supabase.table('scraping job').delete().neq('Job_URL', '').execute()

        # Prepare data for insertion
        jobs_to_insert = []
        for job in jobs_data:
            jobs_to_insert.append({
                'Job_Title': job.get('Job_Title', ''),
                'Company_Name': job.get('Company_Name', ''),
                'Location': job.get('Location', ''),
                'Job_URL': job.get('Job_URL', ''),
                'Job_Description': job.get('Job_Description', ''),
                'Company_Logo': job.get('Company_Logo', ''),
                'Company_URL': job.get('Company_URL', '')
            })

        # Insert data in batches
        batch_size = 100
        for i in range(0, len(jobs_to_insert), batch_size):
            batch = jobs_to_insert[i:i + batch_size]
            supabase.table('scraping job').insert(batch).execute()
            logger.info(f"✅ Inserted batch {i//batch_size + 1} of {len(jobs_to_insert)//batch_size + 1}")

        logger.info(f"✅ Successfully stored {len(jobs_data)} jobs in Supabase")
    except Exception as e:
        logger.error(f"❌ Error storing jobs in Supabase: {str(e)}")
        raise

# Start Chrome with remote debugging
def start_chrome_with_profile():
    try:
        logger.info("🔵 Checking if Chrome needs to be launched with remote debugging...")
        subprocess.Popen([
            r"C:\Program Files\Google\Chrome\Application\chrome.exe",
            "--remote-debugging-port=9222",
            '--user-data-dir=C:\\chrome-remote-profile',
            '--profile-directory=Profile 3'
        ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        logger.info("✅ Chrome launched successfully with remote debugging enabled!")
        logger.info("⏳ Waiting 5 seconds for Chrome to initialize...")
        time.sleep(5)

    except Exception as e:
        logger.error(f"❌ Error launching Chrome: {str(e)}")
        raise

# Extract job details from job listing page
async def extract_job_details(page):
    result = {
        "Job_Description": "❌ Not Found",
        "Company_Logo": "❌ Not Found",
        "Company_URL": "❌ Not Found",
        "Job_Type": "Not Disclosed"
    }

    for attempt in range(3):
        try:
            await page.wait_for_selector("div.jobs-description__content", timeout=30000)

            # Extract Job Type with improved logic
            try:
                # Try multiple selectors for job type
                job_type_selectors = [
                    "div.job-details-preferences-and-skills__pill span.ui-label",
                    "div.job-details-preferences-and-skills__pill span.job-details-preferences-and-skills__pill-text",
                    "div.job-details-preferences-and-skills__pill span"
                ]
                
                for selector in job_type_selectors:
                    job_type_element = page.locator(selector)
                    if await job_type_element.is_visible():
                        job_type = await job_type_element.inner_text()
                        job_type = job_type.strip()
                        
                        # Check if job type is one of the specified types
                        valid_types = ["Hybrid", "Remote", "Onsite"]
                        if job_type in valid_types:
                            result["Job_Type"] = job_type
                            break
            except Exception:
                pass

            # Extract Job Description
            selectors = [
                "div#job-details span p",
                "div.jobs-description__content div.jobs-box__html-content",
                "div.jobs-description__content div.mt4",
                "div.jobs-description__content div.mt4 p",
                "div.jobs-description__content div.mt4 ul li"
            ]
            description_parts = []

            for selector in selectors:
                try:
                    elements = await page.locator(selector).all()
                    for element in elements:
                        content = await element.inner_text()
                        if content.strip():
                            description_parts.append(content.strip())
                except Exception:
                    continue

            if description_parts:
                result["Job_Description"] = "\n\n".join(description_parts)

            # Extract Company Logo with improved logic
            try:
                # Try multiple selectors for company logo
                logo_selectors = [
                    "a.link-without-hover-state.inline-block img.evi-image",
                    "img.evi-image",
                    "img.artdeco-entity-image"
                ]
                
                for selector in logo_selectors:
                    logo_element = page.locator(selector)
                    if await logo_element.is_visible():
                        logo_url = await logo_element.get_attribute('src')
                        if logo_url:
                            result["Company_Logo"] = logo_url
                            break
            except Exception:
                pass

            # Extract Company URL with improved logic
            try:
                # Try multiple selectors for company URL
                url_selectors = [
                    "a.link-without-hover-state.inline-block",
                    "a.ember-view.org-top-card-primary-actions__action",
                    "a.org-top-card-primary-actions__action"
                ]
                
                for selector in url_selectors:
                    url_element = page.locator(selector)
                    if await url_element.is_visible():
                        relative_url = await url_element.get_attribute('href')
                        if relative_url:
                            result["Company_URL"] = "https://www.linkedin.com" + relative_url
                            break
            except Exception:
                pass

            break  # Success, exit retry loop
        except Exception as e:
            if attempt == 2:
                logger.error(f"Error extracting job details after 3 attempts: {str(e)}")
            else:
                logger.warning(f"Retrying job details extraction ({attempt+1}/3)...")
                await page.wait_for_timeout(2000)

    return result

# Scrape a single job's details
async def scrape_job_details(semaphore, context, job, index, total_jobs):
    async with semaphore:
        try:
            page = await context.new_page()
            logger.info(f"🔍 Scraping details for {index}/{total_jobs}: {job['Job_Title']} at {job['Company_Name']}")
            await page.goto(job['Job_URL'], timeout=60000)
            await page.wait_for_timeout(3000)
            details = await extract_job_details(page)
            job.update(details)
            await page.close()
        except Exception as e:
            logger.error(f"⚠️ Failed to scrape details for {job.get('Job_Title', 'Unknown')}: {str(e)}")
            job.update({
                "Job_Description": "❌ Failed to Load Description",
                "Company_Logo": "❌ Failed",
                "Company_URL": "❌ Failed"
            })

async def extract_skills_and_roles(description):
    prompt = (
        f"Given the following job description, extract:\n"
        f"1. The required skills (as a comma-separated list)\n"
        f"2. The main roles and responsibilities (as a short paragraph)\n\n"
        f"Job Description:\n{description}\n\n"
        f"Return your answer in this JSON format:\n"
        f'{{"skills_requirement": "...", "roles_and_responsibility": "..."}}'
    )
    try:
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2
        )
        json_str = response.choices[0].message.content
        data = json.loads(json_str)
        return data.get("skills_requirement", ""), data.get("roles_and_responsibility", "")
    except Exception as e:
        logger.error(f"OpenAI API error: {e}")
        return "", ""

async def scrape_linkedin_jobs():
    supabase = init_supabase()
    async with async_playwright() as p:
        browser = await p.chromium.connect_over_cdp("http://127.0.0.1:9222")
        if not browser.contexts:
            context = await browser.new_context()
        else:
            context = browser.contexts[0]
        page = await context.new_page()
        jobs_url = "https://www.linkedin.com/jobs/search-results/?f_TPR=r86400&geoId=103644278&keywords=data%20analyst&origin=SWITCH_SEARCH_VERTICAL"
        logger.info("Navigating to LinkedIn jobs page...")
        await page.goto(jobs_url)
        # Login if needed
        if "login" in page.url:
            logger.info("Not logged in, performing automatic login...")
            await page.wait_for_selector('input[name="session_key"]', timeout=30000)
            await page.fill('input[name="session_key"]', linkedin_email)
            await page.fill('input[name="session_password"]', linkedin_password)
            await page.click('button[type=submit]')
            await page.wait_for_selector("input.search-global-typeahead__input", timeout=60000)
            logger.info("✅ Logged in successfully!")
            await page.goto(jobs_url)
        else:
            logger.info("Already logged in!")
        # Wait for job listings
        logger.info("Waiting for jobs to load...")
        await page.wait_for_selector("div.artdeco-entity-lockup__title", timeout=30000)
        all_job_data = []
        current_page = 1
        max_pages = 10
        while current_page <= max_pages:
            logger.info(f"Scraping page {current_page}...")
            # Scroll to load more jobs
            for _ in range(5):
                await page.mouse.wheel(0, 3000)
                await page.wait_for_timeout(1000)
            # Get job titles, companies, locations, and URLs
            titles = []
            title_elements = await page.locator("div.artdeco-entity-lockup__title span[aria-hidden='true'] strong").all()
            for element in title_elements:
                title = await element.inner_text()
                titles.append(title.strip())
            companies = []
            company_elements = await page.locator("div.artdeco-entity-lockup__subtitle div[dir='ltr']").all()
            for element in company_elements:
                company = await element.inner_text()
                companies.append(company.strip())
            locations = []
            location_elements = await page.locator("div.artdeco-entity-lockup__caption div[dir='ltr']").all()
            for element in location_elements:
                location = await element.inner_text()
                if location.strip():
                    locations.append(location.strip())
            urls = await page.locator("div.artdeco-entity-lockup__title").evaluate_all("""
                els => els.map(el => {
                    const link = el.closest('a');
                    return link ? link.href : '';
                })
            """)
            for title, company, location, url in zip(titles, companies, locations, urls):
                all_job_data.append({
                    "Job_Title": title,
                    "Company_Name": company,
                    "Location": location,
                    "Job_URL": url
                })
            logger.info(f"✅ Found {len(titles)} jobs on page {current_page}")
            # Next page
            next_button = page.locator("button.jobs-search-pagination__button--next")
            if not await next_button.is_visible() or current_page >= max_pages:
                break
            await next_button.click()
            await page.wait_for_timeout(2000)
            current_page += 1
        logger.info(f"✅ Total jobs found: {len(all_job_data)}")
        # Scrape details for each job
        logger.info("Starting to scrape job details...")
        semaphore = asyncio.Semaphore(3)
        tasks = []
        for index, job in enumerate(all_job_data, 1):
            tasks.append(scrape_job_details(semaphore, context, job, index, len(all_job_data)))
        await asyncio.gather(*tasks)
        # Store in Supabase
        await store_jobs_in_supabase(supabase, all_job_data)
        logger.info(f"✅ All jobs stored in Supabase. No local backup files created.")
        await page.close()
        await context.close()
        await browser.close()

app = FastAPI()

@app.get("/")
async def run_scraper(keyword: str = Query("data analyst")):
    try:
        start_chrome_with_profile()
        await scrape_linkedin_jobs()
        return JSONResponse(content={"status": "success", "message": "Scraping completed successfully"})
    except Exception as e:
        import traceback
        print(traceback.format_exc())
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)

# Only run this if being served directly (not when imported)
if __name__ == "__main__":
    uvicorn.run("scrapingLDO:app", host="0.0.0.0", port=8000) 