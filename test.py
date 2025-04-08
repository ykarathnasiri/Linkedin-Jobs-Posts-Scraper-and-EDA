import requests 
from bs4 import BeautifulSoup
import math
import pandas as pd
from datetime import datetime
import logging
import time
import os
import random
from pathlib import Path
from tqdm import tqdm
from itertools import product

# Configure logging
log_filename = 'linkedin_scraper.log'
try:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, 'a', 'utf-8'),
            logging.StreamHandler()
        ]
    )
except PermissionError:
    temp_log = os.path.join(os.path.expanduser('~'), 'linkedin_scraper.log')
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(temp_log, 'a', 'utf-8'),
            logging.StreamHandler()
        ]
    )

logger = logging.getLogger(__name__)

# Multiple user agents to rotate
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0"
]

# Sort options and time filters
SORT_OPTIONS = {
    'relevant': 'R',  # Most relevant
    'recent': 'DD',   # Most recent
    'applied': 'A'    # Most applied
}

TIME_FILTERS = {
    '24h': '1',
    'week': '1,2,3,4,5,6,7',
    'month': '1,2,3,4',
    'any': ''
}

def get_random_headers():
    """Get random headers to avoid detection"""
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Referer": "https://www.linkedin.com/",
        "Connection": "keep-alive"
    }

def extract_job_data(card, sort_method, time_filter):
    """Extract data from a job card"""
    try:
        job_data = {}
        
        # Get job link and ID
        job_link = card.find("a", {"class": "base-card__full-link"})
        if not job_link:
            return None
            
        job_url = job_link.get('href').split('?')[0]
        job_id = job_url.split('-')[-1]
        
        # Extract basic info from card
        title_elem = card.find("h3", {"class": "base-search-card__title"})
        company_elem = card.find("h4", {"class": "base-search-card__subtitle"})
        location_elem = card.find("span", {"class": "job-search-card__location"})
        time_elem = card.find("time")
        
        job_data.update({
            "job_id": job_id,
            "title": title_elem.text.strip() if title_elem else None,
            "company": company_elem.text.strip() if company_elem else None,
            "location": location_elem.text.strip() if location_elem else None,
            "posted_date": time_elem.get("datetime") if time_elem else None,
            "job_url": job_url,
            "sort_method": sort_method,
            "time_filter": time_filter
        })
        
        # Get detailed info
        details = get_job_details(job_id)
        if details:
            job_data.update(details)
        
        return job_data
        
    except Exception as e:
        logger.error(f"Error extracting job data: {str(e)}")
        return None

def get_job_details(job_id):
    """Get detailed job information"""
    url = f"https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{job_id}"
    try:
        response = requests.get(url, headers=get_random_headers())
        
        if response.status_code != 200:
            return {}

        soup = BeautifulSoup(response.text, 'html.parser')
        
        details = {}
        
        # Extract description
        desc_element = soup.find("div", {"class": "show-more-less-html__markup"})
        if desc_element:
            details["description"] = desc_element.get_text(strip=True)

        # Extract job criteria
        criteria_list = soup.find("ul", {"class": "description__job-criteria-list"})
        if criteria_list:
            for item in criteria_list.find_all("li"):
                header = item.find("h3", {"class": "description__job-criteria-subheader"})
                if header:
                    header_text = header.text.strip()
                    value = item.find("span", {"class": "description__job-criteria-text"})
                    if value:
                        value_text = value.text.strip()
                        if "Seniority level" in header_text:
                            details["experience_level"] = value_text
                        elif "Employment type" in header_text:
                            details["employment_type"] = value_text
                        elif "Job function" in header_text:
                            details["job_function"] = value_text
                        elif "Industries" in header_text:
                            details["industries"] = value_text

        # Additional fields
        details.update({
            "salary": None,  # LinkedIn rarely shows salary
            "required_skills": None,  # Skills are often in description
            "company_size": None,
            "company_industry": None,
            "applicant_count": None
        })

        return details

    except Exception as e:
        logger.error(f"Error fetching details for job {job_id}: {str(e)}")
        return {}

def scrape_jobs_with_filters(location="Sri Lanka", jobs_per_combination=400):
    """Scrape jobs using different sort options and time filters"""
    all_jobs_data = []
    
    for sort_name, sort_value in SORT_OPTIONS.items():
        for filter_name, filter_value in TIME_FILTERS.items():
            logger.info(f"Scraping with sort: {sort_name}, filter: {filter_name}")
            
            base_url = (
                f"https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?"
                f"location={location}&sortBy={sort_value}&f_TPR={filter_value}&start={{}}"
            )
            
            pages = math.ceil(jobs_per_combination / 25)
            
            with tqdm(total=jobs_per_combination, 
                     desc=f"Sort: {sort_name}, Filter: {filter_name}") as pbar:
                
                for page in range(pages):
                    try:
                        url = base_url.format(page * 25)
                        response = requests.get(url, headers=get_random_headers())
                        
                        if response.status_code == 429:
                            logger.warning("Rate limited. Waiting...")
                            time.sleep(60 + random.randint(0, 30))
                            continue
                            
                        if response.status_code != 200:
                            continue

                        soup = BeautifulSoup(response.text, 'html.parser')
                        job_cards = soup.find_all("div", {"class": "base-card"})
                        
                        if not job_cards:
                            break
                            
                        for card in job_cards:
                            job_data = extract_job_data(card, sort_name, filter_name)
                            if job_data:
                                all_jobs_data.append(job_data)
                                pbar.update(1)
                                
                        time.sleep(random.uniform(2, 5))
                        
                    except Exception as e:
                        logger.error(f"Error on page {page}: {str(e)}")
                        continue
                        
            # Longer pause between different search combinations
            time.sleep(random.uniform(10, 15))
            
            # Save progress after each combination
            if all_jobs_data:
                save_to_csv(all_jobs_data)
                
    return all_jobs_data

def save_to_csv(jobs_data, filename=None):
    """Save or append the scraped data to a single CSV file"""
    if not jobs_data:
        return False
        
    data_dir = Path('data')
    data_dir.mkdir(exist_ok=True)
    
    # Use provided filename or create one if it doesn't exist
    if filename is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = data_dir / f"linkedin_jobs_{timestamp}.csv"
    
    try:
        df = pd.DataFrame(jobs_data)
        columns = [
            "job_id", "title", "company", "location", "experience_level",
            "employment_type", "posted_date", "job_function", "industries",
            "salary", "required_skills", "description", "company_size",
            "company_industry", "applicant_count", "job_url", "sort_method",
            "time_filter"
        ]
        df = df.reindex(columns=columns)
        
        # If file exists, append without headers
        if os.path.exists(filename):
            df.to_csv(filename, mode='a', header=False, index=False, encoding='utf-8')
        else:
            # If file doesn't exist, create with headers
            df.to_csv(filename, index=False, encoding='utf-8')
            
        logger.info(f"Data saved/appended to {filename}")
        return True
    except Exception as e:
        logger.error(f"Error saving data: {str(e)}")
        return False

def scrape_jobs_with_filters(location="Sri Lanka", jobs_per_combination=1000):
    """Scrape jobs using different sort options and time filters"""
    # Create single output file name at start
    data_dir = Path('data')
    data_dir.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = data_dir / f"linkedin_jobs_{timestamp}.csv"
    
    jobs_batch = []  # Buffer for batch saving
    batch_size = 50  # Save every 50 jobs
    
    for sort_name, sort_value in SORT_OPTIONS.items():
        for filter_name, filter_value in TIME_FILTERS.items():
            logger.info(f"Scraping with sort: {sort_name}, filter: {filter_name}")
            
            base_url = (
                f"https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?"
                f"location={location}&sortBy={sort_value}&f_TPR={filter_value}&start={{}}"
            )
            
            pages = math.ceil(jobs_per_combination / 25)
            
            with tqdm(total=jobs_per_combination, 
                     desc=f"Sort: {sort_name}, Filter: {filter_name}") as pbar:
                
                for page in range(pages):
                    try:
                        url = base_url.format(page * 25)
                        response = requests.get(url, headers=get_random_headers())
                        
                        if response.status_code == 429:
                            logger.warning("Rate limited. Waiting...")
                            time.sleep(60 + random.randint(0, 30))
                            continue
                            
                        if response.status_code != 200:
                            continue

                        soup = BeautifulSoup(response.text, 'html.parser')
                        job_cards = soup.find_all("div", {"class": "base-card"})
                        
                        if not job_cards:
                            break
                            
                        for card in job_cards:
                            job_data = extract_job_data(card, sort_name, filter_name)
                            if job_data:
                                jobs_batch.append(job_data)
                                pbar.update(1)
                                
                                # Save batch when it reaches the batch size
                                if len(jobs_batch) >= batch_size:
                                    save_to_csv(jobs_batch, output_file)
                                    jobs_batch = []  # Clear batch after saving
                                
                        time.sleep(random.uniform(2, 5))
                        
                    except Exception as e:
                        logger.error(f"Error on page {page}: {str(e)}")
                        # Save any remaining jobs in batch if there's an error
                        if jobs_batch:
                            save_to_csv(jobs_batch, output_file)
                            jobs_batch = []
                        continue
                        
            # Save any remaining jobs in batch after each filter combination
            if jobs_batch:
                save_to_csv(jobs_batch, output_file)
                jobs_batch = []
                
            # Longer pause between different search combinations
            time.sleep(random.uniform(10, 15))
                
    return output_file

def main():
    try:
        while True:
            try:
                jobs_per_combination = input("Enter number of jobs to scrape per combination (default 400): ").strip()
                jobs_per_combination = int(jobs_per_combination) if jobs_per_combination else 400 # LinkedIn only load 40 pages and one page include around 10 Job cards
                if jobs_per_combination > 0:
                    break
                print("Please enter a positive number")
            except ValueError:
                print("Please enter a valid number")

        logger.info("Starting comprehensive LinkedIn job scraping")
        
        output_file = scrape_jobs_with_filters(jobs_per_combination=jobs_per_combination)
        
        # Count total jobs in file
        try:
            df = pd.read_csv(output_file)
            total_jobs = len(df)
            print(f"\nSuccessfully scraped {total_jobs} jobs")
            print(f"Data saved to {output_file}")
        except Exception as e:
            logger.error(f"Error reading final file: {str(e)}")
            print("Error reading final file. Check the log for details.")

    except KeyboardInterrupt:
        print("\nScraping interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")

if __name__ == "__main__":
    main()