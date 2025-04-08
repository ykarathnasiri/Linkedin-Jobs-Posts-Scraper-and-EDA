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

# IT job titles and keywords, categorized by job type
IT_JOB_CATEGORIES = {
    'software_development': [
        'software developer', 'software engineer', 'programmer', 'coder', 'full stack', 'frontend',
        'backend', 'mobile developer', 'ios developer', 'android developer', 'web developer',
        'javascript developer', 'python developer', 'java developer', 'php developer', 'ruby developer',
        '.net developer', 'c# developer', 'c++ developer', 'scala developer', 'go developer', 'golang',
        'react developer', 'angular developer', 'vue developer', 'node.js developer', 'typescript',
        'flutter developer', 'kotlin developer', 'swift developer', 'rust developer', 'elm developer',
        'clojure developer', 'haskell developer', 'elixir developer', 'erlang developer'
    ],
    'data_science': [
        'data scientist', 'data analyst', 'business intelligence', 'bi developer', 'machine learning',
        'ml engineer', 'ai engineer', 'artificial intelligence', 'nlp', 'natural language processing',
        'computer vision', 'deep learning', 'statistical analyst', 'big data', 'data engineer',
        'data architect', 'etl developer', 'analytics', 'data mining', 'predictive modeling',
        'tableau developer', 'power bi developer', 'data visualization', 'statistician', 'r developer'
    ],
    'cloud_devops': [
        'cloud engineer', 'cloud architect', 'devops engineer', 'site reliability engineer', 'sre',
        'infrastructure engineer', 'aws', 'azure', 'gcp', 'google cloud', 'cloud native', 'kubernetes',
        'docker', 'containerization', 'ci/cd', 'jenkins', 'terraform', 'ansible', 'chef', 'puppet',
        'microservices', 'service mesh', 'cloud migration', 'cloud optimization', 'cloud security'
    ],
    'cybersecurity': [
        'security engineer', 'security analyst', 'cybersecurity', 'cyber security', 'information security',
        'infosec', 'penetration tester', 'pen tester', 'ethical hacker', 'security consultant',
        'security architect', 'security administrator', 'security operations', 'soc analyst',
        'threat intelligence', 'vulnerability assessment', 'devsecops', 'security compliance',
        'security auditor', 'cryptography', 'encryption', 'risk management', 'threat modeling'
    ],
    'network_systems': [
        'network engineer', 'network administrator', 'systems administrator', 'sysadmin', 'systems engineer',
        'network architect', 'network security', 'cisco', 'juniper', 'ccna', 'ccnp', 'ccie',
        'telecommunications', 'voip', 'wan', 'lan', 'virtualization', 'vmware', 'hyper-v',
        'storage administrator', 'backup administrator', 'datacenter'
    ],
    'database': [
        'database administrator', 'dba', 'database developer', 'database architect', 'sql developer',
        'oracle developer', 'mysql developer', 'postgresql developer', 'mongodb developer', 'nosql',
        'sql server', 'oracle', 'mysql', 'postgresql', 'mongodb', 'cassandra', 'redis', 'elasticsearch',
        'database engineer', 'database security', 'data modeling', 'data warehousing'
    ],
    'management_analysis': [
        'it project manager', 'technical project manager', 'it manager', 'scrum master', 'agile coach',
        'product manager', 'product owner', 'it director', 'vp of engineering', 'cto', 'cio',
        'technical program manager', 'technology officer', 'it coordinator', 'business analyst',
        'systems analyst', 'technology analyst', 'it consultant', 'solutions architect', 'enterprise architect'
    ],
    'qa_testing': [
        'qa engineer', 'quality assurance', 'test engineer', 'software tester', 'test analyst',
        'automation engineer', 'manual tester', 'test lead', 'quality engineer', 'test manager',
        'performance tester', 'load tester', 'security tester', 'test architect', 'qa analyst',
        'automation tester', 'selenium', 'appium', 'cypress', 'playwright', 'test automation'
    ],
    'support_helpdesk': [
        'it support', 'technical support', 'help desk', 'helpdesk', 'desktop support', 'service desk',
        'it technician', 'computer technician', 'support specialist', 'it support specialist',
        'support analyst', 'it support analyst', 'field technician', 'tier 1 support', 'tier 2 support',
        'tier 3 support', 'end user support', 'it support engineer'
    ],
    'ui_ux': [
        'ui designer', 'ux designer', 'ui/ux designer', 'user interface', 'user experience',
        'ux researcher', 'interaction designer', 'visual designer', 'product designer', 'web designer',
        'mobile designer', 'ui developer', 'frontend designer', 'ux writer', 'information architect',
        'usability specialist', 'ui architect', 'ux architect', 'ux manager'
    ],
    'emerging_tech': [
        'blockchain developer', 'blockchain engineer', 'ar developer', 'vr developer', 'xr developer',
        'augmented reality', 'virtual reality', 'game developer', 'unity developer', 'unreal developer',
        'iot developer', 'internet of things', 'embedded systems', 'firmware engineer', 'robotic engineer',
        'quantum computing', 'edge computing', '5g', 'crypto', 'metaverse', 'digital twin'
    ]
}

# Flatten the categories into a single list of IT keywords
IT_KEYWORDS = [keyword for category in IT_JOB_CATEGORIES.values() for keyword in category]

# Also include individual tech keywords that might appear in job titles or descriptions
TECH_KEYWORDS = [
    'python', 'java', 'javascript', 'js', 'html', 'css', 'sql', 'nosql', 'aws', 'azure', 'gcp',
    'react', 'angular', 'vue', 'node', 'express', 'django', 'flask', 'spring', 'hibernate',
    'kubernetes', 'docker', 'jenkins', 'gitlab', 'github', 'git', 'terraform', 'ansible', 'chef',
    'hadoop', 'spark', 'kafka', 'elasticsearch', 'redis', 'mongodb', 'postgresql', 'mysql', 'oracle',
    'cybersecurity', 'networking', 'linux', 'unix', 'windows', 'cisco', 'juniper', 'firebase',
    'serverless', 'microservices', 'api', 'rest', 'graphql', 'oauth', 'saml', 'sso', 'ldap',
    'active directory', 'jira', 'confluence', 'slack', 'teams', 'sap', 'tableau', 'power bi',
    'excel', 'sharepoint', 'servicenow', 'salesforce', 'dynamics', 'wordpress', 'drupal', 'magento',
    'woocommerce', 'shopify', 'analytics', 'seo', 'sem', 'crm', 'erp', 'wasm', 'webassembly',
    'typescript', 'kotlin', 'swift', 'rust', 'go', 'golang', 'php', 'laravel', 'symfony', 'selenium',
    'appium', 'cypress', 'jest', 'mocha', 'jasmine', 'junit', 'testng', 'cucumber', 'agile', 'scrum',
    'kanban', 'devops', 'devsecops', 'ci/cd', 'penetration testing', 'wireshark', 'nmap', 'metasploit',
    'burp suite', 'kali linux', 'mobile development', 'android', 'ios', 'flutter', 'react native',
    'xamarin', 'cordova', 'ionic', 'unity', 'unreal engine', 'game development', 'ar', 'vr', 'xr',
    'blockchain', 'cryptocurrency', 'smart contracts', 'solidity', 'ethereum', 'hyperledger', 'web3',
    'computer vision', 'nlp', 'neural networks', 'deep learning', 'tensorflow', 'pytorch', 'keras',
    'scikit-learn', 'pandas', 'numpy', 'matplotlib', 'data visualization', 'data engineering',
    'etl', 'data warehousing', 'data mining', 'data modeling', 'cloud computing', 'saas', 'paas', 'iaas',
    'vcenter', 'esxi', 'virtualization', 'vmware', 'hyper-v', 'xen', 'kvm', 'openstack', 'openshift',
    'low code', 'no code', 'power platform', 'power apps', 'power automate', 'mendix', 'outsystems',
    'computer networks', 'tcp/ip', 'dns', 'dhcp', 'vpn', 'wan', 'lan', 'mpls', 'sdwan', 'firewalls',
    'load balancers', 'proxies', 'reverse proxies', 'web servers', 'nginx', 'apache', 'iis',
    'tomcat', 'websphere', 'weblogic', 'jboss', 'wildfly', 'glassfish', 'database design', 'orm',
    'jdbc', 'odbc', 'ai', 'machine learning', 'algorithms', 'data structures', 'web services',
    'soap', 'json', 'xml', 'yaml', 'computer science', 'information systems', 'information technology',
    'computer engineering', 'software engineering', 'agile methodologies', 'scrum', 'kanban', 'lean',
    'project management', 'program management', 'pmp', 'prince2', 'itil', 'cobit', 'iso27001',
    'gdpr', 'hipaa', 'pci-dss', 'sox', 'compliance', 'auditing', 'risk management', 'disaster recovery',
    'business continuity', 'backup', 'recovery', 'high availability', 'fault tolerance', 'monitoring',
    'logging', 'alerting', 'observability', 'apm', 'splunk', 'elk', 'grafana', 'prometheus', 'nagios',
    'zabbix', 'datadog', 'newrelic', 'dynatrace', 'app dynamics', 'siem', 'soar', 'soc', 'threat hunting',
    'incident response', 'forensics', 'malware analysis', 'cryptography', 'encryption', 'vpn', 'ssl',
    'tls', 'ssh', 'sftp', 'ftps', 'dns', 'dhcp', 'ddos', 'waf', 'ids', 'ips', 'dlp', 'xdr', 'edr',
    'mdr', 'sase', 'zero trust', 'ztna', 'casb', 'ciem', 'cnapp', 'cwpp', 'cspm', 'sast', 'dast',
    'iast', 'rasp', 'appsec', 'devsecops', 'secops', 'shift left', 'supply chain security', 'quantum computing',
    'quantum cryptography', 'quantum networks', 'quantum communications', 'quantum algorithms', 'quantum ml',
    'quantum ai', 'quantum internet', 'quantum error correction', 'quantum supremacy', 'quantum advantage',
    'quantum computing', 'quantum', 'edge computing', 'fog computing', 'distributed computing', 'parallel computing',
    'grid computing', 'high performance computing', 'hpc', 'supercomputing', 'compute', 'storage', 'networking',
    'iot', 'internet of things', 'iiot', 'industrial iot', 'smart cities', 'smart homes', 'smart buildings',
    'smart grid', 'smart meters', 'smart sensors', 'embedded systems', 'firmware', 'microcontrollers',
    'microprocessors', 'fpga', 'asic', 'gpu', 'tpu', 'vpu', 'npu', 'dpu', 'accelerators', 'hardware',
    'robotics', 'robotic process automation', 'rpa', 'computer vision', 'image processing', 'image recognition',
    'facial recognition', 'object detection', 'object recognition', 'object tracking', 'optical character recognition',
    'ocr', 'document processing', 'document understanding', 'document automation', 'intelligent document processing',
    'idp', 'intelligent automation', 'hyperautomation', 'digital transformation', 'digitization', 'digitalization',
    'digital adoption', 'digital strategy', 'digital workplace', 'digital workplace transformation', 'digital workplace strategy',
    'digital workplace adoption', 'digital workplace experience', 'digital employee experience', 'dex', 'employee experience',
    'ex', 'user experience', 'ux', 'user interface', 'ui', 'user research', 'user testing', 'usability testing',
    'accessibility', 'a11y', 'wcag', 'ada', 'section 508', 'aoda', 'eaa', 'aria', 'screen readers', 'voice assistants',
    'virtual assistants', 'chatbots', 'conversational ai', 'conversational interfaces', 'voice interfaces', 'voice ui',
    'voice ux', 'voice design', 'voice development', 'voice apps', 'voice skills', 'voice actions', 'voice analytics',
    'speech recognition', 'speech synthesis', 'speech processing', 'speech analysis', 'speech analytics', 'nlp',
    'natural language processing', 'natural language understanding', 'nlu', 'natural language generation', 'nlg',
    'text analysis', 'text analytics', 'text mining', 'sentiment analysis', 'entity recognition', 'ner', 'topic modeling',
    'text classification', 'text clustering', 'text summarization', 'machine translation', 'neural machine translation',
    'nmt', 'statistical machine translation', 'smt', 'machine learning', 'ml', 'deep learning', 'dl', 'neural networks',
    'neural nets', 'artificial neural networks', 'ann', 'convolutional neural networks', 'cnn', 'recurrent neural networks',
    'rnn', 'long short-term memory', 'lstm', 'gated recurrent units', 'gru', 'transformers', 'attention mechanisms',
    'sequence to sequence', 'seq2seq', 'generative adversarial networks', 'gan', 'reinforcement learning', 'rl',
    'deep reinforcement learning', 'drl', 'federated learning', 'transfer learning', 'meta learning', 'one-shot learning',
    'few-shot learning', 'zero-shot learning', 'self-supervised learning', 'ssl', 'semi-supervised learning', 'active learning',
    'ensemble learning', 'boosting', 'bagging', 'random forests', 'gradient boosting', 'xgboost', 'lightgbm',
    'catboost', 'decision trees', 'support vector machines', 'svm', 'naive bayes', 'k-nearest neighbors', 'knn',
    'clustering', 'k-means', 'hierarchical clustering', 'dbscan', 'dimensionality reduction', 'pca', 'lda', 't-sne',
    'umap', 'recommender systems', 'collaborative filtering', 'content-based filtering', 'hybrid recommender systems',
    'matrix factorization', 'svd', 'anomaly detection', 'outlier detection', 'time series analysis', 'time series forecasting',
    'predictive analytics', 'predictive modeling', 'regression', 'classification', 'feature engineering', 'feature selection',
    'feature extraction', 'data preprocessing', 'data cleaning', 'data wrangling', 'data preparation', 'data quality',
    'data governance', 'data management', 'data strategy', 'data ops', 'data lake', 'data lakehouse', 'data warehouse',
    'data mesh', 'data fabric', 'data virtualization', 'data catalog', 'data lineage', 'data discovery', 'data profiling',
    'data quality', 'data observability', 'data versioning', 'data pipelines', 'data integration', 'data transformation',
    'data migration', 'data replication', 'data synchronization', 'data curation', 'data modeling', 'data architecture',
    'data platform', 'data infrastructure', 'big data', 'small data', 'dark data', 'data analytics', 'business analytics',
    'business intelligence', 'bi', 'olap', 'oltp', 'etl', 'extract transform load', 'elt', 'extract load transform',
    'data engineering', 'software engineering', 'computer science', 'cs', 'information technology', 'it', 'information systems',
    'is', 'information science', 'informatics', 'computational science', 'computational engineering', 'computational linguistics',
    'computational biology', 'computational chemistry', 'computational physics', 'computational mathematics', 'computational statistics',
    'computational finance', 'computational economics', 'computational social science', 'computational journalism', 'computational arts',
    'computational design', 'computational architecture', 'computational engineering', 'computational medicine', 'computational health',
    'computational healthcare', 'computational neuroscience', 'computational genomics', 'computational proteomics', 'computational metabolomics',
    'computational systems biology', 'computational drug discovery', 'computational materials science', 'computational fluid dynamics',
    'computational electromagnetics', 'computational mechanics', 'computational acoustics', 'computational optics', 'computational photonics',
    'computational thermodynamics', 'computational heat transfer', 'computational mass transfer', 'computational aerodynamics',
    'computational hydrodynamics', 'computational oceanography', 'computational meteorology', 'computational climatology',
    'computational geophysics', 'computational seismology', 'computational volcanology', 'computational glaciology',
    'computational hydrology', 'computational geology', 'computational geochemistry', 'computational geomorphology',
    'computational paleontology', 'computational archaeology', 'computational anthropology', 'computational sociology',
    'computational psychology', 'computational psychiatry', 'computational cognitive science', 'computational linguistics',
    'computational philology', 'computational musicology', 'computational ethnomusicology', 'computational aesthetics',
    'computational creativity', 'computational poetry', 'computational literature', 'computational humanities', 'digital humanities',
    'digital scholarship', 'digital archaeology', 'digital anthropology', 'digital sociology', 'digital psychology', 'digital psychiatry',
    'digital cognitive science', 'digital linguistics', 'digital philology', 'digital musicology', 'digital ethnomusicology',
    'digital aesthetics', 'digital creativity', 'digital poetry', 'digital literature', 'digital humanities', 'digital scholarship',
    'digital archaeology', 'digital anthropology', 'digital sociology', 'digital psychology', 'digital psychiatry', 'digital cognitive science',
    'digital linguistics', 'digital philology', 'digital musicology', 'digital ethnomusicology', 'digital aesthetics', 'digital creativity',
    'digital poetry', 'digital literature'
]

# Combine all the keywords into a single set
ALL_IT_KEYWORDS = set(IT_KEYWORDS + TECH_KEYWORDS)

# Add common IT professions and roles
IT_PROFESSIONS = [
    'software', 'developer', 'engineer', 'programmer', 'coder', 'administrator', 'analyst', 'architect',
    'specialist', 'technician', 'consultant', 'manager', 'director', 'officer', 'lead', 'head', 'chief',
    'senior', 'junior', 'associate', 'principal', 'staff', 'fellow', 'intern', 'trainee', 'graduate',
    'expert', 'guru', 'ninja', 'rockstar', 'wizard', 'evangelist', 'advocate', 'champion', 'mentor', 'coach',
    'trainer', 'instructor', 'educator', 'professor', 'researcher', 'scientist', 'engineer', 'developer',
    'programmer', 'coder', 'hacker', 'builder', 'maker', 'creator', 'designer', 'artist', 'craftsman',
    'craftsperson', 'artisan', 'technologist', 'technician', 'specialist', 'professional', 'practitioner',
    'operator', 'administrator', 'admin', 'support', 'helpdesk', 'service desk', 'support desk', 'technical support',
    'it support', 'customer support', 'client support', 'user support', 'end user support', 'desktop support',
    'field support', 'remote support', 'onsite support', 'level 1 support', 'level 2 support', 'level 3 support',
    'tier 1 support', 'tier 2 support', 'tier 3 support', 'first line support', 'second line support', 'third line support'
]

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

def is_it_job(job_title, job_description):
    """Check if the job is IT-related based on keywords in title and description."""
    combined_text = f"{job_title} {job_description}".lower() if job_description else job_title.lower()
    for keyword in ALL_IT_KEYWORDS:
        if keyword in combined_text:
            return True
    for profession in IT_PROFESSIONS:
        if profession in combined_text:
            return True
    return False


def extract_job_data(card, sort_method, time_filter):
    """Extract data from a job card and filter for IT jobs"""
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

        job_title = title_elem.text.strip() if title_elem else "" # Capture title for IT check

        job_data.update({
            "job_id": job_id,
            "title": job_title if title_elem else None,
            "company": company_elem.text.strip() if company_elem else None,
            "location": location_elem.text.strip() if location_elem else None,
            "posted_date": time_elem.get("datetime") if time_elem else None,
            "job_url": job_url,
            "sort_method": sort_method,
            "time_filter": time_filter
        })

        # Get detailed info including description before checking if IT job
        details = get_job_details(job_id)
        if details:
            job_data.update(details)
        else:
            job_data["description"] = None # Ensure description exists even if get_job_details fails


        # Check if it is an IT job after fetching description
        if not is_it_job(job_data["title"], job_data.get("description", "")):
            return None  # Skip non-IT jobs

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

        logger.info("Starting comprehensive LinkedIn job scraping for IT jobs only")

        output_file = scrape_jobs_with_filters(jobs_per_combination=jobs_per_combination)

        # Count total jobs in file
        try:
            df = pd.read_csv(output_file)
            total_jobs = len(df)
            print(f"\nSuccessfully scraped {total_jobs} IT jobs")
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