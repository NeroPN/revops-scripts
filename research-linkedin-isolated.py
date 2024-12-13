import os
import json
import logging
import requests
from urllib.parse import urlparse
import re
import time

# Set up logging
data_dir = '/Users/paulnispel/Desktop/code/other/yoyaba/automation-research/data-and-logs/linkedin_ads'
if not os.path.exists(data_dir):
    os.makedirs(data_dir)

logging.basicConfig(
    filename=os.path.join(data_dir, 'linkedin_ads.log'),
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

logger = logging.getLogger()

# Environment variables (Ensure these are set securely in a real-world scenario)
SCRAPER_API_KEY = 'abc'  # Replace with your ScraperAPI key

# Sample payload data (dummy input)
payload = {
    'client_linkedin_ad_library_id': '2527599',
    'competitor_linkedin_ad_library_ids': ['123456', '789012']  # Add as needed
}

def main():
    """
    Main function to scrape LinkedIn Ads for the client and competitors.
    """
    logger.info("Starting LinkedIn Ads scraping process.")
    print("Starting LinkedIn Ads scraping process.")

    # Scrape LinkedIn Ad Library for client
    client_ad_library_id = payload.get('client_linkedin_ad_library_id')
    if client_ad_library_id:
        logger.info(f"Scraping LinkedIn Ad Library for client: {client_ad_library_id}")
        print(f"Scraping LinkedIn Ad Library for client: {client_ad_library_id}")
        scrape_ad_library(client_ad_library_id, entity_type='client')
    else:
        logger.warning("Client LinkedIn Ad Library ID is missing.")
        print("Client LinkedIn Ad Library ID is missing.")

    # Scrape LinkedIn Ad Library for competitors
    competitor_ad_library_ids = payload.get('competitor_linkedin_ad_library_ids', [])
    for idx, competitor_id in enumerate(competitor_ad_library_ids, start=1):
        if competitor_id:
            logger.info(f"Scraping LinkedIn Ad Library for competitor {idx}: {competitor_id}")
            print(f"Scraping LinkedIn Ad Library for competitor {idx}: {competitor_id}")
            scrape_ad_library(competitor_id, entity_type='competitor', competitor_idx=idx)
        else:
            logger.warning(f"Competitor {idx} LinkedIn Ad Library ID is missing.")
            print(f"Competitor {idx} LinkedIn Ad Library ID is missing.")

    logger.info("LinkedIn Ads scraping process completed.")
    print("LinkedIn Ads scraping process completed.")

def scrape_ad_library(company_id, entity_type='client', competitor_idx=None):
    """
    Scrapes the LinkedIn Ad Library for the given company ID and saves raw HTML.

    Parameters:
        company_id (str): The LinkedIn Ad Library ID of the company.
        entity_type (str): 'client' or 'competitor' to differentiate in filenames.
        competitor_idx (int, optional): The index of the competitor for naming purposes.
    """
    base_url = f'https://www.linkedin.com/ad-library/search?companyIds={company_id}&dateOption=last-30-days'
    logger.info(f"Constructed LinkedIn Ad Library URL: {base_url}")
    print(f"Constructed LinkedIn Ad Library URL: {base_url}")

    # Define filenames based on entity type
    if entity_type == 'client':
        main_filename = 'client_linkedin_ads_main.html'
    elif entity_type == 'competitor' and competitor_idx is not None:
        main_filename = f'competitor_{competitor_idx}_linkedin_ads_main.html'
    else:
        main_filename = 'linkedin_ads_main.html'

    # Scrape the main Ad Library page and save raw HTML
    page_content = scrape_with_scraperapi(base_url)
    if page_content:
        save_html_to_file(page_content, data_dir, main_filename)
        logger.info(f"Saved main Ad Library HTML to {main_filename}")
        print(f"Saved main Ad Library HTML to {main_filename}")

        # Extract ad detail links
        ad_links = extract_ad_links(page_content)
        if ad_links:
            logger.info(f"Found {len(ad_links)} ad detail links.")
            print(f"Found {len(ad_links)} ad detail links.")
            for idx, ad_link in enumerate(ad_links[:10], start=1):  # Limit to first 10 ads
                scrape_ad_detail_page(ad_link, entity_type, competitor_idx, ad_idx=idx)
        else:
            logger.warning("No ad detail links found.")
            print("No ad detail links found.")
    else:
        logger.error(f"Failed to scrape LinkedIn Ad Library page for company ID {company_id}.")
        print(f"Failed to scrape LinkedIn Ad Library page for company ID {company_id}.")

def scrape_with_scraperapi(url, extra_params=None):
    """
    Scrapes a given URL using ScraperAPI and returns the raw HTML content.
    Handles basic error logging.

    Parameters:
        url (str): The URL to scrape.
        extra_params (dict, optional): Additional parameters for ScraperAPI.

    Returns:
        str or None: Raw HTML content if successful, else None.
    """
    logger.info(f"Scraping URL: {url}")
    print(f"Scraping URL: {url}")
    api_url = 'https://api.scraperapi.com/'
    params = {'api_key': SCRAPER_API_KEY, 'url': url}
    if extra_params:
        params.update(extra_params)

    request_timeout = 60  # seconds

    try:
        response = requests.get(api_url, params=params, timeout=request_timeout)
        logger.info(f"Received response with status code: {response.status_code} for URL: {url}")
        print(f"Received response with status code: {response.status_code} for URL: {url}")
        if response.status_code == 200:
            return response.text
        elif response.status_code == 429:
            logger.warning(f"Rate limit exceeded for URL: {url}. Skipping.")
            print(f"Rate limit exceeded for URL: {url}. Skipping.")
            return None
        else:
            logger.error(f"Failed to scrape URL {url}: {response.status_code} {response.reason}")
            print(f"Failed to scrape URL {url}: {response.status_code} {response.reason}")
            return None
    except requests.RequestException as e:
        logger.error(f"RequestException while scraping URL {url}: {e}")
        print(f"RequestException while scraping URL {url}: {e}")
        return None

def extract_ad_links(html_content):
    """
    Extracts ad detail links from the main Ad Library HTML content using regex.

    Parameters:
        html_content (str): Raw HTML content of the Ad Library page.

    Returns:
        list: A list of full URLs to ad detail pages.
    """
    ad_links = []
    # Regex pattern to find ad detail links
    pattern = re.compile(r'href="(/ad-library/detail/\d+)"')
    matches = pattern.findall(html_content)
    for match in matches:
        full_link = f'https://www.linkedin.com{match}'
        if full_link not in ad_links:
            ad_links.append(full_link)
    return ad_links

def scrape_ad_detail_page(ad_url, entity_type='client', competitor_idx=None, ad_idx=1):
    """
    Scrapes the ad detail page and saves raw HTML.

    Parameters:
        ad_url (str): The URL of the ad detail page.
        entity_type (str): 'client' or 'competitor' to differentiate in filenames.
        competitor_idx (int, optional): The index of the competitor for naming purposes.
        ad_idx (int): The index of the ad for naming purposes.
    """
    page_content = scrape_with_scraperapi(ad_url)
    if page_content:
        # Define filenames based on entity type
        if entity_type == 'client':
            detail_filename = f'client_linkedin_ad_detail_{ad_idx}.html'
        elif entity_type == 'competitor' and competitor_idx is not None:
            detail_filename = f'competitor_{competitor_idx}_linkedin_ad_detail_{ad_idx}.html'
        else:
            detail_filename = f'linkedin_ad_detail_{ad_idx}.html'

        save_html_to_file(page_content, data_dir, detail_filename)
        logger.info(f"Saved Ad Detail HTML to {detail_filename}")
        print(f"Saved Ad Detail HTML to {detail_filename}")
    else:
        logger.warning(f"Failed to scrape ad detail page: {ad_url}")
        print(f"Failed to scrape ad detail page: {ad_url}")

def save_html_to_file(html_content, directory, filename):
    """
    Saves the raw HTML content to a file in the specified directory.

    Parameters:
        html_content (str): Raw HTML content to save.
        directory (str): Directory where the file will be saved.
        filename (str): Name of the file.
    """
    filepath = os.path.join(directory, filename)
    try:
        with open(filepath, 'w', encoding='utf-8') as file:
            file.write(html_content)
        logger.info(f"Saved HTML content to {filepath}")
        print(f"Saved HTML content to {filepath}")
    except Exception as e:
        logger.error(f"Error saving HTML to file {filepath}: {e}")
        print(f"Error saving HTML to file {filepath}: {e}")

if __name__ == '__main__':
    main()
