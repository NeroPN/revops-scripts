import tiktoken
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time
import os
import json
import logging
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import re
import openai
from openai import OpenAI


client = OpenAI(
    # This is the default and can be omitted
    api_key="abc"
)

# ========================
# Configuration and Setup
# ========================

# Set up main data and logs directory
# It's recommended to make this path configurable via an environment variable
data_dir = os.getenv('DATA_DIR', './data-and-logs')
if not os.path.exists(data_dir):
    os.makedirs(data_dir)

# Set up logging for the main script
logging.basicConfig(
    filename=os.path.join(data_dir, 'app.log'),
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

logger = logging.getLogger()

# Environment variables (set as variables at the top for development purposes)
SCRAPER_API_KEY = 'abc'          # Replace with your ScraperAPI key
VALUESERP_API_KEY = 'abc'     # Replace with your ValueSerp API key
OPENAI_API_KEY = 'abc'  # Replace with your OpenAI API key
SLACK_BOT_TOKEN = 'abc'  # Replace with your Slack Bot Token
SLACK_CHANNEL_NAME = 'abc'   # Replace with your Slack channel name
GOOGLE_SERVICE_ACCOUNT_JSON = 'abc.json'  # Replace with your Google Service Account JSON path


# Sample payload data (dummy input)
payload = {
    'your_yoyaba_email': 'pn@yoyaba.com',
    'slack_channel_to_notify': 'internal_clockodo',
    'google_drive_folder_link': 'https://drive.google.com/drive/u/0/folders/1Yz7P3MCDEetUpP90UFecqJgiuuhxo93G',
    'client_product_type_description': 'Zeiterfassungssoftware',
    'client_name': 'Clockodo',  # Dynamic client name
    'client_website_url': 'clockodo.com',
    'client_linkedin_ad_library_id': '',
    'client_omr_review_page': 'https://omr.com/en/reviews/product/clockodo/all',
    'client_capterra_review_page': 'https://www.capterra.com.de/reviews/178143/clockodo',
    'competitor_websites': ['zep.de'],
    'competitor_linkedin_ad_library_ids': [],
    'competitor_omr_review_pages': ['https://omr.com/en/reviews/product/zep/all'],
    'competitor_capterra_review_pages': []
}

# ========================
# Helper Functions
# ========================

def num_tokens_from_messages(messages, model="gpt-4o"):
    """Returns the number of tokens used by a list of messages."""
    encoding = tiktoken.encoding_for_model(model)
    num_tokens = 0

    for message in messages:
        num_tokens += 4  # Every message has a fixed overhead
        for key, value in message.items():
            num_tokens += len(encoding.encode(value))
    num_tokens += 2  # Every reply is primed with 2 tokens

    return num_tokens

def format_url(url):
    """
    Formats the URL to include 'https://' if missing.

    Parameters:
        url (str): The URL to format.

    Returns:
        str: The formatted URL.
    """
    if not url.startswith(('http://', 'https://')):
        formatted_url = f"https://{url}"
        logger.debug(f"Formatted URL: {formatted_url}")
        return formatted_url
    return url

def extract_domain(url):
    """
    Extracts the domain from a URL.

    Parameters:
        url (str): The URL to extract the domain from.

    Returns:
        str: The extracted domain.
    """
    parsed_url = urlparse(url)
    domain = parsed_url.netloc if parsed_url.netloc else parsed_url.path.split('/')[0]
    domain = domain.replace('www.', '')
    logger.debug(f"Extracted domain: {domain}")
    return domain

def create_session_with_retries():
    session = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retries)
    session.mount('https://', adapter)
    session.mount('http://', adapter)
    return session

def scrape_with_scraperapi(url, extra_params=None, session=None):
    session = create_session_with_retries()
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
    print(f"Scraping URL: {url}")  # Debug print statement
    api_url = 'https://api.scraperapi.com/'
    params = {'api_key': SCRAPER_API_KEY, 'url': url}
    if extra_params:
        params.update(extra_params)

    request_timeout = 60  # seconds

    try:
        response = session.get(api_url, params=params, timeout=request_timeout)
        logger.info(f"Received response with status code: {response.status_code} for URL: {url}")
        print(f"Received response with status code: {response.status_code} for URL: {url}")  # Debug print statement
        if response.status_code == 200:
            return response.text  # Return raw HTML
        elif response.status_code == 429:
            logger.warning(f"Rate limit exceeded for URL: {url}. Skipping.")
            print(f"Rate limit exceeded for URL: {url}. Skipping.")  # Debug print statement
            return None
        else:
            logger.error(f"Failed to scrape URL {url}: {response.status_code} {response.reason}")
            print(f"Failed to scrape URL {url}: {response.status_code} {response.reason}")  # Debug print statement
            return None
    except requests.RequestException as e:
        logger.error(f"RequestException while scraping URL {url}: {e}")
        print(f"RequestException while scraping URL {url}: {e}")  # Debug print statement
        return None

def perform_google_search_multiple_results(query, top_n=3, location='Germany'):
    """
    Performs a Google search using ValueSerp API and returns the top n organic results.

    Parameters:
        query (str): The search query.
        top_n (int): Number of top results to return.
        location (str): The location to perform the search from.

    Returns:
        list: A list of dictionaries containing the top n organic results.
    """
    logger.info(f"Performing Google search for query: '{query}'")
    print(f"Performing Google search for query: '{query}'")  # Debug print statement
    api_url = 'https://api.valueserp.com/search'
    params = {
        'api_key': VALUESERP_API_KEY,
        'q': query,
        'engine': 'google',
        'location': location,
        'language': 'de',
        'output': 'json',
        'num': str(top_n)
    }
    backoff_time = 1
    max_retries = 3
    retries = 0

    while retries < max_retries:
        try:
            response = requests.get(api_url, params=params, timeout=30)
            print(f"ValueSerp API response status code: {response.status_code}")  # Debug print statement
            if response.status_code == 200:
                results = response.json()
                organic_results = results.get('organic_results', [])
                if organic_results:
                    logger.info(f"Found {len(organic_results)} organic results.")
                    print(f"Found {len(organic_results)} organic results.")  # Debug print statement
                    return organic_results[:top_n]
                else:
                    logger.warning("No organic results found in the search response.")
                    print("No organic results found in the search response.")  # Debug print statement
                    return []
            elif response.status_code == 429:
                logger.warning(f"Rate limit exceeded for ValueSerp API. Retrying in {backoff_time} seconds.")
                print(f"Rate limit exceeded for ValueSerp API. Retrying in {backoff_time} seconds.")  # Debug print statement
                time.sleep(backoff_time)
                backoff_time *= 2
                retries += 1
            else:
                logger.error(f"Failed to perform ValueSerp search: {response.status_code} {response.reason}")
                print(f"Failed to perform ValueSerp search: {response.status_code} {response.reason}")  # Debug print statement
                return []
        except requests.RequestException as e:
            logger.error(f"RequestException during ValueSerp search: {e}")
            print(f"RequestException during ValueSerp search: {e}")  # Debug print statement
            return []

    logger.error(f"Failed to perform ValueSerp search after {max_retries} retries.")
    print(f"Failed to perform ValueSerp search after {max_retries} retries.")  # Debug print statement
    return []

def save_html_to_file(content, directory, filename):
    """
    Saves the HTML content or text to a file in the specified directory.

    Parameters:
        content (str): The content to save.
        directory (str): The directory where the file will be saved.
        filename (str): The name of the file.

    Returns:
        None
    """
    filepath = os.path.join(directory, filename)
    try:
        with open(filepath, 'w', encoding='utf-8') as file:
            file.write(content)
        logger.info(f"Saved content to {filepath}")
        print(f"Saved content to {filepath}")  # Debug print statement
    except Exception as e:
        logger.error(f"Error saving content to file {filepath}: {e}")
        print(f"Error saving content to file {filepath}: {e}")  # Debug print statement

def save_json_to_file(data, directory, filename):
    """
    Saves the data to a JSON file in the specified directory.

    Parameters:
        data (dict or list): The data to save.
        directory (str): The directory where the file will be saved.
        filename (str): The name of the JSON file.

    Returns:
        None
    """
    filepath = os.path.join(directory, filename)
    try:
        with open(filepath, 'w', encoding='utf-8') as file:
            json.dump(data, file, ensure_ascii=False, indent=4)
        logger.info(f"Saved JSON data to {filepath}")
        print(f"Saved JSON data to {filepath}")  # Debug print statement
    except Exception as e:
        logger.error(f"Error saving JSON to file {filepath}: {e}")
        print(f"Error saving JSON to file {filepath}: {e}")  # Debug print statement

def strip_html(html_content):
    """
    Strips HTML content to retain only specified tags and removes all links.
    Removes style, script, and noscript tags.

    Parameters:
        html_content (str): The raw HTML content.

    Returns:
        str: Stripped HTML content as a string.
    """
    RELEVANT_TAGS = ['h1', 'h2', 'h3', 'h4', 'p', 'span', 'a', 'div']
    soup = BeautifulSoup(html_content, 'html.parser')

    # Remove style, script, and noscript tags
    for tag in soup(['style', 'script', 'noscript']):
        tag.decompose()

    # Remove all <a> tags but keep their text
    for a_tag in soup.find_all('a'):
        a_tag.replace_with(a_tag.get_text(strip=True))

    text_chunks = []
    seen_texts = set()

    for tag in RELEVANT_TAGS:
        for element in soup.find_all(tag):
            text = element.get_text(strip=True)
            if text and text not in seen_texts:
                text_chunks.append(f"{tag.upper()}: {text}")
                seen_texts.add(text)

    stripped_html = "\n".join(text_chunks)
    return stripped_html

def summarize_content(content, content_type, logger_instance=None):
    """
    Summarizes the given content using OpenAI's API.

    Parameters:
        content (str): The content to summarize.
        content_type (str): The type of content (e.g., 'news_article', 'omr_reviews', 'capterra_reviews', 'homepage', etc.)
        logger_instance (logging.Logger): Logger instance for this function.

    Returns:
        str: The summary of the content.
    """
    if not logger_instance:
        logger_instance = logger

    # Build the prompt based on content type
    if content_type == 'news_article':
        prompt = f"""Summarize the following news article extensively to answer the following questions, but do not answer the questions directly:
{content}

Questions:
**Strategic Questions:**
6. **KEY HEADLINES & GRAPHICS**
   - Key news headlines, graphics, etc., that showcase the market trends and how the market is changing.
   - Link to important pages and summary of most important information.
10. **HOW HAS THE BUYER WORLD CHANGED?**
    - What changes are top of mind in the market?
    - What topics cause insecurity and concerns?
    - How do technological advances and innovation impact the industry?
    - What has changed in customer preferences and customer behavior?
"""
    elif content_type in ['omr_reviews', 'capterra_reviews']:
        prompt = f"""Summarize the following reviews extensively to answer the following questions, but do not answer the questions directly:
{content}

Questions:
8. **INNER VS. OUTER PERCEPTION**
   - **STORIES OF US**: What is the company saying about themselves?
   - **STORIES ABOUT US**: What are people saying about the company?
     (LinkedIn Comments, Testimonials, Review pages, Forums)
   - **STORIES FROM THE PAST**: What used to be beliefs in the market?
     (How did the industry used to approach topics? Customer behavior?)
   - **STORIES FROM THE FUTURE**: What are economic/technological/demographic/cultural/social trends relevant to the industry?
     (What will the future of the industry look like? How does that affect the buyer?)

9. **KEY DIFFERENCES**
   - What are the company's product strengths?
   - What are the company's product weaknesses?
"""
    elif content_type == 'homepage':
        prompt = f"""Summarize the following website content extensively to answer the following questions, but do not answer the questions directly:
{content}

Questions:
1. **COMPANY VISION**
   - What is the desired future state of the company? (5+ years)

2. **STATUS QUO**
   - Where does the Company currently stand?
   - What are the company's strengths?
   - What's the current ARR, sales cycle length, average deal size, etc.?

3. **COMPANY MISSION**
   - How does the company plan to get to the vision?
     (What’s the business, who does it serve, what does it do, objectives, approach)

4. **VALUES**
   - By which values does the company live by?

5. **BUSINESS OBJECTIVES**
   - Which main business objectives does the company want to achieve?
"""
    else:
        # Default prompt for other content types
        prompt = f"""Summarize the following content extensively to answer the relevant strategic questions, but do not answer the questions directly:
{content}

Questions:
[Insert relevant strategic questions here]
"""

    # Send the prompt to OpenAI
    response = get_summary_from_openai(prompt, logger_instance)
    return response

def get_summary_from_openai(prompt, logger_instance=None):
    """
    Sends the prompt to OpenAI's API and returns the response.

    Parameters:
        prompt (str): The prompt to send.
        logger_instance (logging.Logger): Logger instance for this function.

    Returns:
        str: The assistant's response.
    """
    if not logger_instance:
        logger_instance = logger

    try:
        response = client.chat.completions.create(model="gpt-4o", 
        messages=[
            {"role": "user", "content": prompt}
        ],
        max_tokens=16384,
        temperature=0.5)
        assistant_response = response.choices[0].message.content
        return assistant_response.strip()
    except openai.OpenAIError as e:
        logger_instance.error(f"Error getting summary from OpenAI: {e}")
        print(f"Error getting summary from OpenAI: {e}")  # Debug print statement
        return ""

def prepare_openai_prompt(aggregated_data, payload):
    """
    Prepares the prompt for the OpenAI API based on the aggregated data.

    Parameters:
        aggregated_data (dict): The aggregated data from scraping.
        payload (dict): The original payload containing input variables.

    Returns:
        str: The prepared prompt.
    """
    # Initialize prompt components with company and setting context
    prompt = f"""
You are a Creative Strategy Research Assistant at YOYABA.com, a B2B SaaS Marketing Agency specializing in crafting positioning and messaging for our clients. You are tasked with analyzing the following summarized data to provide strategic insights for our client, **{payload.get('client_name', 'the client')}**, a company offering **{payload.get('client_product_type_description', 'their product')}**.

**Research Context:**
- **Client Name:** {payload.get('client_name', 'N/A')}
- **Client Website:** {payload.get('client_website_url', 'N/A')}
"""

    # Add Client Homepage Summary
    client_homepage_summary = aggregated_data['client'].get('homepage', {}).get('summary', '')
    if client_homepage_summary:
        prompt += f"\n**Client Homepage Summary:**\n{client_homepage_summary}\n\n"

    # Add Client Product/Features Pages Summaries
    product_features_pages = aggregated_data['client'].get('product_features_pages', [])
    for idx, page in enumerate(product_features_pages, start=1):
        summary = page.get('summary', '')
        if summary:
            prompt += f"**Client Product/Features Page {idx} Summary:**\n{summary}\n\n"

    # Add Client Pricing Pages Summaries
    pricing_pages = aggregated_data['client'].get('pricing_pages', [])
    for idx, page in enumerate(pricing_pages, start=1):
        summary = page.get('summary', '')
        if summary:
            prompt += f"**Client Pricing Page {idx} Summary:**\n{summary}\n\n"

    # Add Client OMR Reviews Summary
    omr_reviews_summary = aggregated_data['client'].get('omr_reviews_summary', '')
    if omr_reviews_summary:
        prompt += f"**Client OMR Reviews Summary:**\n{omr_reviews_summary}\n\n"

    # Add Client Capterra Reviews Summary
    capterra_reviews_summary = aggregated_data['client'].get('capterra_reviews_summary', '')
    if capterra_reviews_summary:
        prompt += f"**Client Capterra Reviews Summary:**\n{capterra_reviews_summary}\n\n"

    # Add News Articles Summaries
    news_articles = aggregated_data.get('news_articles', [])
    for idx, article in enumerate(news_articles, start=1):
        summary = article.get('summary', '')
        if summary:
            prompt += f"**News Article {idx} Summary:**\n{summary}\n\n"

    # Include the strategic questions
    prompt += """
**Strategic Questions:**

1. **COMPANY VISION**
   What is the desired future state of the company? (5+ years)

2. **STATUS QUO**
   - Where does the Company currently stand?
   - What are the company's strengths?
   - What's the current ARR, sales cycle length, average deal size, etc.?

3. **COMPANY MISSION**
   How does the company plan to get to the vision?
   (What’s the business, who does it serve, what does it do, objectives, approach)

4. **VALUES**
   By which values does the company live by?

5. **BUSINESS OBJECTIVES**
   Which main business objectives does the company want to achieve?

6. **KEY HEADLINES & GRAPHICS**
   - Key news headlines, graphics, etc., that showcase the market trends and how the market is changing.
   - Link to important pages and summary of most important information.

7. **DISCOVERING SPARKS**
   Sparks are short-term opportunities that we can use to bring our message forward (e.g., news, events, hypes, trends). They help us create urgency especially in the Why Change and Why Now stages.

8. **INNER VS. OUTER PERCEPTION**
   - **STORIES OF US**: What is the company saying about themselves?
   - **STORIES ABOUT US**: What are people saying about the company?
     (LinkedIn Comments, Testimonials, Review pages, Forums)
   - **STORIES FROM THE PAST**: What used to be beliefs in the market?
     (How did the industry used to approach topics? Customer behavior?)
   - **STORIES FROM THE FUTURE**: What are economic/technological/demographic/cultural/social trends relevant to the industry?
     (What will the future of the industry look like? How does that affect the buyer?)

9. **KEY DIFFERENCES**
   Where does the inner and outer perception differ?

10. **HOW HAS THE BUYER WORLD CHANGED?**
    - What changes are top of mind in the market?
    - What topics cause insecurity and concerns?
    - How do technological advances and innovation impact the industry?
    - What has changed in customer preferences and customer behavior?

Please provide detailed answers based on the summarized data.
"""

    return prompt


def get_answers_from_openai(prompt):
    """
    Sends the prompt to OpenAI's API and returns the assistant's response.

    Parameters:
        prompt (str): The prompt to send.

    Returns:
        str: The assistant's response.
    """
    try:
        response = client.chat.completions.create(model="gpt-4o",
        messages=[
            {"role": "user", "content": prompt}
        ],
        max_tokens=16384,
        temperature=0.3)
        assistant_response = response.choices[0].message.content
        return assistant_response.strip()
    except openai.OpenAIError as e:
        logger.error(f"Error getting response from OpenAI: {e}")
        print(f"Error getting response from OpenAI: {e}")  # Debug print statement
        return ""

# ========================
# Scraping Functions
# ========================

def scrape_omr_reviews(url, max_pages=3, filename=None, directory=None, session=None):
    """
    Scrapes OMR reviews from the given URL, up to a maximum of 3 pages.

    Parameters:
        url (str): The URL of the OMR review page.
        max_pages (int): Maximum number of pages to scrape.
        filename (str, optional): Base filename to save HTML content.
        directory (str, optional): Directory to save files.

    Returns:
        list: A list of dictionaries containing OMR reviews.
    """
    all_reviews = []
    extra_params = {
        'ultra_premium': 'true',
        'render': 'true',
        'premium': 'true',
        'country_code': 'eu',
        'render_timeout': 50000
    }

    if not session:
        session = create_session_with_retries()

    for page_number in range(1, max_pages + 1):
        if page_number == 1:
            page_url = url  # First page
        else:
            # Ensure the URL ends with a slash before appending the page number
            if url.endswith('/'):
                page_url = f"{url}{page_number}"
            else:
                page_url = f"{url}/{page_number}"
        logger.info(f"Scraping OMR reviews page {page_number}: {page_url}")
        print(f"Scraping OMR reviews page {page_number}: {page_url}")  # Debug print statement
        page_content = scrape_with_scraperapi(page_url, extra_params=extra_params, session=session)
        if page_content:
            # Save the full HTML content
            if filename and directory:
                html_filename = f"{filename}_page_{page_number}.html"
                save_html_to_file(page_content, directory, html_filename)
            soup = BeautifulSoup(page_content, 'html.parser')

            reviews = extract_reviews_from_omr_page(soup)
            if reviews:
                all_reviews.extend(reviews)
                print(f"Extracted {len(reviews)} reviews from page {page_number}")  # Debug print statement
            else:
                logger.info(f"No reviews found on page {page_number}. Stopping pagination.")
                print(f"No reviews found on page {page_number}. Stopping pagination.")  # Debug print statement
                break  # No more reviews found, exit the loop
        else:
            logger.warning(f"Failed to scrape OMR reviews from page {page_number}. Moving on.")
            print(f"Failed to scrape OMR reviews from page {page_number}. Moving on.")  # Debug print statement
            continue  # Move on to the next page
    return all_reviews

def extract_reviews_from_omr_page(soup):
    """
    Extracts all reviews from a BeautifulSoup object of a single OMR page.

    Parameters:
        soup (BeautifulSoup): Parsed HTML content of the page.

    Returns:
        list: A list of dictionaries containing review details.
    """
    reviews = []
    # Find all review containers (update the class name based on actual HTML structure)
    review_containers = soup.find_all('div', attrs={'data-testid': 'product-reviews-list-item'})
    print(f'Found {len(review_containers)} OMR review containers on this page.')

    for container in review_containers:
        try:
            # Initialize a dictionary to hold review data
            review_data = {}

            # Extract Review ID
            overview_div = container.find('div', attrs={'data-testid': 'review-overview'})
            review_id = overview_div.get('id') if overview_div and overview_div.get('id') else None
            review_data['id'] = review_id

            # Extract Review Title
            title_tag = container.find('div', attrs={'data-testid': 'review-overview-title'})
            review_title = title_tag.get_text(strip=True) if title_tag else None
            review_data['title'] = review_title

            # Extract Rating
            rating_div = container.find('div', attrs={'data-testid': 'review-overview-rating'})
            if rating_div:
                filled_stars = rating_div.find_all('svg', class_=re.compile(r'text-yellow'))
                rating = len(filled_stars)
                # Check for half-stars or decimal ratings if applicable
                half_star = rating_div.find('path', d=re.compile(r'M12,15.39'))
                if half_star:
                    rating += 0.5
                review_data['rating'] = rating
            else:
                review_data['rating'] = None

            # Extract Author Information
            author_info = {}
            author_div = container.find('div', attrs={'data-testid': 'review-author'})
            if author_div:
                # Author Name
                name_tag = author_div.find('div', attrs={'data-testid': 'review-author-name'})
                author_name = name_tag.get_text(strip=True) if name_tag else None
                author_info['name'] = author_name

                # Author Date
                date_tag = author_div.find('div', attrs={'data-testid': 'review-author-date'})
                author_date = date_tag.get_text(strip=True) if date_tag else None
                author_info['date'] = author_date

                # Author Validated
                validated_tag = author_div.find('div', attrs={'data-testid': 'review-author-validated'})
                author_validated = False
                if validated_tag:
                    badge = validated_tag.find('span')
                    if badge and 'Validated Reviewer' in badge.get_text():
                        author_validated = True
                author_info['validated'] = author_validated

                # Author Position
                position_tag = author_div.find('div', attrs={'data-testid': 'review-author-company-position'})
                if position_tag:
                    position_text = position_tag.get_text(strip=True)
                    author_position = position_text.replace('at', '').strip()
                else:
                    author_position = None
                author_info['position'] = author_position

                # Author Company Name
                company_name_tag = author_div.find('div', attrs={'data-testid': 'review-author-company-name'})
                company_name = company_name_tag.get_text(strip=True) if company_name_tag else None
                author_info['company_name'] = company_name

                # Author Company Size and Industry
                company_size = None
                company_field = None
                ul_tags = author_div.find_all('ul')
                for ul in ul_tags:
                    li_tags = ul.find_all('li')
                    for li in li_tags:
                        # Company Size
                        size_div = li.find('div', class_=re.compile(r'bg-solid'))
                        if size_div and 'employees' in size_div.get_text().lower():
                            size_text = size_div.get_text(strip=True)
                            company_size_match = re.search(r'(\d+-\d+|\d+)\s*employees', size_text, re.IGNORECASE)
                            if company_size_match:
                                company_size = company_size_match.group(1)
                        # Industry Field
                        field_div = li.find('div', class_=re.compile(r'bg-solid'))
                        if field_div and 'industry' in field_div.get_text().lower():
                            field_text = field_div.get_text(strip=True)
                            company_field = field_text.replace('Industry:', '').strip()
                author_info['company_size'] = company_size
                author_info['industry'] = company_field

            review_data['author'] = author_info

            # Extract Review Sections (What did you like?, What did you not like?, Problems solved)
            sections = {
                'what_did_you_like': None,
                'what_did_you_not_like': None,
                'problems_solved': None
            }

            quotes_div = container.find('div', attrs={'data-testid': 'text-review-quotes'})
            if quotes_div:
                # Positive Feedback
                positive_div = quotes_div.find('div', attrs={'data-testid': 'text-review-quotes-positive'})
                if positive_div:
                    positive_answer = positive_div.find('div', attrs={'data-testid': 'review-quote-answer'})
                    sections['what_did_you_like'] = positive_answer.get_text(strip=True) if positive_answer else None

                # Negative Feedback
                negative_div = quotes_div.find('div', attrs={'data-testid': 'text-review-negative'})
                if negative_div:
                    negative_answer = negative_div.find('div', attrs={'data-testid': 'review-quote-answer'})
                    sections['what_did_you_not_like'] = negative_answer.get_text(strip=True) if negative_answer else None

                # Problems Solved
                problems_div = quotes_div.find('div', attrs={'data-testid': 'text-review-problems'})
                if problems_div:
                    problems_answer = problems_div.find('div', attrs={'data-testid': 'review-quote-answer'})
                    sections['problems_solved'] = problems_answer.get_text(strip=True) if problems_answer else None

            review_data.update(sections)

            # Append the review data to the reviews list
            reviews.append(review_data)

        except Exception as e:
            logger.error(f"Error extracting a OMR review: {e}")
            print(f"Error extracting a OMR review: {e}")  # Debug print statement
            continue  # Continue with the next review

    return reviews

def scrape_capterra_reviews(url, max_pages=1, filename=None, directory=None):
    """
    Scrapes Capterra reviews from the given URL, up to a maximum of 3 pages.

    Parameters:
        url (str): The URL of the Capterra review page.
        max_pages (int): Maximum number of pages to scrape.
        filename (str, optional): Base filename to save HTML content.
        directory (str, optional): Directory to save files.

    Returns:
        list: A list of dictionaries containing Capterra reviews.
    """
    all_reviews = []
    extra_params = {
        'ultra_premium': 'true',
        'render': 'true',
        'premium': 'true',
        'country_code': 'eu',
        'render_timeout': '30000'
    }

    for page_number in range(1, max_pages + 1):
        if page_number == 1:
            page_url = url  # First page
        else:
            # Append page number to URL (Assuming Capterra uses '?page=' parameter)
            if '?' in url:
                page_url = f"{url}&page={page_number}"
            else:
                page_url = f"{url}?page={page_number}"
        logger.info(f"Scraping Capterra reviews page {page_number}: {page_url}")
        print(f"Scraping Capterra reviews page {page_number}: {page_url}")  # Debug print statement
        page_content = scrape_with_scraperapi(page_url, extra_params=extra_params)
        if page_content:
            # Save the full HTML content
            if filename and directory:
                html_filename = f"{filename}_page_{page_number}.html"
                save_html_to_file(page_content, directory, html_filename)
            soup = BeautifulSoup(page_content, 'html.parser')

            reviews = extract_capterra_reviews(page_content)
            if reviews:
                all_reviews.extend(reviews)
                print(f"Extracted {len(reviews)} reviews from page {page_number}")  # Debug print statement
            else:
                logger.info(f"No reviews found on page {page_number}. Stopping pagination.")
                print(f"No reviews found on page {page_number}. Stopping pagination.")  # Debug print statement
                break  # No more reviews found, exit the loop
        else:
            logger.warning(f"Failed to scrape Capterra reviews from page {page_number}. Moving on.")
            print(f"Failed to scrape Capterra reviews from page {page_number}. Moving on.")  # Debug print statement
            continue  # Move on to the next page
    return all_reviews

def extract_company_name(html_content):
    """
    Extracts the company name from embedded JavaScript variables.
    Falls back to extracting from the <title> tag if not found.

    Parameters:
        html_content (str): Raw HTML content of the Capterra reviews page.

    Returns:
        str: Extracted company name or 'Unknown Company' if extraction fails.
    """
    # Attempt to extract from embedded JavaScript variables
    # Patterns to match "item_brand":"Clockodo" or "item_name":"Clockodo"
    brand_pattern = re.compile(r'"item_brand"\s*:\s*"([^"]+)"', re.IGNORECASE)
    name_pattern = re.compile(r'"item_name"\s*:\s*"([^"]+)"', re.IGNORECASE)

    brand_match = brand_pattern.search(html_content)
    if brand_match:
        company_name = brand_match.group(1).strip()
        logging.info(f"Extracted company name from 'item_brand': {company_name}")
        return company_name

    name_match = name_pattern.search(html_content)
    if name_match:
        company_name = name_match.group(1).strip()
        logging.info(f"Extracted company name from 'item_name': {company_name}")
        return company_name

    # Fallback to extracting from the <title> tag
    soup = BeautifulSoup(html_content, 'html.parser')
    title_tag = soup.find('title')
    if title_tag:
        title_text = title_tag.get_text(strip=True)
        # Assuming the title format: "CompanyName Erfahrungen von echten Nutzern - Capterra Deutschland 2024"
        title_match = re.match(r"^(.*?)\s+Erfahrungen", title_text)
        if title_match:
            company_name = title_match.group(1).strip()
            logging.info(f"Extracted company name from <title>: {company_name}")
            return company_name
    logging.error("Company name could not be extracted from embedded JavaScript or <title> tag.")
    return "Unknown Company"

def extract_reviewer_info(reviewer_section):
    """
    Extracts reviewer information from the reviewer section.

    Parameters:
        reviewer_section (BeautifulSoup object): The reviewer section.

    Returns:
        dict: Dictionary containing reviewer information.
    """
    reviewer_info = {}

    # Name
    name_tag = reviewer_section.select_one("div.h5.fw-bold.mb-2")
    reviewer_info['reviewer'] = name_tag.get_text(strip=True) if name_tag else "Anonymous"

    # Role/Position
    role_tag = reviewer_section.select_one("div.text-ash.mb-2")
    reviewer_info['role'] = role_tag.get_text(strip=True) if role_tag else None

    # Company Details
    company_tags = reviewer_section.select("div.mb-2")
    for tag in company_tags:
        text = tag.get_text(strip=True)
        # Verwendete die Software für:
        if "Verwendete die Software für:" in text:
            usage_duration = text.split("Verwendete die Software für:")[-1].strip()
            reviewer_info['usage_duration'] = usage_duration
        # Industry and Company Size
        elif "," in text:
            parts = text.split(",")
            if len(parts) >= 2:
                reviewer_info['industry'] = parts[0].strip()
                reviewer_info['company_size'] = parts[1].strip()
            else:
                reviewer_info['industry'] = parts[0].strip()
        # Herkunft der Bewertung
        elif "Herkunft der Bewertung" in text:
            # Extract tooltip for verification
            origin = tag.find("sylar-tooltip")
            if origin:
                tooltip_title = origin.get("data-bs-title", "").strip()
                reviewer_info['verified'] = True if "verifizierten Nutzer" in tooltip_title else False
            else:
                reviewer_info['verified'] = False

    return reviewer_info

def extract_review_content(content_section, company_name):
    """
    Extracts review content from the content section.

    Parameters:
        content_section (BeautifulSoup object): The content section.
        company_name (str): Name of the company being reviewed.

    Returns:
        dict: Dictionary containing review content.
    """
    content = {}

    # Title
    title_tag = content_section.select_one("h3.h5.fw-bold")
    content['title'] = title_tag.get_text(strip=True) if title_tag else None

    # Rating and Date
    rating_date_div = content_section.select_one("div.text-ash.mb-3")
    if rating_date_div:
        # Rating
        rating_span = rating_date_div.select_one("span.ms-1")
        rating_text = rating_span.get_text(strip=True).replace(',', '.') if rating_span else None
        try:
            content['rating'] = float(rating_text) if rating_text else None
        except ValueError:
            content['rating'] = None

        # Date
        date_span = rating_date_div.select_one("span.ms-2")
        content['comment_date'] = date_span.get_text(strip=True) if date_span else None
    else:
        content['rating'] = None
        content['comment_date'] = None

    # Comments
    comments_p = content_section.find("p", string=re.compile(r"Kommentare:", re.IGNORECASE))
    if comments_p:
        comments = comments_p.find_next_sibling("span").get_text(strip=True)
        content['comments'] = comments
    else:
        content['comments'] = None

    # Pros
    pros_p = content_section.find("p", string=re.compile(r"Vorteile:", re.IGNORECASE))
    if pros_p:
        pros = pros_p.find_next_sibling("p").get_text(strip=True)
        content['pros'] = pros
    else:
        content['pros'] = None

    # Cons
    cons_p = content_section.find("p", string=re.compile(r"Nachteile:", re.IGNORECASE))
    if cons_p:
        cons = cons_p.find_next_sibling("p").get_text(strip=True)
        content['cons'] = cons
    else:
        content['cons'] = None

    # Additional Sections
    additional_sections = content_section.select("p")
    for p in additional_sections:
        text = p.get_text().strip()
        # Use regex to make company-specific phrases dynamic
        # Example: "Warum [Company Name] gewählt wurde:"
        if re.match(r"In Betracht gezogene Alternativen:", text, re.IGNORECASE):
            alternatives = p.find_all("a")
            alternatives_list = [a.get_text(strip=True) for a in alternatives]
            content['considered_alternatives'] = alternatives_list
        elif re.match(fr"Warum {re.escape(company_name)} gewählt wurde:", text, re.IGNORECASE):
            reasons = re.sub(fr"Warum {re.escape(company_name)} gewählt wurde:", "", text, flags=re.IGNORECASE).strip()
            content['reasons_for_choice'] = reasons
        elif re.match(r"Zuvor genutzte Software:", text, re.IGNORECASE):
            prior_software = p.find_all("a")
            prior_software_list = [a.get_text(strip=True) for a in prior_software]
            content['prior_software'] = prior_software_list
        elif re.match(fr"Gründe für den Wechsel zu {re.escape(company_name)}:", text, re.IGNORECASE):
            switch_reasons = re.sub(fr"Gründe für den Wechsel zu {re.escape(company_name)}:", "", text, flags=re.IGNORECASE).strip()
            content['switch_reasons'] = switch_reasons

    return content

def extract_capterra_reviews(html_content, selectors=None):
    """
    Extracts Capterra reviews from the provided HTML content.
    Returns a list of dictionaries containing review details.

    Parameters:
        html_content (str): Raw HTML content of the Capterra reviews page.
        selectors (dict, optional): Dictionary containing CSS selectors for various elements.

    Returns:
        list: A list of dictionaries, each representing a Capterra review.
    """
    reviews = []
    soup = BeautifulSoup(html_content, 'html.parser')

    # Extract the company name
    company_name = extract_company_name(html_content)

    # Default selectors if none provided
    default_selectors = {
        'review_card': "div.review-card",
        'reviewer_section': "div.col-lg-5",
        'name': "div.h5.fw-bold.mb-2",
        'role': "div.text-ash.mb-2",
        'company_details': "div.mb-2",
        'content_section': "div.col-lg-7",
        'title': "h3.h5.fw-bold",
        'rating_date_div': "div.text-ash.mb-3",
        'rating_span': "span.ms-1",
        'date_span': "span.ms-2",
        'comments_p': re.compile(r"Kommentare:", re.IGNORECASE),
        'pros_p': re.compile(r"Vorteile:", re.IGNORECASE),
        'cons_p': re.compile(r"Nachteile:", re.IGNORECASE),
        'additional_sections_p': "p"
    }

    # Update default selectors with any provided selectors
    if selectors:
        default_selectors.update(selectors)

    # Locate all review cards
    review_cards = soup.select(default_selectors['review_card'])
    logging.info(f"Found {len(review_cards)} review cards.")

    for idx, card in enumerate(review_cards, start=1):
        try:
            review_data = {}

            # Extract Reviewer Information
            reviewer_section = card.select_one(default_selectors['reviewer_section'])
            if reviewer_section:
                reviewer_info = extract_reviewer_info(reviewer_section)
                review_data.update(reviewer_info)
            else:
                logging.warning(f"Reviewer section not found in review card {idx}.")

            # Extract Review Content
            content_section = card.select_one(default_selectors['content_section'])
            if content_section:
                content = extract_review_content(content_section, company_name)
                review_data.update(content)
            else:
                logging.warning(f"Content section not found in review card {idx}.")

            # Append the extracted review data to the reviews list
            reviews.append(review_data)

        except Exception as e:
            logging.error(f"Error extracting review {idx}: {e}")
            continue  # Skip to the next review if an error occurs

    return reviews

def scrape_ad_library(company_id, entity_type='client', competitor_idx=None, directory=None, logger_instance=None):
    """
    Scrapes the LinkedIn Ad Library for the given company ID and saves raw HTML.
    Also extracts ad details and saves them as JSON.

    Parameters:
        company_id (str): The LinkedIn Ad Library ID of the company.
        entity_type (str): 'client' or 'competitor' to differentiate in filenames.
        competitor_idx (int, optional): The index of the competitor for naming purposes.
        directory (str): Directory where HTML files and logs will be saved.
        logger_instance (logging.Logger): Logger instance for this function.

    Returns:
        list: A list of dictionaries containing ad details if successful, else empty list.
    """
    if not logger_instance:
        logger_instance = logger

    base_url = f'https://www.linkedin.com/ad-library/search?companyIds={company_id}&dateOption=last-30-days'
    logger_instance.info(f"Constructed LinkedIn Ad Library URL: {base_url}")
    print(f"Constructed LinkedIn Ad Library URL: {base_url}")  # Debug print statement

    # Define filenames based on entity type
    if entity_type == 'client':
        main_html_filename = 'client_linkedin_ads_main.html'
        ads_json_filename = 'client_linkedin_ads.json'
    elif entity_type == 'competitor' and competitor_idx is not None:
        main_html_filename = f'competitor_{competitor_idx}_linkedin_ads_main.html'
        ads_json_filename = f'competitor_{competitor_idx}_linkedin_ads.json'
    else:
        main_html_filename = 'linkedin_ads_main.html'
        ads_json_filename = 'linkedin_ads.json'

    # Scrape the main Ad Library page and save raw HTML
    page_content = scrape_with_scraperapi(base_url)
    if page_content:
        save_html_to_file(page_content, directory, main_html_filename)
        logger_instance.info(f"Saved main Ad Library HTML to {main_html_filename}")
        print(f"Saved main Ad Library HTML to {main_html_filename}")  # Debug print statement

        # Extract ad detail links without processing them
        ad_links = extract_ad_links(page_content)
        if ad_links:
            logger_instance.info(f"Found {len(ad_links)} ad detail links.")
            print(f"Found {len(ad_links)} ad detail links.")  # Debug print statement
            ads = []
            for idx, ad_link in enumerate(ad_links[:10], start=1):  # Limit to first 10 ads
                ad_detail = scrape_ad_detail_page(ad_link, entity_type, competitor_idx, ad_idx=idx, directory=directory, logger_instance=logger_instance)
                if ad_detail:
                    ads.append(ad_detail)
            # Save the ads data as JSON
            if ads:
                ads_json = {
                    'company_id': company_id,
                    'ads': ads
                }
                save_json_to_file(ads_json, directory, ads_json_filename)
                logger_instance.info(f"Saved ads data to {ads_json_filename}")
                print(f"Saved ads data to {ads_json_filename}")

                # Return the ads list for aggregation
                return ads
        else:
            logger_instance.warning("No ad detail links found.")
            print("No ad detail links found.")
    else:
        logger_instance.error(f"Failed to scrape LinkedIn Ad Library page for company ID {company_id}.")
        print(f"Failed to scrape LinkedIn Ad Library page for company ID {company_id}.")

    return []

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

def scrape_ad_detail_page(ad_url, entity_type='client', competitor_idx=None, ad_idx=1, directory=None, logger_instance=None):
    """
    Scrapes the ad detail page and saves raw HTML.
    Also extracts ad copy and call-to-action and returns them.

    Parameters:
        ad_url (str): The URL of the ad detail page.
        entity_type (str): 'client' or 'competitor' to differentiate in filenames.
        competitor_idx (int, optional): The index of the competitor for naming purposes.
        ad_idx (int): The index of the ad for naming purposes.
        directory (str): Directory where HTML files and logs will be saved.
        logger_instance (logging.Logger): Logger instance for this function.

    Returns:
        dict or None: Dictionary containing ad details if successful, else None.
    """
    if not logger_instance:
        logger_instance = logger

    page_content = scrape_with_scraperapi(ad_url)
    if page_content:
        # Define filenames based on entity type
        if entity_type == 'client':
            detail_html_filename = f'client_linkedin_ad_detail_{ad_idx}.html'
            detail_json_filename = f'client_linkedin_ad_detail_{ad_idx}.json'
        elif entity_type == 'competitor' and competitor_idx is not None:
            detail_html_filename = f'competitor_{competitor_idx}_linkedin_ad_detail_{ad_idx}.html'
            detail_json_filename = f'competitor_{competitor_idx}_linkedin_ad_detail_{ad_idx}.json'
        else:
            detail_html_filename = f'linkedin_ad_detail_{ad_idx}.html'
            detail_json_filename = f'linkedin_ad_detail_{ad_idx}.json'

        # Save the raw HTML content
        save_html_to_file(page_content, directory, detail_html_filename)
        logger_instance.info(f"Saved Ad Detail HTML to {detail_html_filename}")
        print(f"Saved Ad Detail HTML to {detail_html_filename}")

        # Extract the ad copy content
        soup = BeautifulSoup(page_content, 'html.parser')
        ad_copy = soup.find('p', class_='commentary__content')
        ad_copy_text = ad_copy.get_text(strip=True) if ad_copy else "Ad copy not found"

        # Extract the call-to-action
        cta_button = soup.find('button', class_='ad-preview__cta')
        cta_text = cta_button.get_text(strip=True) if cta_button else "Call-to-action not found"

        # Output the results
        print("Ad Copy:", ad_copy_text)
        print("Call-to-Action:", cta_text)

        ad_detail = {
            'ad_url': ad_url,
            'ad_copy': ad_copy_text,
            'call_to_action': cta_text
        }

        # Save the extracted ad details as JSON
        save_json_to_file(ad_detail, directory, detail_json_filename)
        logger_instance.info(f"Saved Ad Detail data to {detail_json_filename}")
        print(f"Saved Ad Detail data to {detail_json_filename}")

        return ad_detail
    else:
        logger_instance.warning(f"Failed to scrape ad detail page: {ad_url}")
        print(f"Failed to scrape ad detail page: {ad_url}")  # Debug print statement
        return None

def extract_homepage_content(html_content):
    """
    Extracts relevant content from the homepage and other news pages.
    Returns the stripped text.

    Parameters:
        html_content (str): The raw HTML content.

    Returns:
        str: Stripped HTML content as a string.
    """
    stripped_text = strip_html(html_content)
    return stripped_text

# ========================
# Main Function
# ========================

def main():
    """
    Main function to automate the research process.
    """
    logger.info("Starting research automation process.")
    print("Starting research automation process.")  # Debug print statement

    # Step 1: Validate required fields
    client_website = payload.get('client_website_url')
    if not client_website:
        logger.error('Client Website URL is required.')
        print("Client Website URL is required.")  # Debug print statement
        return

    # Ensure URLs are properly formatted
    client_domain = extract_domain(client_website)
    client_website = format_url(client_website)
    competitor_websites = [format_url(url) for url in payload.get('competitor_websites', [])]
    competitor_domains = [extract_domain(url) for url in payload.get('competitor_websites', [])]

    # Initialize aggregated data structure
    aggregated_data = {
        'client': {},
        'competitors': [],
        'linkedin_ads': {
            'client_ads': [],
            'competitor_ads': []
        },
        # Additional data structures for news, ads, reviews, etc.
    }

    # Step 2: Scrape Client Homepage (with HTML stripping)
    client_homepage_content = scrape_with_scraperapi(client_website)
    if client_homepage_content:
        stripped_homepage_content = extract_homepage_content(client_homepage_content)
        save_html_to_file(stripped_homepage_content, data_dir, 'client_stripped_homepage_content.txt')
        # Summarize the content
        summary = summarize_content(stripped_homepage_content, 'homepage')
        aggregated_data['client']['homepage'] = {
            'url': client_website,
            'type': 'client_homepage',
            'summary': summary  # Store the summary instead of full content
        }
        # Optionally, save the summary to a file
        save_html_to_file(summary, data_dir, 'client_homepage_summary.txt')
    else:
        logger.error(f"Failed to scrape client homepage: {client_website}")
        print(f"Failed to scrape client homepage: {client_website}")  # Debug print statement


    # Step 3: Perform Google searches and scrape top 3 organic hits for client
    client_queries = [
        f"site:{client_domain} product OR features",
        f"site:{client_domain} pricing"
    ]
    query_types = ['product_features_pages', 'pricing_pages']
    base_filenames = ['client_product_features', 'client_pricing']

    for query, page_type, base_filename in zip(client_queries, query_types, base_filenames):
        search_results = perform_google_search_multiple_results(query, top_n=3, location='Germany')
        if search_results:
            pages = []
            for idx, result in enumerate(search_results):
                result_url = result.get('link')
                if result_url:
                    page_content = scrape_with_scraperapi(result_url)
                    if page_content:
                        stripped_content = extract_homepage_content(page_content)
                        # Summarize the content
                        summary = summarize_content(stripped_content, page_type)
                        pages.append({
                            'url': result_url,
                            'type': page_type,
                            'summary': summary
                        })
                        # Save the summary to a file
                        summary_filename = f"{base_filename}_{idx+1}_summary.txt"
                        save_html_to_file(summary, data_dir, summary_filename)
                    else:
                        logger.error(f"Failed to scrape page: {result_url}")
                        print(f"Failed to scrape page: {result_url}")  # Debug print statement
            aggregated_data['client'][page_type] = pages
        else:
            logger.error(f"No results found for query: '{query}'")
            print(f"No results found for query: '{query}'")  # Debug print statement


    # Step 4: Process competitor websites similarly
    for idx, competitor_website in enumerate(competitor_websites):
        competitor_domain = competitor_domains[idx]
        competitor_data = {}
        # Scrape competitor homepage (with HTML stripping)
        competitor_homepage_content = scrape_with_scraperapi(competitor_website)
        if competitor_homepage_content:
            stripped_competitor_homepage = extract_homepage_content(competitor_homepage_content)
            competitor_data['homepage'] = {
                'url': competitor_website,
                'type': 'competitor_homepage',
                'html': stripped_competitor_homepage
            }
            # Save the stripped content to a file
            filename = f"competitor_{idx+1}_homepage_stripped.txt"
            save_html_to_file(stripped_competitor_homepage, data_dir, filename)
        else:
            logger.error(f"Failed to scrape competitor homepage: {competitor_website}")
            print(f"Failed to scrape competitor homepage: {competitor_website}")  # Debug print statement

        # Perform Google searches for competitor
        competitor_queries = [
            f"site:{competitor_domain} product OR features",
            f"site:{competitor_domain} pricing"
        ]
        query_types = ['product_features_pages', 'pricing_pages']
        base_filenames = [f"competitor_{idx+1}_product_features", f"competitor_{idx+1}_pricing"]

        for query, page_type, base_filename in zip(competitor_queries, query_types, base_filenames):
            search_results = perform_google_search_multiple_results(query, top_n=3, location='Germany')
            if search_results:
                pages = []
                for idx2, result in enumerate(search_results):
                    result_url = result.get('link')
                    if result_url:
                        page_content = scrape_with_scraperapi(result_url)
                        if page_content:
                            stripped_content = extract_homepage_content(page_content)
                            pages.append({
                                'url': result_url,
                                'type': page_type,
                                'html': stripped_content
                            })
                            # Save the stripped content to a file
                            stripped_filename = f"{base_filename}_{idx2+1}_stripped.txt"
                            save_html_to_file(stripped_content, data_dir, stripped_filename)
                        else:
                            logger.error(f"Failed to scrape page: {result_url}")
                            print(f"Failed to scrape page: {result_url}")  # Debug print statement
                competitor_data[page_type] = pages
            else:
                logger.error(f"No results found for query: '{query}'")
                print(f"No results found for query: '{query}'")  # Debug print statement

        aggregated_data['competitors'].append(competitor_data)


    # Step 6: Scrape OMR reviews for client and competitors
    # Create a dedicated directory for OMR Reviews
    omr_reviews_dir = os.path.join(data_dir, 'omr_reviews')
    if not os.path.exists(omr_reviews_dir):
        os.makedirs(omr_reviews_dir)

    # Update logging for OMR Reviews
    omr_logger = logging.getLogger('OMR_Reviews')
    omr_handler = logging.FileHandler(os.path.join(omr_reviews_dir, 'omr_reviews.log'))
    omr_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    omr_logger.addHandler(omr_handler)
    omr_logger.setLevel(logging.INFO)

    # Scrape client OMR reviews
    client_omr_url = payload.get('client_omr_review_page')
    if client_omr_url:
        omr_logger.info(f"Scraping OMR reviews for client: {client_omr_url}")
        print(f"Scraping OMR reviews for client: {client_omr_url}")  # Debug print statement
        client_reviews = scrape_omr_reviews(client_omr_url, filename='client_omr_reviews', directory=omr_reviews_dir)
        if client_reviews:
            # Concatenate reviews into one string
            reviews_text = "\n".join([json.dumps(review, ensure_ascii=False) for review in client_reviews])
            # Summarize the reviews
            summary = summarize_content(reviews_text, 'omr_reviews')
            aggregated_data['client']['omr_reviews_summary'] = summary
            # Save the summary
            save_html_to_file(summary, omr_reviews_dir, 'client_omr_reviews_summary.txt')
        else:
            omr_logger.error(f"Failed to scrape OMR reviews for client: {client_omr_url}")
            print(f"Failed to scrape OMR reviews for client: {client_omr_url}")  # Debug print statement
    else:
        omr_logger.warning("Client OMR review page URL is missing.")
        print("Client OMR review page URL is missing.")  # Debug print statement

    # Step 7: Scrape Capterra reviews for client and competitors
    # Create a dedicated directory for Capterra Reviews
    capterra_reviews_dir = os.path.join(data_dir, 'capterra_reviews')
    if not os.path.exists(capterra_reviews_dir):
        os.makedirs(capterra_reviews_dir)

    # Update logging for Capterra Reviews
    capterra_logger = logging.getLogger('Capterra_Reviews')
    capterra_handler = logging.FileHandler(os.path.join(capterra_reviews_dir, 'capterra_reviews.log'))
    capterra_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    capterra_logger.addHandler(capterra_handler)
    capterra_logger.setLevel(logging.INFO)

    # Scrape client Capterra reviews
    client_capterra_url = payload.get('client_capterra_review_page')
    if client_capterra_url:
        capterra_logger.info(f"Scraping Capterra reviews for client: {client_capterra_url}")
        print(f"Scraping Capterra reviews for client: {client_capterra_url}")  # Debug print statement
        client_capterra_reviews = scrape_capterra_reviews(client_capterra_url, filename='client_capterra_reviews', directory=capterra_reviews_dir)
        # After scraping client Capterra reviews
        if client_capterra_reviews:
            # Concatenate reviews into one string
            reviews_text = "\n".join([json.dumps(review, ensure_ascii=False) for review in client_capterra_reviews])
            # Summarize the reviews
            summary = summarize_content(reviews_text, 'capterra_reviews')
            aggregated_data['client']['capterra_reviews_summary'] = summary
            # Save the summary
            save_html_to_file(summary, capterra_reviews_dir, 'client_capterra_reviews_summary.txt')
        else:
            capterra_logger.error(f"Failed to scrape Capterra reviews for client: {client_capterra_url}")
            print(f"Failed to scrape Capterra reviews for client: {client_capterra_url}")  # Debug print statement

    else:
        capterra_logger.warning("Client Capterra review page URL is missing.")
        print("Client Capterra review page URL is missing.")  # Debug print statement

    # Step 8: Scrape LinkedIn Ad Library for client and competitors
    # Create a dedicated directory for LinkedIn Ads
    linkedin_ads_dir = os.path.join(data_dir, 'linkedin_ads')
    if not os.path.exists(linkedin_ads_dir):
        os.makedirs(linkedin_ads_dir)

    # Update logging for LinkedIn Ads
    linkedin_ads_logger = logging.getLogger('LinkedIn_Ads')
    linkedin_ads_log = os.path.join(linkedin_ads_dir, 'linkedin_ads.log')
    linkedin_ads_handler = logging.FileHandler(linkedin_ads_log)
    linkedin_ads_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    linkedin_ads_logger.addHandler(linkedin_ads_handler)
    linkedin_ads_logger.setLevel(logging.INFO)

    # Scrape LinkedIn ad library for client
    client_ad_library_id = payload.get('client_linkedin_ad_library_id')
    competitor_ad_library_ids = payload.get('competitor_linkedin_ad_library_ids', [])

    if client_ad_library_id:
        linkedin_ads_logger.info(f"Scraping LinkedIn Ad Library for client: {client_ad_library_id}")
        print(f"Scraping LinkedIn Ad Library for client: {client_ad_library_id}")  # Debug print statement
        client_ads = scrape_ad_library(
            company_id=client_ad_library_id,
            entity_type='client',
            competitor_idx=None,
            directory=linkedin_ads_dir,
            logger_instance=linkedin_ads_logger
        )
        aggregated_data['linkedin_ads']['client_ads'] = client_ads
    else:
        linkedin_ads_logger.warning("Client LinkedIn Ad Library ID is missing.")
        print("Client LinkedIn Ad Library ID is missing.")  # Debug print statement

    # Scrape LinkedIn ad library for competitors
    competitor_ads_all = []
    for idx, competitor_id in enumerate(competitor_ad_library_ids, start=1):
        if competitor_id:
            linkedin_ads_logger.info(f"Scraping LinkedIn Ad Library for competitor {idx}: {competitor_id}")
            print(f"Scraping LinkedIn Ad Library for competitor {idx}: {competitor_id}")  # Debug print statement
            competitor_ads = scrape_ad_library(
                company_id=competitor_id,
                entity_type='competitor',
                competitor_idx=idx,
                directory=linkedin_ads_dir,
                logger_instance=linkedin_ads_logger
            )
            competitor_ads_all.append(competitor_ads)
        else:
            linkedin_ads_logger.warning(f"Competitor {idx} LinkedIn Ad Library ID is missing.")
            print(f"Competitor {idx} LinkedIn Ad Library ID is missing.")  # Debug print statement
    aggregated_data['linkedin_ads']['competitor_ads'] = competitor_ads_all

    # Step 9: Aggregate data and prepare OpenAI prompt
    prompt = prepare_openai_prompt(aggregated_data, payload)

    # Save the prompt to a file for reference
    save_html_to_file(prompt, data_dir, 'openai_prompt.txt')

    # Step 10: Send prompt to OpenAI and process response
    assistant_response = get_answers_from_openai(prompt)

    # Save the assistant's response to a file
    save_html_to_file(assistant_response, data_dir, 'openai_response.txt')

    # Step 11: Output results to Slack and Google Docs
    # TODO: Implement Slack and Google Docs integration

    # Step 11: Output results to Slack and Google Docs
    # TODO: Create a custom GPT for client at hand

    logger.info("Research automation process completed.")
    print("Research automation process completed.")  # Debug print statement

# ========================
# Execution Entry Point
# ========================

if __name__ == '__main__':
        main()
