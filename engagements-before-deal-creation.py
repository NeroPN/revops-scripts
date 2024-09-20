"""
HubSpot Custom Code Action Script

This script retrieves all engagements (calls, meetings, emails) associated with contacts of a company linked to a specific deal. It filters engagements that occurred before the deal's creation date.

Instructions:
1. Configure Input Fields in HubSpot custom code action:
   - Input: "deal_id" - Record ID of the deal
   - Input: "deal_create_date" - Deal create date property

2. Set your API key and replace "ACCESS_TOKEN" in row 38.

3. Set engagement types (default is emails, meetings, calls) in row 35.
4. Define output fields: 
    'engagements_filtered_json' - string
    'engagements_all_json' - string
    'deal_create_date_formatted' - string
    'count_calls_before_deal_creation' - number
    'count_meetings_before_deal_creation' - number
    'count_emails_before_deal_creation' - number
    'count_engagements_before_deal_creation' - number
"""
import requests
import json
import os
import time
import random
from datetime import datetime
import concurrent.futures

# === Configuration ===
BASE_URL = 'https://api.hubapi.com'
MAX_RETRIES = 3          
RETRY_BACKOFF = 2        
ENGAGEMENT_TYPES = ['calls', 'meetings', 'emails']  

# === Authentication Configuration ===
ACCESS_TOKEN = os.getenv('RevOps_KEY')  
if not ACCESS_TOKEN:
    raise Exception('Access token is not set correctly. Please ensure the "Workflows" environment variable is configured.')

HEADERS = {
    'Content-Type': 'application/json',
    'Authorization': f'Bearer {ACCESS_TOKEN}'
}

def log_message(message):
    """
    Log messages with timestamp.
    """
    print(f"{datetime.utcnow().isoformat()} - INFO - {message}")

def make_request(url, params=None):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, headers=HEADERS, params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as http_err:
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", RETRY_BACKOFF))
                wait_time = retry_after * (RETRY_BACKOFF ** attempt) + random.uniform(0, 1)
                log_message(f"Rate limit hit. Retrying after {wait_time:.2f} seconds... (Attempt {attempt})")
                time.sleep(wait_time)
            elif 500 <= response.status_code < 600:
                # Server-side error, retry
                wait_time = RETRY_BACKOFF * (2 ** attempt) + random.uniform(0, 1)
                log_message(f"Server error ({response.status_code}). Retrying after {wait_time:.2f} seconds... (Attempt {attempt})")
                time.sleep(wait_time)
            else:
                try:
                    error_detail = response.json().get('message', response.text)
                except json.JSONDecodeError:
                    error_detail = response.text
                raise Exception(f"HTTP error {response.status_code}: {error_detail}") from http_err
        except requests.exceptions.RequestException as req_err:
            raise Exception(f"Request error: {req_err}") from req_err
    raise Exception(f"Failed to fetch data from {url} after {MAX_RETRIES} attempts.")

def make_post_request(url, data):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.post(url, headers=HEADERS, json=data, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as http_err:
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", RETRY_BACKOFF))
                wait_time = retry_after * (RETRY_BACKOFF ** attempt) + random.uniform(0, 1)
                log_message(f"Rate limit hit. Retrying after {wait_time:.2f} seconds... (Attempt {attempt})")
                time.sleep(wait_time)
            elif 500 <= response.status_code < 600:
                # Server-side error, retry
                wait_time = RETRY_BACKOFF * (2 ** attempt) + random.uniform(0, 1)
                log_message(f"Server error ({response.status_code}). Retrying after {wait_time:.2f} seconds... (Attempt {attempt})")
                time.sleep(wait_time)
            else:
                try:
                    error_detail = response.json().get('message', response.text)
                except json.JSONDecodeError:
                    error_detail = response.text
                raise Exception(f"HTTP error {response.status_code}: {error_detail}") from http_err
        except requests.exceptions.RequestException as req_err:
            raise Exception(f"Request error: {req_err}") from req_err
    raise Exception(f"Failed to fetch data from {url} after {MAX_RETRIES} attempts.")

def get_associated_company_id(deal_id):
    url = f"{BASE_URL}/crm/v3/objects/deals/{deal_id}"
    params = {
        'associations': 'companies'
    }
    data = make_request(url, params)
    associations = data.get('associations', {}).get('companies', {}).get('results', [])
    if not associations:
        raise Exception(f"No associated company found for deal {deal_id}")
    company_id = associations[0].get('id')
    if not company_id:
        raise Exception(f"Association object does not contain 'id' for deal {deal_id}")
    return company_id

def get_company_contacts(company_id):
    contacts = []
    url = f"{BASE_URL}/crm/v3/objects/companies/{company_id}/associations/contacts"
    params = {
        'limit': 100
    }
    while url:
        data = make_request(url, params)
        results = data.get('results', [])
        for assoc in results:
            to_object_id = assoc.get('toObjectId')
            if to_object_id:
                contacts.append(to_object_id)
            else:
                id = assoc.get('id')
                if id:
                    contacts.append(id)
                else:
                    log_message(f"Missing 'toObjectId' and 'id' in association: {assoc}")
        paging = data.get('paging', {})
        next_page = paging.get('next', {})
        url = next_page.get('link')
        params = None 
    return contacts

def get_engagement_ids_for_contact(contact_id):
    engagement_ids = {eng_type: [] for eng_type in ENGAGEMENT_TYPES}
    for engagement_type in ENGAGEMENT_TYPES:
        log_message(f"Fetching {engagement_type} for Contact ID: {contact_id}")
        associated_ids = []
        url = f"{BASE_URL}/crm/v3/objects/contacts/{contact_id}/associations/{engagement_type}"
        params = {
            'limit': 500  # Increase limit to max allowed
        }
        while url:
            try:
                data = make_request(url, params)
                results = data.get('results', [])
                for assoc in results:
                    engagement_id = assoc.get('toObjectId') or assoc.get('id')
                    if engagement_id:
                        associated_ids.append(engagement_id)
                    else:
                        log_message(f"Missing 'toObjectId' and 'id' in association: {assoc}")
                paging = data.get('paging', {})
                next_page = paging.get('next', {})
                url = next_page.get('link')
                params = None
            except Exception as e:
                log_message(f"Error fetching associations for {engagement_type} on Contact ID {contact_id}: {e}")
                break
        engagement_ids[engagement_type].extend(associated_ids)
    return engagement_ids

def batch_get_engagement_details(engagement_type, engagement_ids):
    engagements = []
    batch_size = 100  # HubSpot batch API limit
    for i in range(0, len(engagement_ids), batch_size):
        batch_ids = engagement_ids[i:i+batch_size]
        url = f"{BASE_URL}/crm/v3/objects/{engagement_type}/batch/read"
        payload = {
            "properties": ["hs_timestamp", "hs_activity_type", "subject", "createdate"],
            "inputs": [{"id": str(eng_id)} for eng_id in batch_ids],
            "archived": False
        }
        try:
            data = make_post_request(url, payload)
            results = data.get('results', [])
            for eng in results:
                engagements.append({
                    'engagement_id': eng['id'],
                    'engagement_type': engagement_type[:-1],  # 'calls' -> 'call'
                    'engagement_outcome': eng.get('properties', {}).get('hs_activity_type') or eng.get('properties', {}).get('subject'),
                    'timestamp': eng.get('properties', {}).get('hs_timestamp'),
                    'created_date': eng.get('properties', {}).get('createdate')
                })
        except Exception as e:
            log_message(f"Failed to retrieve batch details for {engagement_type}: {e}")
    return engagements

def main(event):
    default_output = {
        'engagements_json': '',
        'engagements_all_json': '',
        'deal_create_date_formatted': '',
        'count_calls_before_deal_creation': 0,
        'count_meetings_before_deal_creation': 0,
        'count_emails_before_deal_creation': 0,
        'count_engagements_before_deal_creation': 0,
        'error_message': ''
    }

    try:
        input_fields = event.get('inputFields', {})
        deal_id = input_fields.get('deal_id')
        deal_create_date_unix = input_fields.get('deal_create_date')

        if not deal_id:
            default_output['error_message'] = 'Deal ID is missing.'
            return {'outputFields': default_output}

        if not deal_create_date_unix:
            default_output['error_message'] = 'Deal creation date is missing.'
            return {'outputFields': default_output}

        try:
            deal_create_date_unix = int(deal_create_date_unix)
            if deal_create_date_unix > 10**12:
                deal_create_date_unix = deal_create_date_unix / 1000
                log_message(f"Converted deal_create_date from milliseconds to seconds: {deal_create_date_unix}")
        except ValueError:
            default_output['error_message'] = 'Invalid deal creation date format. It should be a UNIX timestamp in milliseconds.'
            return {'outputFields': default_output}


        deal_create_date_formatted_initial = datetime.utcfromtimestamp(deal_create_date_unix).strftime('%Y-%m-%d %H:%M:%S UTC')
        log_message(f"Deal Create date log: {deal_create_date_unix} ({deal_create_date_formatted_initial})")

        company_id = get_associated_company_id(deal_id)
        log_message(f"Associated Company ID: {company_id}")

        contact_ids = get_company_contacts(company_id)
        log_message(f"Associated Contact IDs: {contact_ids}")

        if not contact_ids:
            raise Exception(f"No contacts associated with company {company_id}")

        all_engagement_ids = {eng_type: [] for eng_type in ENGAGEMENT_TYPES}

        # Fetch engagement IDs for all contacts in parallel
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future_to_contact = {executor.submit(get_engagement_ids_for_contact, contact_id): contact_id for contact_id in contact_ids}
            for future in concurrent.futures.as_completed(future_to_contact):
                contact_id = future_to_contact[future]
                try:
                    engagement_ids = future.result()
                    # Collect engagement IDs per type
                    for eng_type in ENGAGEMENT_TYPES:
                        all_engagement_ids[eng_type].extend(engagement_ids[eng_type])
                except Exception as e:
                    log_message(f"Error fetching engagements for Contact ID {contact_id}: {e}")

        all_engagements = []
        for eng_type in ENGAGEMENT_TYPES:
            eng_ids = all_engagement_ids[eng_type]
            if not eng_ids:
                continue
            log_message(f"Fetching details for {len(eng_ids)} {eng_type}")
            engagements = batch_get_engagement_details(eng_type, eng_ids)
            all_engagements.extend(engagements)

        all_engagements_filtered = []
        count_calls = 0
        count_meetings = 0
        count_emails = 0
        count_total = 0

        for engagement in all_engagements:
            try:
                # Convert timestamp to unix timestamp
                engagement_timestamp = None
                try:
                    engagement_timestamp = datetime.strptime(engagement['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ').timestamp()
                except ValueError:
                    engagement_timestamp = datetime.strptime(engagement['timestamp'], '%Y-%m-%dT%H:%M:%SZ').timestamp()
                if engagement_timestamp < deal_create_date_unix:
                    all_engagements_filtered.append(engagement)
                    if engagement['engagement_type'] == 'call':
                        count_calls += 1
                    elif engagement['engagement_type'] == 'meeting':
                        count_meetings += 1
                    elif engagement['engagement_type'] == 'email':
                        count_emails += 1
                    count_total += 1
            except Exception as e:
                log_message(f"Error processing engagement ID {engagement['engagement_id']}: {e}")

        engagements_json = json.dumps(all_engagements_filtered, indent=2)
        engagements_all_json = json.dumps(all_engagements, indent=2)

        deal_create_date_formatted = datetime.utcfromtimestamp(deal_create_date_unix).strftime('%Y-%m-%d %H:%M:%S UTC')

        output_fields = {
            'engagements_filtered_json': engagements_json,
            'engagements_all_json': engagements_all_json,
            'deal_create_date_formatted': deal_create_date_formatted,
            'count_calls_before_deal_creation': count_calls,
            'count_meetings_before_deal_creation': count_meetings,
            'count_emails_before_deal_creation': count_emails,
            'count_engagements_before_deal_creation': count_total,
            'error_message': ''
        }

        return {'outputFields': output_fields}

    except Exception as e:
        log_message(f"Error: {str(e)}")
        output_fields = default_output.copy()
        output_fields['error_message'] = str(e)
        return {'outputFields': output_fields}
