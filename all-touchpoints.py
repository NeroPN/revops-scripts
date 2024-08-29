import time
import requests
import csv

# Replace this with your HubSpot Bearer Token
BEARER_TOKEN = 'YOUR_KEY'

# Test mode variable
TEST_MODE = False  # Set to False for full processing
TEST_LIMIT = 300  # Limit for the number of contacts to process in test mode

def make_request_with_retries(url, headers, params=None, retries=5, backoff=2):
    for i in range(retries):
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 429:  # Too Many Requests
            print(f"Rate limit hit. Retrying in {backoff} seconds...")
            time.sleep(backoff)
            backoff *= 2.5  # Exponential backoff
        else:
            response.raise_for_status()
            return response
    raise Exception("Max retries exceeded")

def get_contacts_with_history(offset=None, limit=50):
    url = 'https://api.hubapi.com/crm/v3/objects/contacts'
    headers = {'Authorization': f'Bearer {BEARER_TOKEN}'}
    params = {
        'limit': limit,
        'after': offset,
        'propertiesWithHistory': 'hs_latest_source,hs_latest_source_data_1,hs_latest_source_data_2',
        'properties': 'associatedcompanyid,hs_analytics_source,hs_analytics_source_data_1,hs_analytics_source_data_2'
    }
    response = make_request_with_retries(url, headers, params)
    return response.json()

def build_touchpoints(properties_with_history, contact_id, associatedcompanyid, hs_analytics_source, hs_analytics_source_data_1, hs_analytics_source_data_2):
    touchpoints = []
    
    valid_sources = {
        'REFERRALS',
        'OTHER_CAMPAIGNS',
        'ORGANIC_SOCIAL',
        'SOCIAL_ORGANIC',
        'PAID_SEARCH',
        'PAID_SOCIAL',
        'EVENTS',
        'NETWORK',
        'OFFLINE',
        'EMAIL_MARKETING',
        'DIRECT_TRAFFIC',
        'ORGANIC_SEARCH',
        'OTHER'
    }

    hs_latest_source = properties_with_history.get('hs_latest_source', [])
    hs_latest_source_data_1 = properties_with_history.get('hs_latest_source_data_1', [])
    hs_latest_source_data_2 = properties_with_history.get('hs_latest_source_data_2', [])

    for i in range(len(hs_latest_source)):
        source = hs_latest_source[i].get('value', 'unknown')
        source_data_1 = hs_latest_source_data_1[i].get('value', 'unknown') if i < len(hs_latest_source_data_1) else 'unknown'
        source_data_2 = hs_latest_source_data_2[i].get('value', 'unknown') if i < len(hs_latest_source_data_2) else 'unknown'
        timestamp = hs_latest_source[i].get('timestamp', None)
        
        if source in valid_sources:
            touchpoint = {
                'contact_id': contact_id,
                'associatedcompanyid': associatedcompanyid,
                'timestamp_touchpoint': timestamp,
                'source_touchpoint': source,
                'source_data_1_touchpoint': source_data_1,
                'source_data_2_touchpoint': source_data_2,
                'hs_analytics_source': hs_analytics_source,
                'hs_analytics_source_data_1': hs_analytics_source_data_1,
                'hs_analytics_source_data_2': hs_analytics_source_data_2
            }
            touchpoints.append(touchpoint)

    return touchpoints

def write_touchpoints_to_csv(touchpoints, filename='touchpoints.csv'):
    fieldnames = [
        'contact_id', 'associatedcompanyid', 'timestamp_touchpoint', 'source_touchpoint', 
        'source_data_1_touchpoint', 'source_data_2_touchpoint', 'hs_analytics_source', 
        'hs_analytics_source_data_1', 'hs_analytics_source_data_2'
    ]
    with open(filename, mode='a', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        if file.tell() == 0:  # If file is empty, write header
            writer.writeheader()
        writer.writerows(touchpoints)
        print(f"Still writing.. {touchpoints}")

def process_contacts_in_batches(batch_size=50):
    offset = None
    processed_count = 0
    
    while True:
        if TEST_MODE and processed_count >= TEST_LIMIT:
            print(f"Test mode active. Processed {processed_count} contacts. Exiting.")
            break
        
        response = get_contacts_with_history(offset=offset, limit=50)
        contacts = response.get('results', [])
        if not contacts:
            break
        
        all_touchpoints = []
        for contact in contacts:
            contact_id = contact.get('id')
            properties_with_history = contact.get('propertiesWithHistory', {})
            associatedcompanyid = contact.get('properties', {}).get('associatedcompanyid', 'unknown')
            hs_analytics_source = contact.get('properties', {}).get('hs_analytics_source', 'unknown')
            hs_analytics_source_data_1 = contact.get('properties', {}).get('hs_analytics_source_data_1', 'unknown')
            hs_analytics_source_data_2 = contact.get('properties', {}).get('hs_analytics_source_data_2', 'unknown')
            
            touchpoints = build_touchpoints(properties_with_history, contact_id, associatedcompanyid, hs_analytics_source, hs_analytics_source_data_1, hs_analytics_source_data_2)
            all_touchpoints.extend(touchpoints)
        
        write_touchpoints_to_csv(all_touchpoints)
        processed_count += len(contacts)
        
        offset = response.get('paging', {}).get('next', {}).get('after')
        if not offset or (TEST_MODE and processed_count >= TEST_LIMIT):
            break

if __name__ == "__main__":
    process_contacts_in_batches(batch_size=1000)
    print("DONE")
