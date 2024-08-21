"""
HubSpot Datetime Property Automation Script

This script automates the process of creating datetime properties in HubSpot 
based on existing date properties for different HubSpot objects (e.g., contacts, companies, deals).
It then backfills these datetime properties with the last value change timestamp 
from the history of the corresponding date properties.

Requirements:
- Python 3.x
- HubSpot API Key

Setup Instructions:
1. Install Python 3.x from https://www.python.org/ if you haven't already.
2. Ensure you have pip installed for managing Python packages.

Configuration:
1. Replace 'YOUR_HUBSPOT_API_KEY' with your actual HubSpot private APP API key.
    Your private app needs at least the following scopes:
        - crm.objects.companies.read
        - crm.objects.deals.read
        - crm.objects.contacts.read
        - crm.schemas.contacts.read
        - crm.schemas.companies.read
        - crm.schemas.deals.read

2. Update the `custom_date_fields` dictionary with your custom date properties for each HubSpot object type.
   - "contacts": List of custom date properties for contacts (e.g., "date_of_birth", "last_purchase_date").
   - "companies": List of custom date properties for companies (e.g., "foundation_date", "last_funding_date").
   - "deals": List of custom date properties for deals (e.g., "close_date", "contract_signed_date").

3. Run the script from your terminal or command prompt:
   python your_script_name.py

The script will automatically:
- Install required packages if not already installed.
- Create new datetime properties in HubSpot based on the provided custom date fields.
- Backfill the datetime properties with the last value change timestamp from the property history.

Example:
If you have a "close_date" property in deals, the script will create a new property named "close_date_datetime"
and backfill it with the timestamp of the last change to "close_date".

"""

import subprocess
import sys
import requests
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import random
from datetime import datetime
import concurrent.futures

# Install required packages if not already installed
def install_packages(packages):
    for package in packages:
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])
required_packages = ['requests']
install_packages(required_packages)

# Config logs
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("hubspot_datetime_automation.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

# ======================= USER CONFIG =======================

# HubSpot API Key - Replace this with your actual API key
HUBSPOT_API_KEY = 'YOUR KEY'

# Custom date properties per HubSpot object type
custom_date_fields = {
    "contacts": ["custom_date1", "custom_date2"],  # Update these fields as needed
    "companies": ["custom_date1"],  # Update these fields as needed
    "deals": ["custom_date1"]  # Update these fields as needed
}

# ==================================================================


BASE_URL = 'https://api.hubapi.com'

def get_group_name(object_type):
    if object_type == "contacts":
        return "contactinformation"
    elif object_type == "companies":
        return "companyinformation"
    elif object_type == "deals":
        return "dealinformation"
    else:
        raise ValueError(f"Unknown object type: {object_type}")

def property_exists(object_type, datetime_field_name):
    url = f"{BASE_URL}/crm/v3/properties/{object_type}/{datetime_field_name}"
    headers = {
        'Authorization': f'Bearer {HUBSPOT_API_KEY}',
        'Content-Type': 'application/json'
    }
    response = requests.get(url, headers=headers)
    return response.status_code == 200

def create_datetime_property(object_type, date_field):
    datetime_field_name = f"{date_field}_datetime"
    
    if property_exists(object_type, datetime_field_name):
        logging.info(f"Property {datetime_field_name} already exists for {object_type}. Skipping creation.")
        return

    datetime_field_label = f"{date_field} - datetime"
    group_name = get_group_name(object_type)

    url = f"{BASE_URL}/crm/v3/properties/{object_type}"
    headers = {
        'Authorization': f'Bearer {HUBSPOT_API_KEY}',
        'Content-Type': 'application/json'
    }
    payload = {
        "label": datetime_field_label,
        "name": datetime_field_name,
        "groupName": group_name,
        "type": "datetime",
        "fieldType": "date",
        "formField": True
    }

    response = requests.post(url, json=payload, headers=headers)
    
    if response.status_code == 201:
        logging.info(f"Created datetime property: {datetime_field_label} for {object_type}")
    else:
        logging.error(f"Failed to create datetime property for {object_type}: {response.status_code}, {response.json()}")

def fetch_objects_batch(object_type, date_fields, after=None):
    url = f'{BASE_URL}/crm/v3/objects/{object_type}'
    headers = {
        'Authorization': f'Bearer {HUBSPOT_API_KEY}',
        'Content-Type': 'application/json'
    }
    params = {
        'limit': 50, 
        'propertiesWithHistory': ','.join(date_fields),
    }
    if after:
        params['after'] = after

    retries = 0
    while True:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 1))
            retries += 1
            wait_time = retry_after * (2 ** retries) + random.uniform(0, 1)
            logging.warning(f"Rate limit hit. Retrying after {wait_time:.2f} seconds... (Attempt {retries})")
            time.sleep(wait_time)
        else:
            if response.status_code == 200:
                return response.json()
            else:
                logging.error(f"Failed to fetch {object_type} batch. Status: {response.status_code}. Response: {response.json()}")
                return None

def determine_timestamp_format(history):
    if not history:
        return None

    latest_entry = history[0]
    latest_value = latest_entry['value']
    latest_change = None
    for version in history:
        if 'timestamp' in version:
            latest_change = version['timestamp']
            break

    try:
        value_date = datetime.strptime(latest_value, "%Y-%m-%d").date()
        if latest_change:
            change_date = datetime.fromisoformat(latest_change.replace('Z', '+00:00')).date()
            if change_date == value_date:
                return latest_change
        fallback_timestamp = latest_value + "T06:00:00.000Z"
        return fallback_timestamp
    except ValueError as e:
        logging.error(f"Error parsing date {latest_value}: {e}")
        return latest_value

def convert_to_unix_timestamp(timestamp):
    try:
        dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        return int(dt.timestamp() * 1000)
    except ValueError as e:
        logging.error(f"Error converting timestamp: {timestamp}. Error: {e}")
        return None

def batch_update_records(object_type, batch_payload):
    if not batch_payload:
        logging.warning(f"No valid updates to send for {object_type}. Skipping batch update.")
        return
    
    logging.debug(f"Preparing to update {len(batch_payload)} records for {object_type}. Payload: {batch_payload}")

    url = f"{BASE_URL}/crm/v3/objects/{object_type}/batch/update"
    headers = {
        'Authorization': f'Bearer {HUBSPOT_API_KEY}',
        'Content-Type': 'application/json'
    }
    payload = {"inputs": batch_payload}
    
    response = requests.post(url, json=payload, headers=headers)
    
    if response.status_code == 200:
        logging.info(f"Batch update for {object_type} successful.")
    else:
        logging.error(f"Batch update for {object_type} failed: {response.status_code}, {response.json()}")

def process_objects():
    logging.info("Starting process to create datetime properties and backfill data.")
    
    for object_type, date_fields in custom_date_fields.items():
        if not date_fields:
            logging.info(f"No custom date fields specified for {object_type}. Skipping.")
            continue

        for date_field in date_fields:
            create_datetime_property(object_type, date_field)

    logging.info("Starting process to fetch and update objects with datetime properties.")
    
    for object_type, date_fields in custom_date_fields.items():
        if not date_fields:
            logging.info(f"No custom date fields specified for {object_type}. Skipping.")
            continue

        after = None
        has_more = True

        while has_more:
            logging.info(f"Fetching {object_type} objects batch starting after: {after}")
            response_data = fetch_objects_batch(object_type, date_fields, after)
            
            if not response_data or 'results' not in response_data:
                logging.info(f"No {object_type} objects fetched.")
                break

            objects = response_data['results']
            logging.info(f"Fetched {len(objects)} {object_type} objects.")
            logging.debug(f"Fetched {object_type} objects data: {objects}") 

            batch_payload = []
            for obj in objects:
                object_id = obj['id']
                for date_field in date_fields:
                    history = obj.get('propertiesWithHistory', {}).get(date_field, [])
                    logging.debug(f"History for {object_type} ID {object_id}, field {date_field}: {history}")  
                    
                    last_change_timestamp = determine_timestamp_format(history)
                    if last_change_timestamp:
                        datetime_field_name = f"{date_field}_datetime"
                        timestamp_value = convert_to_unix_timestamp(last_change_timestamp)
                        
                        if timestamp_value:
                            existing_object = next((item for item in batch_payload if item["id"] == object_id), None)
                            if existing_object:
                                existing_object["properties"][datetime_field_name] = timestamp_value
                            else:
                                batch_payload.append({
                                    "id": object_id,
                                    "properties": {datetime_field_name: timestamp_value}
                                })
                        else:
                            logging.warning(f"Skipping update for {object_id}: {datetime_field_name} has a null or invalid value.")

            if batch_payload:
                logging.debug(f"Prepared payload for {object_type} update: {batch_payload}")  
                batch_update_records(object_type, batch_payload)
            else:
                logging.info(f"No valid updates to process for {object_type} in this batch.")

            paging = response_data.get('paging', {})
            has_more = 'next' in paging and 'after' in paging['next']
            after = paging['next']['after'] if has_more else None


if __name__ == "__main__":
    process_objects()
