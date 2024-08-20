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
import random
from datetime import datetime
import concurrent.futures

# Install required packages if not already installed
def install_packages(packages):
    for package in packages:
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])

# List of required packages
required_packages = ['requests']

# Install the required packages
install_packages(required_packages)

# Configure logging to file
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("hubspot_datetime_automation.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

# ======================= USER CONFIGURATION =======================

# HubSpot API Key - Replace this with your actual API key
HUBSPOT_API_KEY = 'YOUR PRIVATE APP KEY'

# Custom date properties per HubSpot object type
custom_date_fields = {
    "contacts": ["date_test", "another_date_field"],  # Update these fields as needed
    "companies": ["another_date_field"],  # Update these fields as needed
    "deals": ["another_date_field"]  # Update these fields as needed
}

# ==================================================================

# Base URL for HubSpot API
BASE_URL = 'https://api.hubapi.com'

def get_group_name(object_type):
    logging.debug(f"get_group_name called with object_type: {object_type}")
    if object_type == "contacts":
        return "contactinformation"
    elif object_type == "companies":
        return "companyinformation"
    elif object_type == "deals":
        return "dealinformation"
    else:
        raise ValueError(f"Unknown object type: {object_type}")

def property_exists(object_type, datetime_field_name):
    logging.debug(f"property_exists called with object_type: {object_type}, datetime_field_name: {datetime_field_name}")
    url = f"{BASE_URL}/crm/v3/properties/{object_type}/{datetime_field_name}"
    headers = {
        'Authorization': f'Bearer {HUBSPOT_API_KEY}',
        'Content-Type': 'application/json'
    }
    response = requests.get(url, headers=headers)
    logging.info(f"Property check response for {datetime_field_name}: {response.status_code}, {response.json()}")
    return response.status_code == 200

def create_datetime_property(object_type, date_field):
    logging.debug(f"create_datetime_property called with object_type: {object_type}, date_field: {date_field}")
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

    logging.debug(f"Creating property with payload: {payload}")
    response = requests.post(url, json=payload, headers=headers)
    logging.info(f"Create property response for {datetime_field_name}: {response.status_code}, {response.json()}")
    
    if response.status_code == 201:
        logging.info(f"Created datetime property: {datetime_field_label} for {object_type}")
    else:
        logging.error(f"Failed to create datetime property for {object_type}: {response.status_code}, {response.json()}")

def fetch_property_history(object_type, object_id, property_name):
    logging.debug(f"fetch_property_history called with object_type: {object_type}, object_id: {object_id}, property_name: {property_name}")
    url = f'{BASE_URL}/crm/v3/objects/{object_type}/{object_id}'
    headers = {
        'Authorization': f'Bearer {HUBSPOT_API_KEY}',
        'Content-Type': 'application/json'
    }
    params = {
        'propertiesWithHistory': property_name
    }

    retries = 0
    while True:
        logging.info(f"Fetching property history for {object_type} ID {object_id} and property {property_name}...")
        response = requests.get(url, headers=headers, params=params)
        logging.info(f"Fetch property history response for {object_id}: {response.status_code}, {response.json()}")
        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 1))
            retries += 1
            wait_time = retry_after * (2 ** retries) + random.uniform(0, 1)
            logging.warning(f"Rate limit hit. Retrying after {wait_time:.2f} seconds... (Attempt {retries})")
            time.sleep(wait_time)
        else:
            if response.status_code == 200:
                data = response.json()
                history = data.get('propertiesWithHistory', {}).get(property_name, [])
                if history:
                    logging.info(f"Property history retrieved for {object_type} ID {object_id}. History: {history}")
                    return determine_timestamp_format(history)
                else:
                    logging.warning(f"No history found for {object_type} ID {object_id} and property {property_name}.")
                    return None
            else:
                logging.error(f"Failed to fetch property history for {object_type} ID {object_id}. Status: {response.status_code}. Response: {response.json()}")
                return None

def determine_timestamp_format(history):
    logging.debug(f"determine_timestamp_format called with history: {history}")
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
                logging.info(f"Matching timestamp found for value date {value_date}: {latest_change}")
                return latest_change
        fallback_timestamp = latest_value + "T06:00:00.000Z"
        logging.info(f"No matching timestamp found. Using fallback timestamp: {fallback_timestamp}")
        return fallback_timestamp
    except ValueError as e:
        logging.error(f"Error parsing date {latest_value}: {e}")
        return latest_value

def convert_to_unix_timestamp(timestamp):
    logging.debug(f"convert_to_unix_timestamp called with timestamp: {timestamp}")
    try:
        dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        unix_timestamp = int(dt.timestamp() * 1000)
        logging.info(f"Converted timestamp {timestamp} to Unix timestamp: {unix_timestamp}")
        return unix_timestamp
    except ValueError as e:
        logging.error(f"Error converting timestamp: {timestamp}. Error: {e}")
        return None

def update_datetime_property(object_type, object_id, datetime_field_name, datetime_value):
    logging.debug(f"update_datetime_property called with object_type: {object_type}, object_id: {object_id}, datetime_field_name: {datetime_field_name}, datetime_value: {datetime_value}")
    unix_timestamp = convert_to_unix_timestamp(datetime_value)
    if not unix_timestamp:
        logging.error(f"Invalid timestamp for {object_type} ID {object_id}: {datetime_value}. Skipping update.")
        return

    url = f"{BASE_URL}/crm/v3/objects/{object_type}/{object_id}"
    headers = {
        'Authorization': f'Bearer {HUBSPOT_API_KEY}',
        'Content-Type': 'application/json'
    }
    payload = {
        "properties": {
            datetime_field_name: unix_timestamp
        }
    }

    logging.info(f"Updating {object_type} ID {object_id} with {datetime_field_name}: {unix_timestamp}")
    response = requests.patch(url, json=payload, headers=headers)
    logging.info(f"Update property response for {object_id}: {response.status_code}, {response.json()}")

    if response.status_code == 200:
        logging.info(f"Successfully updated {object_type} ID {object_id} with {datetime_field_name}.")
    else:
        logging.error(f"Failed to update {object_type} ID {object_id} with {datetime_field_name}. Status: {response.status_code}")

def process_objects():
    logging.info("Starting process to create datetime properties and backfill data.")
    for object_type, date_fields in custom_date_fields.items():
        for date_field in date_fields:
            logging.debug(f"Creating datetime property for object_type: {object_type}, date_field: {date_field}")
            create_datetime_property(object_type, date_field)

    logging.info("Starting process to fetch and update objects with datetime properties.")
    for object_type, date_fields in custom_date_fields.items():
        after = None
        has_more = True

        while has_more:
            objects_url = f"{BASE_URL}/crm/v3/objects/{object_type}"
            headers = {
                'Authorization': f'Bearer {HUBSPOT_API_KEY}',
                'Content-Type': 'application/json'
            }
            params = {
                'limit': 50,
                'propertiesWithHistory': ','.join(date_fields)
            }
            if after:
                params['after'] = after

            logging.info(f"Fetching {object_type} objects with {date_fields} history...")
            response = requests.get(objects_url, headers=headers, params=params)
            logging.info(f"Fetch objects response: {response.status_code}, {response.json()}")

            if response.status_code != 200:
                logging.error(f"Failed to fetch {object_type}. Status: {response.status_code}")
                break

            objects = response.json().get('results', [])
            logging.info(f"Fetched {len(objects)} {object_type} objects.")

            for obj in objects:
                object_id = obj['id']
                properties = obj.get('properties', {})
                properties_with_history = obj.get('propertiesWithHistory', {})

                for date_field in date_fields:
                    logging.debug(f"Processing {object_type} ID {object_id} for property {date_field}")
                    if date_field in properties_with_history:
                        last_change_timestamp = fetch_property_history(object_type, object_id, date_field)
                        if last_change_timestamp:
                            datetime_field_name = f"{date_field}_datetime"
                            logging.info(f"Updating {datetime_field_name} for {object_type} ID {object_id} with timestamp {last_change_timestamp}...")
                            update_datetime_property(object_type, object_id, datetime_field_name, last_change_timestamp)
                        else:
                            logging.warning(f"No valid timestamp found for {object_type} ID {object_id} on property {date_field}. Skipping update.")

            # Check if there's more data to fetch
            paging = response.json().get('paging', {})
            has_more = 'next' in paging and 'after' in paging['next']
            after = paging['next']['after'] if has_more else None

            if has_more:
                logging.info(f"More {object_type} objects to fetch. Continuing to next page.")
            else:
                logging.info(f"All {object_type} objects fetched and processed.")

if __name__ == "__main__":
    process_objects()