import requests
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any
import logging

# ----------------------- Configuration -----------------------

# Fetch API keys from environment variables for security
# It's recommended to store sensitive information like API keys in environment variables.
# For demonstration purposes, they are hardcoded here. Replace with environment variables in production.
HUBSPOT_ACCESS_TOKEN = 'abc'
OPENAI_API_KEY = 'abc'

if not HUBSPOT_ACCESS_TOKEN:
    raise ValueError("HUBSPOT_ACCESS_TOKEN environment variable not set.")

if not OPENAI_API_KEY:
    raise ValueError("OPENAI_API_KEY environment variable not set.")

# Define the properties to search for within workflow JSON details, including their object types
PROPERTIES_TO_SEARCH: List[Dict[str, str]] = [
    {'objectType': 'contacts', 'propertyName': 'lifecyclestage'},
    {'objectType': 'contacts', 'propertyName': 'service_s__of_interest__biopharma_'},
    {'objectType': 'contacts', 'propertyName': 'specialty'},
    {'objectType': 'contacts', 'propertyName': 'campaigns'},
    {'objectType': 'contacts', 'propertyName': 'hs_lead_status'},
    {'objectType': 'companies', 'propertyName': 'lifecyclestage'},
    {'objectType': 'companies', 'propertyName': 'company_type'},
    {'objectType': 'deals', 'propertyName': 'pipeline'},
    {'objectType': 'deals', 'propertyName': 'dealstage'}

    # Add more properties as needed
]

# Define the output directories where matched, enabled workflows and summaries will be saved
MATCHED_OUTPUT_DIRECTORY = 'matched_workflows'
ENABLED_OUTPUT_DIRECTORY = 'enabled_workflows'
SUMMARIES_OUTPUT_DIRECTORY = 'summaries'

# Define the number of concurrent threads
MAX_WORKERS = 5

# Define retry configuration
MAX_RETRIES = 5
INITIAL_BACKOFF = 2  # in seconds
BACKOFF_FACTOR = 3    # exponential backoff factor

# OpenAI API Configuration
OPENAI_API_URL = "https://api.openai.com/v1/chat/completions"
MODEL_NAME = "gpt-4o"  # Ensure correct model name

# Prompt Logging Configuration
OPENAI_PROMPTS_LOG_FILE = 'openai_prompts.log'

# ----------------------- Logging Configuration -----------------------

# Main logger configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')
logger = logging.getLogger(__name__)

# Dedicated logger for OpenAI prompts
prompt_logger = logging.getLogger('openai_prompts')
prompt_logger.setLevel(logging.DEBUG)
# Create file handler for prompts
prompt_fh = logging.FileHandler(OPENAI_PROMPTS_LOG_FILE)
prompt_fh.setLevel(logging.DEBUG)
# Create formatter and add to handler
formatter = logging.Formatter('%(asctime)s %(levelname)s:%(message)s')
prompt_fh.setFormatter(formatter)
# Add handler to the prompt_logger
prompt_logger.addHandler(prompt_fh)

# ----------------------- Helper Functions -----------------------

def fetch_property_details(properties: List[Dict[str, str]], access_token: str) -> Dict[str, Any]:
    property_details = {}
    base_url = "https://api.hubapi.com/crm/v3/properties/{objectType}/{propertyName}"
    headers = {
        'Authorization': f"Bearer {access_token}",
        'Accept': "application/json"
    }

    for prop in properties:
        object_type = prop['objectType']
        property_name = prop['propertyName']
        url = base_url.format(objectType=object_type, propertyName=property_name)

        backoff = INITIAL_BACKOFF
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                response = requests.get(url, headers=headers, timeout=20)
                if response.status_code == 200:
                    data = response.json()
                    prompt_logger.debug(f"Fetched details for property '{property_name}': {data}'.")
                    property_details[property_name] = {
                        'label': data.get('label', property_name),
                        'type': data.get('type', 'string'),
                        'options': data.get('options', []) if data.get('type') == 'enumeration' else []
                    }
                    logger.info(f"Fetched details for property '{property_name}' in object '{object_type}'.")
                    prompt_logger.debug(f"Fetched details for property '{property_name}': {property_details}'.")
                    break
                elif response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', backoff))
                    logger.warning(f"Rate limited when fetching property '{property_name}'. Retrying after {retry_after} seconds (Attempt {attempt}/{MAX_RETRIES}).")
                    time.sleep(retry_after)
                    backoff *= BACKOFF_FACTOR
                else:
                    logger.error(f"Failed to fetch property '{property_name}'. Status code: {response.status_code}")
                    try:
                        error_message = response.json()
                        logger.error(f"Error message: {error_message}")
                    except json.JSONDecodeError:
                        logger.error(f"Error response: {response.text}")
                    break
            except requests.exceptions.RequestException as e:
                logger.error(f"Request exception while fetching property '{property_name}': {e}. Retrying in {backoff} seconds (Attempt {attempt}/{MAX_RETRIES}).")
                time.sleep(backoff)
                backoff *= BACKOFF_FACTOR
        else:
            logger.error(f"Exceeded maximum retries for fetching property '{property_name}'.")

    # If dealstage or pipeline are present, fetch pipelines to populate options
    need_deal_pipelines = any(prop['propertyName'] in ['pipeline', 'dealstage'] and prop['objectType'] == 'deals' for prop in properties)
    if need_deal_pipelines:
        deal_pipelines = fetch_pipelines('deals', access_token)

        # Populate pipeline options from pipelines
        if 'pipeline' in property_details:
            pipeline_options = []
            for p in deal_pipelines:
                pipeline_options.append({
                    'label': p.get('label'),
                    'value': p.get('id')
                })
            property_details['pipeline']['options'] = pipeline_options
            logger.info("Populated pipeline property options from pipelines.")

        # Populate dealstage options from pipelines
        if 'dealstage' in property_details:
            dealstage_options = []
            for p in deal_pipelines:
                stages = p.get('stages', [])
                for stage in stages:
                    if isinstance(stage, dict):
                        dealstage_options.append({
                            'label': stage.get('label'),
                            'value': stage.get('id')
                        })
            property_details['dealstage']['options'] = dealstage_options
            logger.info("Populated dealstage property options from pipelines.")

    return property_details


def fetch_pipelines(object_type: str, access_token: str) -> List[Dict[str, Any]]:
    """
    Fetch all pipelines for the specified object type using HubSpot's v3 API.
    """
    url = f"https://api.hubapi.com/crm/v3/pipelines/{object_type}"
    headers = {
        'Authorization': f"Bearer {access_token}",
        'Accept': "application/json"
    }
    params = {
        'includeInactive': 'false'
    }

    pipelines = []
    backoff = INITIAL_BACKOFF
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, headers=headers, params=params, timeout=20)
            if response.status_code == 200:
                data = response.json()
                if 'results' in data and isinstance(data['results'], list):
                    pipelines = data['results']
                else:
                    logger.error(f"Unexpected response structure: {json.dumps(data, indent=2)}")
                logger.info(f"Fetched pipelines for {object_type}.")
                return pipelines
            elif response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', backoff))
                logger.warning(f"Rate limited when fetching pipelines for '{object_type}'. Retrying after {retry_after} seconds (Attempt {attempt}/{MAX_RETRIES}).")
                time.sleep(retry_after)
                backoff *= BACKOFF_FACTOR
            else:
                logger.error(f"Failed to fetch pipelines for '{object_type}'. Status code: {response.status_code}")
                try:
                    error_message = response.json()
                    logger.error(f"Error message: {error_message}")
                except json.JSONDecodeError:
                    logger.error(f"Error response: {response.text}")
                break
        except requests.exceptions.RequestException as e:
            logger.error(f"Request exception while fetching pipelines for '{object_type}': {e}. Retrying in {backoff} seconds (Attempt {attempt}/{MAX_RETRIES}).")
            time.sleep(backoff)
            backoff *= BACKOFF_FACTOR
    else:
        logger.error(f"Exceeded maximum retries for fetching pipelines for '{object_type}'.")

    return pipelines

def fetch_all_workflows_v4(access_token: str) -> List[Dict[str, Any]]:
    """
    Fetch all workflows (flows) from HubSpot using the v4 API with pagination and manual retry logic.

    Parameters:
        access_token (str): HubSpot API Bearer Token.

    Returns:
        List[Dict[str, Any]]: A list of workflows.
    """
    url = "https://api.hubapi.com/automation/v4/flows"
    headers = {
        'accept': "application/json",
        'authorization': f"Bearer {access_token}"
    }

    workflows = []
    params = {
        'limit': 100  # Adjust as needed; HubSpot may have a default limit
    }

    while True:
        backoff = INITIAL_BACKOFF
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                response = requests.get(url, headers=headers, params=params, timeout=20)
                if response.status_code == 200:
                    data = response.json()
                    fetched_workflows = data.get('results', [])
                    workflows.extend(fetched_workflows)
                    logger.info(f"Fetched {len(fetched_workflows)} workflows.")

                    # Check if there are more workflows to fetch
                    if 'paging' in data and 'next' in data['paging']:
                        url = data['paging']['next']['link']
                        params = {}  # Parameters are included in the 'next' link
                    else:
                        return workflows
                    break  # Exit the retry loop if successful
                elif response.status_code == 429:
                    # Handle rate limiting
                    retry_after = int(response.headers.get('Retry-After', backoff))
                    logger.warning(f"Rate limited when fetching workflows. Retrying after {retry_after} seconds (Attempt {attempt}/{MAX_RETRIES}).")
                    time.sleep(retry_after)
                    backoff *= BACKOFF_FACTOR
                else:
                    logger.error(f"Failed to fetch workflows. Status code: {response.status_code}")
                    try:
                        error_message = response.json()
                        logger.error(f"Error message: {error_message}")
                    except json.JSONDecodeError:
                        logger.error(f"Error response: {response.text}")
                    return workflows
            except requests.exceptions.RequestException as e:
                logger.error(f"Request exception while fetching workflows: {e}. Retrying in {backoff} seconds (Attempt {attempt}/{MAX_RETRIES}).")
                time.sleep(backoff)
                backoff *= BACKOFF_FACTOR
        else:
            logger.error(f"Exceeded maximum retries for fetching workflows.")
            return workflows

def fetch_workflow_details_v4(workflow_id: str, access_token: str) -> Dict[str, Any]:
    """
    Fetch detailed information for a specific workflow using the v4 API with manual retry logic.

    Parameters:
        workflow_id (str): The ID of the workflow.
        access_token (str): HubSpot API Bearer Token.

    Returns:
        Dict[str, Any]: Workflow details as a JSON object or empty dict if failed.
    """
    url = f"https://api.hubapi.com/automation/v4/flows/{workflow_id}"
    headers = {
        'accept': "application/json",
        'authorization': f"Bearer {access_token}"
    }

    backoff = INITIAL_BACKOFF
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, headers=headers, timeout=20)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', backoff))
                logger.warning(f"Rate limited when fetching workflow {workflow_id}. Retrying after {retry_after} seconds (Attempt {attempt}/{MAX_RETRIES}).")
                time.sleep(retry_after)
                backoff *= BACKOFF_FACTOR
            else:
                logger.error(f"Failed to fetch workflow {workflow_id}. Status code: {response.status_code}")
                try:
                    error_message = response.json()
                    logger.error(f"Error message: {error_message}")
                except json.JSONDecodeError:
                    logger.error(f"Error response: {response.text}")
                return {}
        except requests.exceptions.RequestException as e:
            logger.error(f"Request exception while fetching workflow {workflow_id}: {e}. Retrying in {backoff} seconds (Attempt {attempt}/{MAX_RETRIES}).")
            time.sleep(backoff)
            backoff *= BACKOFF_FACTOR

    logger.error(f"Exceeded maximum retries for workflow {workflow_id}.")
    return {}

def search_properties_in_json(json_data: Any, properties: List[str]) -> bool:
    """
    Recursively search for specified properties in a nested JSON object.

    Parameters:
        json_data (Any): The JSON data to search through.
        properties (List[str]): The list of properties to search for.

    Returns:
        bool: True if any property is found, False otherwise.
    """
    if isinstance(json_data, dict):
        for key, value in json_data.items():
            if key == 'property' and isinstance(value, str) and value in properties:
                return True
            if isinstance(value, (dict, list)):
                if search_properties_in_json(value, properties):
                    return True
    elif isinstance(json_data, list):
        for item in json_data:
            if search_properties_in_json(item, properties):
                return True
    return False

def save_workflow_json(workflow_data: Dict[str, Any], output_dir: str) -> None:
    """
    Save workflow JSON data to a file within the specified directory.

    Parameters:
        workflow_data (Dict[str, Any]): The workflow JSON data.
        output_dir (str): The directory where the JSON file will be saved.
    """
    workflow_id = workflow_data.get('id', 'unknown_id')
    workflow_name = workflow_data.get('name', 'Unnamed_Workflow')
    # Sanitize the workflow name to create a valid filename
    sanitized_name = "".join([c if c.isalnum() or c in (' ', '_', '-') else '_' for c in workflow_name]).strip().replace(' ', '_')
    json_filename = f"{sanitized_name}_{workflow_id}.json"
    file_path = os.path.join(output_dir, json_filename)

    try:
        with open(file_path, 'w', encoding='utf-8') as json_file:
            json.dump(workflow_data, json_file, indent=4)
        logger.info(f"Saved workflow '{workflow_name}' (ID: {workflow_id}) to {file_path}")
    except Exception as e:
        logger.error(f"Failed to save workflow {workflow_id} to file. Error: {e}")

def summarize_workflow_requests(workflow_data: Dict[str, Any], property_details: Dict[str, Any], properties_str: str) -> str:
    # Define the system message with detailed instructions
    system_message = (
        "You are an expert in Revenue Operations for B2B SaaS, HubSpot CRM and Marketing Automation. "
        "Your task is to analyze and summarize HubSpot workflow JSON data. "
        "HubSpot workflows automate various business processes by defining triggers, actions, and conditions involving different CRM objects such as Contacts, Deals, Companies, and Tickets. "
        "HubSpots main objects are marked by 0-1 Contacts, 0-2 Companies and 0-3 Deals.\n\n"
        "Each workflow consists of the following components:\n\n"
        "1. **Triggers:** Events or criteria that start the workflow, such as form submissions, contact property changes, or deal stage transitions.\n"
        "2. **Actions:** Tasks the workflow performs automatically, such as sending emails, updating contact properties, creating tasks, or enrolling contacts in other workflows.\n"
        "3. **Conditions:** Logical statements that determine the flow's path, allowing for branching based on contact or deal properties.\n\n"
        "When summarizing, please include the following details:\n"
        "- **Workflow Name and ID:** Clearly state the workflow's name and unique identifier.\n"
        "Refer to the following property details to interpret property values accurately. Avoid using the internal values but rather the labels to describe the system's logic:\n"
    )
    
    # Append property details to the system message
    for prop_name, details in property_details.items():
        system_message += f"- **{details['label']} ({prop_name}):** "
        if prop_name == 'dealstage':
            # For dealstage, list pipeline stages
            if details['type'] == 'enumeration' and details['options']:
                options = ", ".join([f"{opt['label']} ({opt['value']})" for opt in details['options']])
                system_message += f"Enumeration representing deal stages with options: {options}.\n"
            else:
                system_message += f"Type: {details['type']}.\n"
        else:
            # Handle other properties
            if details['type'] == 'enumeration' and details['options']:
                options = ", ".join([f"{opt['label']} ({opt['value']})" for opt in details['options']])
                system_message += f"Enumeration with options: {options}.\n"
            else:
                system_message += f"Type: {details['type']}.\n"
    
    user_prompt = (
        "Please provide a simple summary of the following HubSpot workflow. "
        "We need to understand what triggers the workflow and what the actions mainly do, without having to go into all the details. "
        "The goal is to gain a better understanding of the HubSpot system logic from the HubSpot system at hand. "
        "Ensure that the summary is easy to read and understand. Especially focus on how the following properties are used: "
        f"{properties_str}.\n\n"
        f"{json.dumps(workflow_data, indent=2)}"
    )
    
    # Log the prompts for verification
    prompt_logger.debug("System Message:")
    prompt_logger.debug(system_message)
    prompt_logger.debug("User Prompt:")
    prompt_logger.debug(user_prompt)

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {OPENAI_API_KEY}"
    }

    data = {
        "model": MODEL_NAME,
        "temperature": 0.1,  # Lower temperature for more factual summaries
        "max_tokens": 5000,   # Adjust based on desired summary length
        "messages": [
            {
                "role": "system",
                "content": system_message
            },
            {
                "role": "user",
                "content": user_prompt
            }
        ]
    }

    backoff = INITIAL_BACKOFF
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.post(OPENAI_API_URL, headers=headers, json=data, timeout=60)
            if response.status_code == 200:
                reply = response.json()
                # Validate the response structure
                if 'choices' in reply and len(reply['choices']) > 0 and 'message' in reply['choices'][0]:
                    summary = reply['choices'][0]['message']['content'].strip()
                    logger.info(f"Received summary from OpenAI for workflow.")
                    return summary
                else:
                    logger.error("Unexpected OpenAI API response structure.")
                    return "Error: Unexpected OpenAI API response structure."
            elif response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', backoff))
                logger.warning(f"Rate limited by OpenAI API. Retrying after {retry_after} seconds (Attempt {attempt}/{MAX_RETRIES}).")
                time.sleep(retry_after)
                backoff *= BACKOFF_FACTOR
            else:
                logger.error(f"OpenAI API returned status code {response.status_code}: {response.text}")
                return f"Error: OpenAI API returned status code {response.status_code}"
        except requests.exceptions.RequestException as e:
            logger.error(f"Exception during OpenAI request: {e}. Retrying in {backoff} seconds (Attempt {attempt}/{MAX_RETRIES}).")
            time.sleep(backoff)
            backoff *= BACKOFF_FACTOR

    logger.error(f"Exceeded maximum retries for summarizing workflow.")
    return "Error: Exceeded maximum retries for OpenAI API."

def save_summary(workflow_id: str, summary: str, output_dir: str) -> None:
    """
    Save the OpenAI-generated summary to a TEXT file.

    Parameters:
        workflow_id (str): The ID of the workflow.
        summary (str): The summary text.
        output_dir (str): The directory where the summary file will be saved.
    """
    txt_filename = f"{workflow_id}.txt"
    file_path = os.path.join(output_dir, txt_filename)

    try:
        with open(file_path, 'w', encoding='utf-8') as txt_file:
            txt_file.write(f"# Workflow ID: {workflow_id}\n\n")
            txt_file.write(summary)
        logger.info(f"Saved summary for Workflow ID {workflow_id} to {file_path}")
    except Exception as e:
        logger.error(f"Failed to save summary for workflow {workflow_id}. Error: {e}")

def process_workflow(workflow: Dict[str, Any], access_token: str,
                    properties: List[Dict[str, str]],
                    matched_output_dir: str,
                    enabled_output_dir: str,
                    summaries_output_dir: str,
                    property_details: Dict[str, Any],
                    properties_str: str) -> Dict[str, bool]:
    """
    Process a single workflow: fetch details, check for properties and enabled status, summarize, and save accordingly.

    Parameters:
        workflow (Dict[str, Any]): The workflow object.
        access_token (str): HubSpot API Bearer Token.
        properties (List[Dict[str, str]]): List of properties with objectType and propertyName.
        matched_output_dir (str): Directory to save matched workflows.
        enabled_output_dir (str): Directory to save enabled workflows.
        summaries_output_dir (str): Directory to save summaries.
        property_details (Dict[str, Any]): Details of properties including labels and options.
        properties_str (str): Comma-separated string of property names.

    Returns:
        Dict[str, bool]: A dictionary indicating if the workflow was matched, enabled, and/or summarized.
    """
    workflow_id = str(workflow.get('id'))
    workflow_name = workflow.get('name', 'Unnamed_Workflow')
    result = {'matched': False, 'enabled': False, 'summarized': False}

    logger.info(f"\nProcessing Workflow ID: {workflow_id}, Name: {workflow_name}")
    workflow_details = fetch_workflow_details_v4(workflow_id, access_token)

    if not workflow_details:
        logger.error(f"Skipping workflow {workflow_id} due to fetch error.")
        return result

    # Check if workflow is enabled
    if workflow_details.get('isEnabled', False):
        logger.info(f"Workflow '{workflow_name}' (ID: {workflow_id}) is enabled. Saving JSON.")
        save_workflow_json(workflow_details, enabled_output_dir)
        result['enabled'] = True

        # Check if workflow contains any of the specified properties
        property_names = [prop['propertyName'] for prop in properties]
        if search_properties_in_json(workflow_details, property_names):
            logger.info(f"Workflow '{workflow_name}' (ID: {workflow_id}) contains specified properties. Saving JSON.")
            save_workflow_json(workflow_details, matched_output_dir)
            result['matched'] = True

            ### Generate and save summary
            summary = summarize_workflow_requests(workflow_details, property_details, properties_str)
            if not summary.startswith("Error:"):
                save_summary(workflow_id, summary, summaries_output_dir)
                result['summarized'] = True
            else:
                logger.error(f"Skipping summary for Workflow ID {workflow_id} due to error.")
    else:
        logger.info(f"Workflow '{workflow_name}' (ID: {workflow_id}) is disabled. Skipping summarization.")

    return result

def generate_system_documentation(property_details: Dict[str, Any], summaries_dir: str, output_file: str, properties_str: str) -> None:
    """
    Generate a documentation-like file by sending a prompt to OpenAI's API that includes all summaries and property details.

    Parameters:
        property_details (Dict[str, Any]): Details of properties including labels and options.
        summaries_dir (str): Directory where individual summaries are saved.
        output_file (str): File path where the generated documentation will be saved.
        properties_str (str): Comma-separated string of property names.
    """
    # Read all summaries from the summaries directory
    summaries = []
    for filename in os.listdir(summaries_dir):
        if filename.endswith('.txt'):
            file_path = os.path.join(summaries_dir, filename)
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    summary_content = f.read()
                    summaries.append(summary_content)
            except Exception as e:
                logger.error(f"Error reading summary file {file_path}: {e}")

    if not summaries:
        logger.error("No summaries found to generate system documentation.")
        return

    # Prepare the key properties string
    key_properties_formatted = "\n".join([
        f"- **{prop['propertyName']}** (Object Type: {prop['objectType']})"
        for prop in PROPERTIES_TO_SEARCH
    ])

    # Add detailed options for enumeration properties
    for prop in PROPERTIES_TO_SEARCH:
        prop_name = prop['propertyName']
        if prop_name in property_details and property_details[prop_name]['type'] == 'enumeration':
            options = property_details[prop_name].get('options', [])
            if options:
                options_formatted = ", ".join([f"{opt['label']} ({opt['value']})" for opt in options])
                key_properties_formatted += f"\n  - **{prop_name} Options:** {options_formatted}"

    # Prepare the custom prompt with key properties
    custom_prompt = f"""
You are a B2B SaaS Revenue Operations (RevOps) Specialist with expertise in HubSpot CRM. Your primary focus is on Understanding how the complete hubspot system at hand is built and how workflows are interconnected. Look at each workflow and how actions of a workflow may be connected to triggers of another workflow. In B2B SaaS, prospetcs and companies and deals flow through a Hubspot system. Our goal is to understand how this happens.

You are been provided with a list of workflows configured in a HubSpot instance. These workflows are centered around the following key system properties:

{key_properties_formatted}

In the following always refer to the Labels rather than the internal Ids and to the workflow names rather than the workflow Ids.

Your tasks are as follows:
1. Analyze the Current Setup. Understand how the system is built and how workflows are interconnected. Look at each workflow and how actions of a workflow may be connected to triggers of another workflow. In B2B SaaS, prospetcs and companies and deals flow through a Hubspot system. Our goal is to understand how this happens.
2. Produce Structured Funnel Design description:
    * Create a complete funnel design logic description that outlines the current funnel design configurations so that someone without any knowledge about the company or the hubspot instance can easily understand the setup. Include specific details about the types of activities involved in lead engagement (e.g., emails, calls, meetings). Outline the exact criteria and interactions used for lead management (e.g., scoring thresholds, specific actions taken by leads).
    * The complete logic description should be well-organized, clear, complete and easy to navigate, following the structure of the example provided below.
2. Generate RevOps Roadmap Recommendations:
    * Provide a list of actionable recommendations for the RevOps team to start with based on potential logic weaknesses and gaps. Provide examples.
    * Ensure that the recommendations are practical, prioritized, and aligned with best practices in RevOps and HubSpot CRM utilization.

Desired Output Format:
Your complete and extensive logic description should resemble the following structure, incorporating clear headings, bullet points, and text. Assume that the system is mostly logical, but can also have logic flaws.

HubSpot Logic Documentation: Key Properties & Funnel Design
Table of Contents
1. Complete Funnel Design Analysis (answers how the funnel is designed and built in the hubspot system, describes logic and relationship between different properties and property options)
1.1. High level funnel design: Important properties & Derived Status & Stage Definitions (when is what field option set?)
1.2. Lead Recognition, Lead Assignment & Owner management
1.3. Lead Engagement, Qualification & disqualification process (also if recycling here)
1.3. Deal creation & Sales process, Closed won and closed lost handling (also if recycling here)
1.4. Post-Sales Customer Management & Churn handling 
2. RevOps Roadmap Recommendations (3 items)
3. Key Workflows to understand and what they do - An overview
4. Create a section containing all workflows analyzed clustered by topic and interdependence, amke sure to include Id and Name


Workflows:

    """

    # Concatenate all summaries
    all_summaries_text = "\n\n".join(summaries)

    # Prepare the user prompt
    user_prompt = (
        "Below are summaries of HubSpot workflows. Using these summaries and the property details provided, please generate a system audit & documentation-like file that includes:\n"
        "- An overview of how the core HubSpot system is designed for the marketing, sales, and customer service (CS) funnel, especially based on the key properties given.\n"
        "- Setting logic, meanings, and derived or assumed definitions of stages and statuses. Focus especially on interconnectedness of the system.\n"
        "- Focus on the key properties and explain their roles within the system. If they are enumeration properties, explain how the different options are most likely defined and edited.\n"
        "- Point out any weaknesses in the systems core design and how they could be approached\n"
        "Ensure the documentation is clear, well-structured, and easy to understand.\n\n"
        "**KEY Properties:**\n"
        f"{properties_str}\n\n"
        "**Workflow Summaries:**\n"
        f"{all_summaries_text}"
    )

    # Log the prompt to the dedicated OpenAI prompts log
    prompt_logger.info("Sending system documentation prompt to OpenAI API:")
    prompt_logger.info("System Message:")
    prompt_logger.info(custom_prompt)
    prompt_logger.info("User Prompt:")
    prompt_logger.info(user_prompt)

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {OPENAI_API_KEY}"
    }

    data = {
        "model": MODEL_NAME,
        "temperature": 0.3,  # Lower temperature for more factual responses
        "max_tokens": 16000,   # Adjust based on desired length
        "messages": [
            {
                "role": "system",
                "content": custom_prompt
            },
            {
                "role": "user",
                "content": user_prompt
            }
        ]
    }

    backoff = INITIAL_BACKOFF
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            # response = requests.post(OPENAI_API_URL, headers=headers, json=data, timeout=120)
            print("HERE WE WOULD HAVE SENT THE SYSTEM DOC REQUEST, BUT WE DID NOT (YET)")
            return
            if response.status_code == 200:
                reply = response.json()
                # Validate the response structure
                if 'choices' in reply and len(reply['choices']) > 0 and 'message' in reply['choices'][0]:
                    documentation = reply['choices'][0]['message']['content'].strip()
                    # Save the documentation to a file
                    with open(output_file, 'w', encoding='utf-8') as doc_file:
                        doc_file.write(documentation)
                    logger.info(f"Saved system documentation to {output_file}")
                    return
                else:
                    logger.error("Unexpected OpenAI API response structure.")
                    return
            elif response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', backoff))
                logger.warning(f"Rate limited by OpenAI API. Retrying after {retry_after} seconds (Attempt {attempt}/{MAX_RETRIES}).")
                time.sleep(retry_after)
                backoff *= BACKOFF_FACTOR
            else:
                logger.error(f"OpenAI API returned status code {response.status_code}: {response.text}")
                return
        except requests.exceptions.RequestException as e:
            logger.error(f"Exception during OpenAI request: {e}. Retrying in {backoff} seconds (Attempt {attempt}/{MAX_RETRIES}).")
            time.sleep(backoff)
            backoff *= BACKOFF_FACTOR

    logger.error("Exceeded maximum retries for generating system documentation.")

def combine_summaries_with_prompt(summaries_dir: str, output_file: str, key_properties: List[Dict[str, str]], property_details: Dict[str, Any]) -> None:
    """
    Combine all summary .txt files into a single file with a custom prompt at the top.

    Parameters:
        summaries_dir (str): Directory containing individual summary .txt files.
        output_file (str): Path to the output consolidated .txt file.
        key_properties (List[Dict[str, str]]): List of key properties with objectType and propertyName.
        property_details (Dict[str, Any]): Detailed property information including labels and options.
    """
    # Prepare the key properties string
    key_properties_formatted = "\n".join([
        f"- **{prop['propertyName']}** (Object Type: {prop['objectType']})"
        for prop in key_properties
    ])

    # Add detailed options for enumeration properties
    for prop in key_properties:
        prop_name = prop['propertyName']
        if prop_name in property_details and property_details[prop_name]['type'] == 'enumeration':
            options = property_details[prop_name].get('options', [])
            if options:
                options_formatted = ", ".join([f"{opt['label']} ({opt['value']})" for opt in options])
                key_properties_formatted += f"\n  - **{prop_name} Options:** {options_formatted}"

    # Define the custom prompt with the key properties inserted
    custom_prompt = f"""
You are a B2B SaaS Revenue Operations (RevOps) Specialist with expertise in HubSpot CRM. Your primary focus is on Understanding how the complete hubspot system at hand is built and how workflows are interconnected. Look at each workflow and how actions of a workflow may be connected to triggers of another workflow. In B2B SaaS, prospetcs and companies and deals flow through a Hubspot system. Our goal is to understand how this happens.

You are been provided with a list of workflows configured in a HubSpot instance. These workflows are centered around the following key system properties:

{key_properties_formatted}

In the following always refer to the Labels rather than the internal Ids and to the workflow names rather than the workflow Ids.

Your tasks are as follows:
1. Analyze the Current Setup. Understand how the system is built and how workflows are interconnected. Look at each workflow and how actions of a workflow may be connected to triggers of another workflow. In B2B SaaS, prospetcs and companies and deals flow through a Hubspot system. Our goal is to understand how this happens.
2. Produce Structured Funnel Design description:
    * Create a complete funnel design logic description that outlines the current funnel design configurations so that someone without any knowledge about the company or the hubspot instance can easily understand the setup. Include specific details about the types of activities involved in lead engagement (e.g., emails, calls, meetings). Outline the exact criteria and interactions used for lead management (e.g., scoring thresholds, specific actions taken by leads).
    * The complete logic description should be well-organized, clear, complete and easy to navigate, following the structure of the example provided below.
2. Generate RevOps Roadmap Recommendations:
    * Provide a list of actionable recommendations for the RevOps team to start with based on potential logic weaknesses and gaps. Provide examples.
    * Ensure that the recommendations are practical, prioritized, and aligned with best practices in RevOps and HubSpot CRM utilization.

Desired Output Format:
Your complete and extensive logic description should resemble the following structure, incorporating clear headings, bullet points, and text. 

HubSpot Logic Documentation: Key Properties & Funnel Design
Table of Contents
1. Complete Funnel Design Analysis (answers how the funnel is designed and built in the hubspot system, describes logic and relationship between different properties and property options)
1.1. High level funnel design: Important properties & Derived Status & Stage Definitions (when is what field option set?)
1.2. Lead Recognition, Lead Assignment & Owner management
1.3. Lead Engagement, Qualification & disqualification process (also if recycling here)
1.3. Deal creation & Sales process, Closed won and closed lost handling (also if recycling here)
1.4. Post-Sales Customer Management & Churn handling 
2. RevOps Roadmap Recommendations (max 3 items)
3. Key Workflows to understand and what they do - An overview
4. Create a section containing all workflows analyzed clustered by topic and interdependence, amke sure to include Id and Name


Workflows:
    """

    try:
        with open(output_file, 'w', encoding='utf-8') as outfile:
            # Write the custom prompt at the top
            outfile.write(custom_prompt.strip() + '\n\n')
            logger.info(f"Added custom prompt to {output_file}")
            
            # Iterate through all summary files and append their contents
            for filename in os.listdir(summaries_dir):
                if filename.endswith('.txt'):
                    file_path = os.path.join(summaries_dir, filename)
                    try:
                        with open(file_path, 'r', encoding='utf-8') as infile:
                            content = infile.read()
                            outfile.write(content + '\n\n')  # Add spacing between summaries
                        logger.info(f"Appended summary from {filename}")
                    except Exception as e:
                        logger.error(f"Failed to read {file_path}: {e}")
        logger.info(f"Successfully created consolidated file at {output_file}")
    except Exception as e:
        logger.error(f"Failed to create consolidated file {output_file}: {e}")

# ----------------------- Entry Point -----------------------

def main():
    # Create the output directories if they don't exist
    for directory in [MATCHED_OUTPUT_DIRECTORY, ENABLED_OUTPUT_DIRECTORY, SUMMARIES_OUTPUT_DIRECTORY]:
        if not os.path.exists(directory):
            os.makedirs(directory)
            logger.info(f"Created output directory: {directory}")

    # Step 0: Fetch property details
    logger.info("\nFetching property details...")
    property_details = fetch_property_details(PROPERTIES_TO_SEARCH, HUBSPOT_ACCESS_TOKEN)
    logger.info(f"Fetched details for {len(property_details)} properties.")
    # **New Logging Statement**
    logger.debug(f"Property Details: {json.dumps(property_details, indent=2)}")

    properties_str = ", ".join([prop['propertyName'] for prop in PROPERTIES_TO_SEARCH])

    # Step 1: Fetch all workflows using v4 API
    logger.info("\nFetching all workflows...")
    all_workflows = fetch_all_workflows_v4(HUBSPOT_ACCESS_TOKEN)
    logger.info(f"Total workflows fetched: {len(all_workflows)}")

    if not all_workflows:
        logger.info("No workflows fetched from HubSpot.")
        return

    # Step 2: Process workflows concurrently using ThreadPoolExecutor
    matched_count = 0
    enabled_count = 0
    summarized_count = 0

    logger.info("\nProcessing workflows concurrently...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all workflows to the executor
        future_to_workflow = {
            executor.submit(
                process_workflow,
                workflow,
                HUBSPOT_ACCESS_TOKEN,
                PROPERTIES_TO_SEARCH,
                MATCHED_OUTPUT_DIRECTORY,
                ENABLED_OUTPUT_DIRECTORY,
                SUMMARIES_OUTPUT_DIRECTORY,
                property_details,  # Pass property detail
                properties_str     # Pass properties_str
            ): workflow for workflow in all_workflows
        }

        # As each future completes, update the counts
        for future in as_completed(future_to_workflow):
            try:
                result = future.result()
                if result.get('matched'):
                    matched_count += 1
                if result.get('enabled'):
                    enabled_count += 1
                if result.get('summarized'):
                    summarized_count += 1
            except Exception as exc:
                workflow = future_to_workflow[future]
                workflow_id = workflow.get('id', 'unknown_id')
                logger.error(f"Workflow ID {workflow_id} generated an exception: {exc}")

    logger.info(f"\nProcess completed.")
    logger.info(f"Total matched workflows (containing specified properties): {matched_count}")
    logger.info(f"Total enabled workflows (enabled: true): {enabled_count}")
    logger.info(f"Total summaries generated: {summarized_count}")

    # Step 3: Generate system documentation
    logger.info("\nGenerating system documentation based on summaries and property details...")
    output_file_doc = "hubspot_system_documentation.txt"
    generate_system_documentation(property_details, SUMMARIES_OUTPUT_DIRECTORY, output_file_doc, properties_str)

    # Step 4: Combine Summaries with Custom Prompt
    logger.info("\nCombining all summaries into a single file with a custom prompt...")
    combined_output_file = "combined_output_with_prompt.txt"  # Specify your desired output file name and path
    combine_summaries_with_prompt(SUMMARIES_OUTPUT_DIRECTORY, combined_output_file, PROPERTIES_TO_SEARCH, property_details)
    logger.info(f"Combined summaries with prompt saved to {combined_output_file}")

# ----------------------- Entry Point -----------------------

if __name__ == "__main__":
    main()
