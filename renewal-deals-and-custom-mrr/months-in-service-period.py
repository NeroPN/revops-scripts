from datetime import datetime
from dateutil.relativedelta import relativedelta
import os
import json

# === Configuration Section ===
CONFIG = {
    # Toggle between testing and production modes
    'testing': False,  # Set to True to enable testing mode, False for production

    # Define input field names
    'input_fields': {
        'invoicing_period_start': 'deal_invoicing_period_start_date',
        'invoicing_period_end': 'deal_invoicing_period_end_date',
        'projected_invoicing_period_end': 'projected_deal_invoicing_period_end_date',
    },

    # Define test input fields with Unix timestamps for testing
    'test_input_fields': {
        'deal_invoicing_period_start_date': 1644883200000,          # Example: 2022-02-15 (Unix timestamp in ms)
        'deal_invoicing_period_end_date': 1672531199000,            # Example: 2023-01-31 (Unix timestamp in ms)
        'projected_deal_invoicing_period_end_date': 1704067199000, # Example: 2024-01-31 (Unix timestamp in ms)
    },

    # Define output field names
    'output_fields': {
        'invoicing_period_months': 'invoicing_period_months',
        'invoicing_period_length': 'invoicing_period_length',
        'is_projected': 'is_projected',
    },

    # Default values
    'defaults': {
        'is_projected': 'NO',
    }
}
# ==============================

def main(event):
    # === Extract Input Fields ===
    if CONFIG['testing']:
        input_fields = CONFIG['test_input_fields']
    else:
        input_fields = event.get('inputFields', {})
    # ==============================

    # Helper function to safely extract fields with default values
    def get_field(field_name, default_value=None):
        return input_fields.get(field_name, default_value)

    # Extract invoicing period start and end dates using configured field names
    invoicing_start_timestamp = get_field(CONFIG['input_fields']['invoicing_period_start'])
    invoicing_end_timestamp = get_field(CONFIG['input_fields']['invoicing_period_end'])
    projected_invoicing_end_timestamp = get_field(CONFIG['input_fields']['projected_invoicing_period_end'])

    # Debugging prints (optional, ensure your environment captures these)
    print(f'invoicing_start_timestamp: {invoicing_start_timestamp}')
    print(f'invoicing_end_timestamp: {invoicing_end_timestamp}')
    print(f'projected_invoicing_end_timestamp: {projected_invoicing_end_timestamp}')

    # Function to convert Unix timestamp in milliseconds to datetime object
    def convert_unix_to_datetime(timestamp):
        try:
            return datetime.utcfromtimestamp(int(timestamp) / 1000)
        except (ValueError, TypeError):
            return None

    # Convert timestamps to datetime objects
    invoicing_start_date = convert_unix_to_datetime(invoicing_start_timestamp)
    
    # Determine which end date to use
    if invoicing_end_timestamp:
        invoicing_end_date = convert_unix_to_datetime(invoicing_end_timestamp)
        is_projected = CONFIG['defaults']['is_projected']
    elif projected_invoicing_end_timestamp:
        invoicing_end_date = convert_unix_to_datetime(projected_invoicing_end_timestamp)
        is_projected = 'YES'
    else:
        invoicing_end_date = None
        is_projected = CONFIG['defaults']['is_projected']  # Default to 'NO' if no end date is provided

    # Debugging prints
    print(f'invoicing_start_date: {invoicing_start_date}')
    print(f'invoicing_end_date: {invoicing_end_date}')
    print(f'is_projected: {is_projected}')

    # Validate dates
    if not invoicing_start_date:
        error_message = 'Invalid invoicing period start date input.'
        return {'error': error_message}
    if not invoicing_end_date:
        error_message = 'Invalid invoicing period end date input.'
        return {'error': error_message}
    if invoicing_start_date > invoicing_end_date:
        error_message = 'Invoicing period start date is after the end date.'
        return {'error': error_message}

    # Function to generate list of "yyyy-mm" strings from start to end date, inclusive
    def generate_month_list(start_date, end_date):
        months = []
        current_date = datetime(start_date.year, start_date.month, 1)
        end_date = datetime(end_date.year, end_date.month, 1)
        while current_date <= end_date:
            months.append(current_date.strftime('%Y-%m'))
            current_date += relativedelta(months=1)
        return months

    # Generate the list of months
    invoicing_months_list = generate_month_list(invoicing_start_date, invoicing_end_date)

    # Convert the list to a semicolon-separated string
    invoicing_period_months = ';'.join(invoicing_months_list)

    # Calculate the length of the array
    invoicing_period_length = len(invoicing_months_list)

    # Debugging prints
    print(f'invoicing_period_months: {invoicing_period_months}')
    print(f'invoicing_period_length: {invoicing_period_length}')

    # === Prepare Output Fields ===
    output_fields = {
        CONFIG['output_fields']['invoicing_period_months']: invoicing_period_months,
        CONFIG['output_fields']['invoicing_period_length']: invoicing_period_length,  # Length of the array
        CONFIG['output_fields']['is_projected']: is_projected  # Binary flag indicating projection
    }
    # =============================

    return {
        'outputFields': output_fields
    }

# === Example Usage ===
if __name__ == "__main__":
    # Example event with Unix timestamps in milliseconds
    example_event = {
        'inputFields': {
            CONFIG['input_fields']['invoicing_period_start']: 1644883200000,          # 2022-02-15
            # CONFIG['input_fields']['invoicing_period_end']: 1672531199000,        # Uncomment to test with actual end date
            CONFIG['input_fields']['projected_invoicing_period_end']: 1704067199000, # 2024-01-31
        }
    }

    result = main(example_event)
    print(json.dumps(result, indent=2))
