import boto3
import json
import time
from botocore.exceptions import ClientError

# Initialize the boto3 client for Amazon Connect
connect_client = boto3.client('connect', region_name='us-east-1')

# Set the parameters
instance_id = "c3175ce9-154c-46cb-a559-94cdbbb3583a"
time_range = {
    "Type": "INITIATION_TIMESTAMP",
    "StartTime": "2024-11-12T18:00:00Z",
    "EndTime": "2024-11-12T23:59:59Z"
}

# Function to fetch and extract contact details
def get_contacts(instance_id, time_range):
    contacts = []
    next_token = None
    max_retries = 5
    base_delay = 1
    request_delay = 0.5  # Add default delay between requests
    page_count = 0
    total_contacts = 0

    print(f"Starting contact search from {time_range['StartTime']} to {time_range['EndTime']}")

    # Get initial response to check total count
    params = {
        'InstanceId': instance_id,
        'TimeRange': {
            'Type': 'INITIATION_TIMESTAMP',
            'StartTime': time_range['StartTime'],
            'EndTime': time_range['EndTime']
        },
        'MaxResults': 100
    }

    # Add retry logic for initial call
    for attempt in range(max_retries):
        try:
            initial_response = connect_client.search_contacts(**params)
            break
        except ClientError as e:
            if e.response['Error']['Code'] == 'ThrottlingException':
                if attempt == max_retries - 1:
                    raise
                delay = base_delay * (2 ** attempt)
                print(f"Rate limited. Retrying in {delay} seconds... (Attempt {attempt + 1}/{max_retries})")
                time.sleep(delay)
            else:
                raise

    total_records = initial_response.get('TotalCount', 0)
    print(f"\nTotal records to retrieve: {total_records}")

    # Process first page
    page_count += 1
    print(f"\nFetching page {page_count}...")

    while True:
        # Add delay between requests
        time.sleep(request_delay)
        page_count += 1
        print(f"\nFetching page {page_count}...")

        # Prepare API call parameters - Updated to match AWS Connect API requirements
        params = {
            'InstanceId': instance_id,
            'TimeRange': {
                'Type': 'INITIATION_TIMESTAMP',
                'StartTime': time_range['StartTime'],
                'EndTime': time_range['EndTime']
            },
            'MaxResults': 100
        }
        if next_token:
            params['NextToken'] = next_token

        # Add retry logic
        for attempt in range(max_retries):
            try:
                response = connect_client.search_contacts(**params)
                break  # Success, exit retry loop
            except ClientError as e:
                if e.response['Error']['Code'] == 'ThrottlingException':  # Check for rate limiting
                    if attempt == max_retries - 1:  # Last attempt
                        raise  # Re-raise the exception if all retries failed
                    delay = base_delay * (2 ** attempt)  # Exponential backoff
                    print(f"Rate limited. Retrying in {delay} seconds... (Attempt {attempt + 1}/{max_retries})")
                    time.sleep(delay)
                else:
                    raise  # Re-raise if it's not a throttling error

        # Process contacts and update counters
        page_contacts = response.get('Contacts', [])
        total_contacts += len(page_contacts)
        total_records = response.get('TotalCount', 0)
        print(f"Retrieved {len(page_contacts)} contacts on this page. Total contacts so far: {total_contacts}")
        print(f"Total records in search results: {total_records}")

        # Extract the required fields
        for contact in page_contacts:
            # Get evaluation details if they exist
            evaluations = contact.get('EvaluationData', {}).get('Evaluations', [])
            if evaluations:
                print(f"\nFound {len(evaluations)} evaluation(s) for contact {contact.get('Id')}:")
                for eval in evaluations:
                    print(f"  - Evaluation ID: {eval.get('EvaluationId')}")
                    print(f"  - Form Name: {eval.get('EvaluationFormName')}")
                    print(f"  - Status: {eval.get('Status')}")
            
            contact_info = {
                'ContactId': contact.get('Id'),
                'AgentId': contact.get('AgentInfo', {}).get('Id'),
                'InitiationTimestamp': contact.get('InitiationTimestamp').isoformat() if contact.get('InitiationTimestamp') else None,
                'DisconnectTimestamp': contact.get('DisconnectTimestamp').isoformat() if contact.get('DisconnectTimestamp') else None,
                'Evaluations': [{
                    'EvaluationId': eval.get('EvaluationId'),
                    'FormName': eval.get('EvaluationFormName'),
                    'Status': eval.get('Status')
                } for eval in evaluations]
            }
            contacts.append(contact_info)

        # Check if there's more data to retrieve
        next_token = response.get('NextToken')
        if not next_token:
            print(f"\nPagination complete. Retrieved {total_contacts} contacts across {page_count} pages.")
            break
    
    return contacts

# Fetch contacts
contact_data = get_contacts(instance_id, time_range)

# Output the result
output_file = "contacts.json"
with open(output_file, "w") as f:
    json.dump(contact_data, f, indent=4)

print(f"Extracted contact data has been saved to {output_file}")