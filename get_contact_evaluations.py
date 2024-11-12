import boto3
import json
import time
from botocore.exceptions import ClientError
import botocore.parsers
from datetime import datetime

# Initialize the Amazon Connect client
client = boto3.client('connect', region_name='us-east-1')

# Configuration
instance_id = "c3175ce9-154c-46cb-a559-94cdbbb3583a"
evaluation_form_id = "919c2fcc-ff87-43a8-af22-769d19d72268"
time_range = {
    "Type": "INITIATION_TIMESTAMP",
    "StartTime": "2024-11-12T20:00:00Z",
    "EndTime": "2024-11-12T23:59:59Z"
}

# Retry configuration
max_retries = 15
base_delay = 3
request_delay = 1

class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if hasattr(obj, 'isoformat'):
            return obj.isoformat()
        return super().default(obj)

def retry_with_backoff(func, *args, **kwargs):
    """Generic retry function with exponential backoff"""
    for attempt in range(max_retries):
        try:
            return func(*args, **kwargs)
        except ClientError as e:
            if e.response['Error']['Code'] == 'ThrottlingException':
                if attempt == max_retries - 1:
                    raise
                delay = base_delay * (2 ** attempt)
                print(f"Rate limited. Retrying in {delay} seconds... (Attempt {attempt + 1}/{max_retries})")
                time.sleep(delay)
            else:
                raise
    return None

def search_contacts():
    """Search for contacts within the specified time range"""
    contacts = []
    next_token = None
    page_count = 0
    total_contacts = 0

    print(f"Starting contact search from {time_range['StartTime']} to {time_range['EndTime']}")

    # Get initial count of contacts
    params = {
        'InstanceId': instance_id,
        'TimeRange': time_range,
        'MaxResults': 1  # Minimum value to just get the count
    }
    initial_response = retry_with_backoff(client.search_contacts, **params)
    if initial_response:
        print(f"Total contacts to process: {initial_response['TotalCount']}")

    while True:
        time.sleep(request_delay)
        page_count += 1
        print(f"\nFetching page {page_count}...")

        params = {
            'InstanceId': instance_id,
            'TimeRange': time_range,
            'MaxResults': 100
        }
        if next_token:
            params['NextToken'] = next_token

        response = retry_with_backoff(client.search_contacts, **params)
        if not response:
            break

        page_contacts = response.get('Contacts', [])
        total_contacts += len(page_contacts)
        print(f"Retrieved {len(page_contacts)} contacts. Total: {total_contacts}")

        for contact in page_contacts:
            contacts.append({
                'ContactId': contact.get('Id'),
                'AgentId': contact.get('AgentInfo', {}).get('Id'),
                'InitiationTimestamp': contact.get('InitiationTimestamp'),
                'DisconnectTimestamp': contact.get('DisconnectTimestamp')
            })

        next_token = response.get('NextToken')
        if not next_token:
            print(f"\nContact search complete. Retrieved {total_contacts} contacts.")
            break

    return contacts

def get_evaluations(contacts):
    """Get evaluation details for each contact"""
    results = []
    total_contacts = len(contacts)

    print(f"\nProcessing evaluations for {total_contacts} contacts...")

    for index, contact in enumerate(contacts, 1):
        contact_id = contact['ContactId']
        print(f"Processing contact {index}/{total_contacts}: {contact_id}")

        try:
            evaluations = retry_with_backoff(
                client.list_contact_evaluations,
                InstanceId=instance_id,
                ContactId=contact_id
            )
            
            if not evaluations:
                continue

            for evaluation in evaluations.get('EvaluationSummaryList', []):
                if evaluation['EvaluationFormId'] == evaluation_form_id:
                    print(f"Processing evaluation {evaluation['EvaluationId']}")
                    
                    try:
                        evaluation_details = retry_with_backoff(
                            client.describe_contact_evaluation,
                            InstanceId=instance_id,
                            EvaluationId=evaluation['EvaluationId']
                        )
                        
                        if evaluation_details:
                            # Extract score safely with a default value of None
                            score = evaluation_details.get('Evaluation', {}).get('Metadata', {}).get('Score', {}).get('Percentage')
                            
                            results.append({
                                'ContactId': contact_id,
                                'ContactDetails': contact,
                                'EvaluationDetails': {
                                    'EvaluationId': evaluation_details['Evaluation']['EvaluationId'],
                                    'Score': score
                                }
                            })
                    except botocore.parsers.ResponseParserError as e:
                        print(f"Error parsing evaluation {evaluation['EvaluationId']}: {str(e)}")
                        # Optionally store the failed evaluation ID for later investigation
                        results.append({
                            'ContactId': contact_id,
                            'ContactDetails': contact,
                            'EvaluationError': {
                                'EvaluationId': evaluation['EvaluationId'],
                                'Error': str(e)
                            }
                        })
        except Exception as e:
            print(f"Error processing contact {contact_id}: {str(e)}")
            continue

    return results

def main():
    # Search for contacts
    print("Step 1: Searching for contacts...")
    contacts = search_contacts()
    
    # Save contacts to file (optional)
    with open('contacts.json', 'w') as file:
        json.dump(contacts, file, indent=4, cls=DateTimeEncoder)
    
    # Get evaluations for contacts
    print("\nStep 2: Getting evaluations...")
    evaluation_results = get_evaluations(contacts)

    # Save final results
    output_file = 'evaluations.json'
    with open(output_file, 'w') as file:
        json.dump(evaluation_results, file, indent=4, cls=DateTimeEncoder)

    print(f"\nProcess complete!")
    print(f"Total contacts processed: {len(contacts)}")
    print(f"Total evaluations found: {len(evaluation_results)}")
    print(f"Results saved to {output_file}")

if __name__ == "__main__":
    main() 