import boto3
import json
import time
from botocore.exceptions import ClientError
import botocore.parsers

# Initialize the Amazon Connect client
client = boto3.client('connect', region_name='us-east-1')

# Instance ID and Evaluation Form ID
instance_id = "c3175ce9-154c-46cb-a559-94cdbbb3583a"
evaluation_form_id = "919c2fcc-ff87-43a8-af22-769d19d72268"

# Retry configuration
max_retries = 10
base_delay = 2
skip_first_contact_count = 200
skip_contacts = True

# Add this new class before get_evaluations()
class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if hasattr(obj, 'isoformat'):
            return obj.isoformat()
        return super().default(obj)

def get_evaluations():
    results = []
    
    # Read contact IDs from the contacts.json file
    try:
        with open('contacts.json', 'r') as file:
            contacts = json.load(file)
    except FileNotFoundError:
        print("contacts.json not found. Please run search_contacts.py first.")
        return
    
    total_contacts = len(contacts)
    print(f"Processing evaluations for {total_contacts} contacts...")

    for index, contact in enumerate(contacts, 1):
        # Skip first N contacts if skip_contacts is True
        if skip_contacts and index <= skip_first_contact_count:
            print(f"Skipping contact {index}/{total_contacts}")
            continue
            
        contact_id = contact['ContactId']
        print(f"Processing contact {index}/{total_contacts}: {contact_id}")

        # Add retry logic for list_contact_evaluations
        for attempt in range(max_retries):
            try:
                evaluations = client.list_contact_evaluations(
                    InstanceId=instance_id,
                    ContactId=contact_id
                )
                break
            except ClientError as e:
                if e.response['Error']['Code'] == 'ThrottlingException':
                    if attempt == max_retries - 1:
                        raise
                    delay = base_delay * (2 ** attempt)
                    print(f"Rate limited. Retrying in {delay} seconds... (Attempt {attempt + 1}/{max_retries})")
                    time.sleep(delay)
                else:
                    print(f"Error processing contact {contact_id}: {str(e)}")
                    break

        # Filter evaluations by Evaluation Form ID
        for evaluation in evaluations.get('EvaluationSummaryList', []):
            if evaluation['EvaluationFormId'] == evaluation_form_id:
                print(f"Processing evaluation {evaluation['EvaluationId']} for contact {contact_id}")
                # Add retry logic for get_contact_evaluation
                for attempt in range(max_retries):
                    try:
                        evaluation_details = client.describe_contact_evaluation(
                            InstanceId=instance_id,
                            EvaluationId=evaluation['EvaluationId']
                        )
                        results.append({
                            'ContactId': contact_id,
                            'EvaluationDetails': evaluation_details['Evaluation']
                        })
                        break
                    except ClientError as e:
                        if e.response['Error']['Code'] == 'ThrottlingException':
                            if attempt == max_retries - 1:
                                raise
                            delay = base_delay * (2 ** attempt)
                            print(f"Rate limited. Retrying in {delay} seconds... (Attempt {attempt + 1}/{max_retries})")
                            time.sleep(delay)
                        else:
                            print(f"Error getting evaluation details for {evaluation['EvaluationId']}: {str(e)}")
                            break
                    except botocore.parsers.ResponseParserError as e:
                        print(f"Failed to parse evaluation data for {evaluation['EvaluationId']}: {str(e)}")
                        break

    return results

if __name__ == "__main__":
    # Get evaluations
    evaluation_results = get_evaluations()

    # Save results to a JSON file
    output_file = 'evaluations.json'
    with open(output_file, 'w') as file:
        json.dump(evaluation_results, file, indent=4, cls=DateTimeEncoder)

    print(f"\nEvaluation results saved to {output_file}")
    print(f"Total evaluations processed: {len(evaluation_results)}")