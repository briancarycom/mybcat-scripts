import boto3
import json
import datetime

# Initialize the Amazon Connect client
client = boto3.client('connect', region_name='us-east-1')

# Instance ID and Evaluation Form ID
instance_id = "c3175ce9-154c-46cb-a559-94cdbbb3583a"
evaluation_form_id = "919c2fcc-ff87-43a8-af22-769d19d72268"

# Container for results
results = []

# Step 1: Get evaluation form structure
try:
    evaluation_form = client.describe_evaluation_form(
        InstanceId=instance_id,
        EvaluationFormId=evaluation_form_id
    )['EvaluationForm']
    
    # Step 2: Get all contact evaluations for a time period
    try:
        end_time = datetime.datetime.now(datetime.UTC)
        start_time = end_time - datetime.timedelta(days=1)
        
        # First, get all contacts
        contact_paginator = client.get_paginator('list_contacts')
        contact_pages = contact_paginator.paginate(
            InstanceId=instance_id,
            StartTime=start_time,
            EndTime=end_time
        )
        
        for contact_page in contact_pages:
            for contact in contact_page.get('ContactList', []):
                try:
                    # Get evaluations for each contact
                    evaluation_paginator = client.get_paginator('list_contact_evaluations')
                    evaluation_pages = evaluation_paginator.paginate(
                        InstanceId=instance_id,
                        ContactId=contact['Id']
                    )
                    
                    for evaluation_page in evaluation_pages:
                        for evaluation in evaluation_page.get('EvaluationSummaryList', []):
                            # Only process evaluations that match our form ID
                            if evaluation.get('EvaluationFormId') == evaluation_form_id:
                                try:
                                    evaluation_result = client.get_contact_evaluation(
                                        InstanceId=instance_id,
                                        EvaluationId=evaluation['EvaluationId']
                                    )
                                    
                                    result = {
                                        'evaluation_id': evaluation['EvaluationId'],
                                        'contact_id': evaluation['ContactId'],
                                        'score': evaluation_result['Evaluation'].get('Score'),
                                        'form_details': evaluation_form,
                                        'status': evaluation_result['Evaluation']['Status'],
                                        'created_time': evaluation_result['Evaluation']['CreatedTime'].isoformat()
                                    }
                                    
                                    results.append(result)
                                except client.exceptions.ClientError as e:
                                    print(f"Error processing evaluation {evaluation['EvaluationId']}: {e}")
                except client.exceptions.ClientError as e:
                    print(f"Error listing evaluations for contact {contact['Id']}: {e}")
    except client.exceptions.ClientError as e:
        print(f"Error listing evaluations: {e}")
except client.exceptions.ClientError as e:
    print(f"Error getting evaluation form: {e}")

# Save results to a JSON file
output_file = 'evaluation_results.json'
with open(output_file, 'w') as file:
    json.dump(results, file, indent=4)

print(f"Evaluation results saved to {output_file}")