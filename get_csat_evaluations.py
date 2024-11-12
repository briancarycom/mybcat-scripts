import boto3
import json

# Initialize the Amazon Connect client
client = boto3.client('connect', region_name='us-east-1')

# Instance ID and Evaluation Form ID
instance_id = "c3175ce9-154c-46cb-a559-94cdbbb3583a"
evaluation_form_id = "919c2fcc-ff87-43a8-af22-769d19d72268"

# Container for results
results = []

# get all contacts for the instance
contacts = client.list_contacts(InstanceId=instance_id)

for contact in contacts['ContactList']:
    # List all evaluations
    evaluations = client.list_contact_evaluations(InstanceId=instance_id, ContactId=contact['Id'])

    # Filter evaluations by Evaluation Form ID
    for evaluation in evaluations['EvaluationSummaryList']:
        if evaluation['EvaluationFormId'] == evaluation_form_id:
            # Get detailed information about each evaluation
            evaluation_details = client.describe_evaluation(
                InstanceId=instance_id,
                EvaluationId=evaluation['EvaluationId']
            )
            results.append(evaluation_details['Evaluation'])

    # Save results to a JSON file
    output_file = 'evaluations.json'
    with open(output_file, 'w') as file:
        json.dump(results, file, indent=4)

    print(f"Evaluation results saved to {output_file}")