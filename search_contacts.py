import boto3
import json

# Initialize the boto3 client for Amazon Connect
connect_client = boto3.client('connect', region_name='us-east-1')

# Set the parameters
instance_id = "c3175ce9-154c-46cb-a559-94cdbbb3583a"
time_range = {
    "Type": "INITIATION_TIMESTAMP",
    "StartTime": "2024-11-01T00:00:00Z",
    "EndTime": "2024-11-13T23:59:59Z"
}

# Function to fetch and extract contact details
def get_contacts(instance_id, time_range):
    contacts = []
    next_token = None

    while True:
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

        # Make the API call
        response = connect_client.search_contacts(**params)
        
        # Extract the required fields
        for contact in response.get('Contacts', []):
            contact_info = {
                'ContactId': contact.get('Id'),
                'AgentId': contact.get('AgentInfo', {}).get('Id'),
                'InitiationTimestamp': contact.get('InitiationTimestamp'),
                'DisconnectTimestamp': contact.get('DisconnectTimestamp')
            }
            contacts.append(contact_info)

        # Check if there's more data to retrieve
        next_token = response.get('NextToken')
        if not next_token:
            break
    
    return contacts

# Fetch contacts
contact_data = get_contacts(instance_id, time_range)

# Output the result
output_file = "contacts.json"
with open(output_file, "w") as f:
    json.dump(contact_data, f, indent=4)

print(f"Extracted contact data has been saved to {output_file}")