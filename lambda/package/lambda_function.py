import boto3
import json
import time
from botocore.exceptions import ClientError
import botocore.parsers
from datetime import datetime, timedelta

def get_previous_hour_range():
    """Get the start and end time for the previous completed hour"""
    current = datetime.utcnow()
    end_time = current.replace(minute=0, second=0, microsecond=0)
    start_time = end_time - timedelta(hours=1)
    return start_time, end_time

def lambda_handler(event, context):
    # Initialize the clients
    client = boto3.client('connect')
    s3_client = boto3.client('s3')
    
    # Configuration
    instance_id = "c3175ce9-154c-46cb-a559-94cdbbb3583a"
    evaluation_form_id = "919c2fcc-ff87-43a8-af22-769d19d72268"
    bucket_name = "custom-reports"
    
    # Get time range for previous hour
    start_time, end_time = get_previous_hour_range()
    
    # Create S3 path using the end_time (which represents the hour we're processing)
    s3_path = end_time.strftime('%Y/%m/%d/%H')
    s3_key = f"{s3_path}/evaluations.json"
    
    time_range = {
        "Type": "INITIATION_TIMESTAMP",
        "StartTime": start_time.isoformat() + "Z",
        "EndTime": end_time.isoformat() + "Z"
    }
    
    # Retry configuration
    max_retries = 15
    base_delay = 3
    request_delay = 1
    
    def retry_with_backoff(func, *args, **kwargs):
        """Generic retry function with exponential backoff"""
        for attempt in range(max_retries):
            try:
                return func(*args, **kwargs)
            except ClientError as e:
                if e.response['Error']['Code'] in ['ThrottlingException', 'TooManyRequestsException']:
                    if attempt == max_retries - 1:
                        raise
                    delay = base_delay * (2 ** attempt)
                    print(f"Rate limited. Retrying in {delay} seconds... (Attempt {attempt + 1}/{max_retries})")
                    time.sleep(delay)
                else:
                    raise
        return None

    # Add queue cache to avoid repeated API calls for the same queue
    queue_cache = {}
    
    def get_queue_name(queue_id):
        """Get queue name from queue ID using cache"""
        if not queue_id:
            return None
            
        if queue_id in queue_cache:
            return queue_cache[queue_id]
            
        try:
            response = retry_with_backoff(
                client.describe_queue,
                InstanceId=instance_id,
                QueueId=queue_id
            )
            queue_name = response.get('Queue', {}).get('Name')
            queue_cache[queue_id] = queue_name
            return queue_name
        except Exception as e:
            print(f"Error getting queue name for {queue_id}: {str(e)}")
            return None

    # Add agent cache similar to queue cache
    agent_cache = {}
    
    def get_agent_name(agent_id):
        """Get agent name from agent ID using cache"""
        if not agent_id:
            return None
            
        if agent_id in agent_cache:
            return agent_cache[agent_id]
            
        try:
            response = retry_with_backoff(
                client.describe_user,
                InstanceId=instance_id,
                UserId=agent_id
            )
            agent_name = response.get('User', {}).get('Username')
            agent_cache[agent_id] = agent_name
            return agent_name
        except Exception as e:
            print(f"Error getting agent name for {agent_id}: {str(e)}")
            return None

    def search_contacts():
        """Search for contacts within the specified time range"""
        contacts = []
        next_token = None
        page_count = 0
        total_contacts = 0

        print(f"Starting contact search from {time_range['StartTime']} to {time_range['EndTime']}")

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
                queue_id = contact.get('QueueInfo', {}).get('Id')
                agent_id = contact.get('AgentInfo', {}).get('Id')
                
                # Calculate call duration
                initiation_time = contact.get('InitiationTimestamp')
                disconnect_time = contact.get('DisconnectTimestamp')
                duration_seconds = None
                if initiation_time and disconnect_time:
                    duration = disconnect_time - initiation_time
                    duration_seconds = int(duration.total_seconds())
                
                contacts.append({
                    'ContactId': contact.get('Id'),
                    'AgentId': agent_id,
                    'AgentName': get_agent_name(agent_id),
                    'QueueId': queue_id,
                    'QueueName': get_queue_name(queue_id),
                    'InitiationTimestamp': initiation_time,
                    'DisconnectTimestamp': disconnect_time,
                    'DurationSeconds': duration_seconds
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

        for index, contact in enumerate(contacts, 1):
            contact_id = contact['ContactId']
            print(f"Processing contact {index}/{total_contacts}: {contact_id}")

            try:
                time.sleep(request_delay)  # Add delay between requests
                evaluations = retry_with_backoff(
                    client.list_contact_evaluations,
                    InstanceId=instance_id,
                    ContactId=contact_id
                )
                
                if not evaluations:
                    continue

                for evaluation in evaluations.get('EvaluationSummaryList', []):
                    if evaluation['EvaluationFormId'] == evaluation_form_id:
                        try:
                            evaluation_details = retry_with_backoff(
                                client.describe_contact_evaluation,
                                InstanceId=instance_id,
                                EvaluationId=evaluation['EvaluationId']
                            )
                            
                            if evaluation_details:
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

    try:
        # Search for contacts
        print("Step 1: Searching for contacts...")
        contacts = search_contacts()
        
        # Get evaluations for contacts
        print("\nStep 2: Getting evaluations...")
        evaluation_results = get_evaluations(contacts)

        # Prepare the results
        results = {
            'message': 'Process completed successfully',
            'timeRange': time_range,
            'totalContacts': len(contacts),
            'totalEvaluations': len(evaluation_results),
            'results': evaluation_results
        }

        # Upload to S3
        print(f"\nStep 3: Uploading results to S3 bucket: {bucket_name}/{s3_key}")
        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=json.dumps(results, default=str),
            ContentType='application/json'
        )

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Process completed successfully',
                'timeRange': time_range,
                'totalContacts': len(contacts),
                'totalEvaluations': len(evaluation_results),
                's3Location': f"s3://{bucket_name}/{s3_key}"
            }, default=str)
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            })
        } 