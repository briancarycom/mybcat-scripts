import boto3
import json
import time
from botocore.exceptions import ClientError
import botocore.parsers
from datetime import datetime, timedelta
import pytz
from functools import lru_cache

def get_previous_hour_range():
    """Get the start and end time for the previous completed hour"""
    current = datetime.utcnow()
    print(f"Current UTC time: {current}")
    
    end_time = current.replace(minute=0, second=0, microsecond=0)
    print(f"End time (current hour start): {end_time}")
    
    start_time = end_time - timedelta(hours=1)
    print(f"Start time (previous hour): {start_time}")
    
    return start_time, end_time

def lambda_handler(event, context):
    # Initialize the clients
    client = boto3.client('connect')
    s3_client = boto3.client('s3')
    
    # Check if we should skip S3 upload (for testing)
    skip_s3 = event.get('queryStringParameters', {}).get('skipS3', 'false').lower() == 'true'
    
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

    @lru_cache(maxsize=100)
    def get_hours_of_operation(queue_id):
        """Get and cache hours of operation for a queue"""
        if not queue_id:
            return None
        
        try:
            # Added retry logic to both API calls
            queue = retry_with_backoff(
                client.describe_queue,
                InstanceId=instance_id,
                QueueId=queue_id
            )
            hours_of_operation_id = queue['Queue']['HoursOfOperationId']
            
            hours_of_operation = retry_with_backoff(
                client.describe_hours_of_operation,
                InstanceId=instance_id,
                HoursOfOperationId=hours_of_operation_id
            )
            return hours_of_operation['HoursOfOperation']
        except Exception as e:
            print(f"Error getting hours of operation for queue {queue_id}: {str(e)}")
            return None

    def was_call_during_open_hours(queue_id, initiation_timestamp, initiation_method, contact_details=None, contact_count=None):
        """Check if the call was made during operating hours"""
        queue_name = get_queue_name(queue_id)
        count_info = f"(Call {contact_count['current']}/{contact_count['total']})" if contact_count else ""
        print(f"\nChecking hours for queue {queue_id} ({queue_name}) at {initiation_timestamp} {count_info}")
        
        # Handle abandoned calls (no queue or agent)
        if not queue_id:
            print("No queue ID found - likely an abandoned call")
            # if contact_details:
            #     print("Contact details:", json.dumps(contact_details, indent=2, default=str))
            return {
                'during_hours': None,  # Using None to indicate abandoned call
                'timezone': 'UTC',
                'local_time': initiation_timestamp.isoformat(),
                'status': 'ABANDONED'
            }
        
        # Check if this is an external outbound call
        if initiation_method == 'EXTERNAL_OUTBOUND':
            print("External outbound call - considering as during operating hours")
            return {
                'during_hours': True,
                'timezone': 'UTC',  # Default to UTC for outbound calls
                'local_time': initiation_timestamp.isoformat()
            }
        
        if not queue_id:
            # Add more debug information about the contact
            contact_info = {
                'timestamp': str(initiation_timestamp),
                'initiation_method': initiation_method,
                'channel': contact.get('Channel'),
                'queue_info': contact.get('QueueInfo'),
                'initial_contact_id': contact.get('InitialContactId'),
                'previous_contact_id': contact.get('PreviousContactId')
            }
            print(f"Debug info for contact with missing queue_id:")
            print(json.dumps(contact_info, indent=2, default=str))
            raise ValueError("Queue ID is None - this might be a transfer or multi-queue contact")
        
        hours_of_operation = get_hours_of_operation(queue_id)
        if not hours_of_operation:
            raise ValueError(f"Could not retrieve hours of operation for queue {queue_id}")
        
        if not initiation_timestamp:
            raise ValueError("Missing timestamp")
        
        # Get timezone from hours of operation
        timezone_str = hours_of_operation.get('TimeZone')
        if not timezone_str:
            raise ValueError(f"No timezone specified for queue {queue_id}")
        
        # List the hours of operation config
        print(f"Hours of operation config for queue {queue_id} ({queue_name}):")
        print(f"Queue timezone: {timezone_str}")
        # print(f"Full hours config: {json.dumps(hours_of_operation['Config'], indent=2)}")
        
        try:
            # Convert UTC timestamp to queue's timezone
            timezone = pytz.timezone(timezone_str)
            # Make sure initiation_timestamp is timezone-aware before converting
            if initiation_timestamp.tzinfo is None:
                initiation_timestamp = pytz.utc.localize(initiation_timestamp)
            call_time = initiation_timestamp.astimezone(timezone)
            print(f"Call time in queue timezone: {call_time}")
            
            day_name = call_time.strftime('%A').upper()
            print(f"Day of week: {day_name}")
            
            for entry in hours_of_operation['Config']:
                if entry['Day'] == day_name:
                    # Check if this is a 24/7 queue (start and end both at 00:00)
                    is_24_7 = (entry['StartTime']['Hours'] == 0 and 
                              entry['StartTime']['Minutes'] == 0 and
                              entry['EndTime']['Hours'] == 0 and 
                              entry['EndTime']['Minutes'] == 0)
                    
                    if is_24_7:
                        print("24/7 queue detected (00:00-00:00) - considering as during operating hours")
                        return {
                            'during_hours': True,
                            'timezone': timezone_str,
                            'local_time': call_time.isoformat()
                        }
                    
                    # Create time objects using the call's date and timezone
                    start_time = timezone.localize(
                        call_time.replace(
                            hour=int(entry['StartTime']['Hours']),
                            minute=int(entry['StartTime']['Minutes']),
                            second=0,
                            microsecond=0
                        ).replace(tzinfo=None)
                    )
                    
                    end_time = timezone.localize(
                        call_time.replace(
                            hour=int(entry['EndTime']['Hours']),
                            minute=int(entry['EndTime']['Minutes']),
                            second=0,
                            microsecond=0
                        ).replace(tzinfo=None)
                    )
                    
                    print(f"Comparing times (all in {timezone_str}):")
                    print(f"Start: {start_time}")
                    print(f"Call:  {call_time}")
                    print(f"End:   {end_time}")
                    
                    if start_time <= call_time <= end_time:
                        print("Call was during operating hours")
                        return {
                            'during_hours': True,
                            'timezone': timezone_str,
                            'local_time': call_time.isoformat()
                        }
            
            print("Call was outside operating hours")
            return {
                'during_hours': False,
                'timezone': timezone_str,
                'local_time': call_time.isoformat()
            }
            
        except pytz.exceptions.PytzError as e:
            raise ValueError(f"Invalid timezone {timezone_str} for queue {queue_id}: {str(e)}")

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

            for index, contact in enumerate(page_contacts, 1):
                print(f'Processing contact: {contact.get("Id")}')

                # Get contact attributes
                try:
                    attributes_response = retry_with_backoff(
                        client.get_contact_attributes,
                        InstanceId=instance_id,
                        InitialContactId=contact.get('InitialContactId', contact.get('Id'))
                    )
                    contact_attributes = attributes_response.get('Attributes', {})
                    print(f"Contact attributes: {contact_attributes}")
                except Exception as e:
                    print(f"Error getting attributes for contact {contact.get('Id')}: {str(e)}")
                    contact_attributes = {}

                # Add the rest of your contact processing logic here
                queue_id = contact.get('QueueInfo', {}).get('Id')
                agent_id = contact.get('AgentInfo', {}).get('Id')
                initiation_timestamp = contact.get('InitiationTimestamp')
                
                # Calculate call duration
                disconnect_time = contact.get('DisconnectTimestamp')
                duration_seconds = None
                if initiation_timestamp and disconnect_time:
                    duration = disconnect_time - initiation_timestamp
                    duration_seconds = int(duration.total_seconds())
                
                # Check if call was during operating hours
                during_hours_result = was_call_during_open_hours(
                    queue_id, 
                    initiation_timestamp,
                    contact.get('InitiationMethod'),
                    contact,  # Pass the full contact details for better debugging
                    {'current': total_contacts - len(page_contacts) + index, 'total': total_contacts}
                )

                # Determine call status
                call_status = 'ABANDONED'
                if queue_id and agent_id:
                    call_status = 'HANDLED'
                elif queue_id and not agent_id:
                    call_status = 'QUEUE_ABANDONED'
                
                # Add attributes to the contact info
                contact_info = {
                    'ContactId': contact.get('Id'),
                    'AgentId': agent_id,
                    'AgentName': get_agent_name(agent_id),
                    'QueueId': queue_id,
                    'QueueName': get_queue_name(queue_id),
                    'InitiationTimestamp': initiation_timestamp,
                    'DisconnectTimestamp': disconnect_time,
                    'DurationSeconds': duration_seconds,
                    'DuringOperatingHours': during_hours_result.get('during_hours'),
                    'QueueTimezone': during_hours_result.get('timezone'),
                    'LocalCallTime': during_hours_result.get('local_time'),
                    'InitiationMethod': contact.get('InitiationMethod'),
                    'CallStatus': call_status,
                    'CallerSurveyResponse1': int(contact_attributes.get('survey_result_1', 0)) if contact_attributes.get('survey_result_1') else None,
                }
                contacts.append(contact_info)

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
        # print(f"Contacts: {contacts}")

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

        # Upload to S3 if not skipped
        if not skip_s3:
            print(f"\nStep 3: Uploading results to S3 bucket: {bucket_name}/{s3_key}")
            s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=json.dumps(results, default=str),
                ContentType='application/json'
            )
            s3_location = f"s3://{bucket_name}/{s3_key}"
        else:
            print("\nStep 3: Skipping S3 upload (skipS3=true)")
            print("\nResults:")
            print(json.dumps(results, default=str, indent=2, ensure_ascii=False))
            s3_location = None

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Process completed successfully',
                'timeRange': time_range,
                'totalContacts': len(contacts),
                'totalEvaluations': len(evaluation_results),
                's3Location': s3_location,
                'results': results if skip_s3 else None  # Include full results in response if S3 was skipped
            }, default=str)
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            })
        } 