import boto3
import json
from datetime import datetime, timedelta
import requests
from google.cloud import bigquery
from google.oauth2 import service_account
import os
from dotenv import load_dotenv
import pytz
from functools import lru_cache

# Configuration
UPDATE_BIGQUERY = False  # Set to True when ready to update
DATES_TO_BACKFILL = ['2024-11-12']
API_ENDPOINT = "https://rmjs3kanak.execute-api.us-east-1.amazonaws.com/evaluations"
PROJECT_ID = "mybcat"
DATASET_ID = "amazon_connect_evaluations_test"
TABLE_ID = "user_evaluations"
instance_id = "c3175ce9-154c-46cb-a559-94cdbbb3583a"

@lru_cache(maxsize=100)
def get_hours_of_operation(connect_client, instance_id, queue_id):
    """Get and cache hours of operation for a queue"""
    if not queue_id:
        return None
    
    try:
        queue = connect_client.describe_queue(
            InstanceId=instance_id,
            QueueId=queue_id
        )
        hours_of_operation_id = queue['Queue']['HoursOfOperationId']
        
        hours_of_operation = connect_client.describe_hours_of_operation(
            InstanceId=instance_id,
            HoursOfOperationId=hours_of_operation_id
        )
        return hours_of_operation['HoursOfOperation']
    except Exception as e:
        print(f"Error getting hours of operation for queue {queue_id}: {str(e)}")
        return None

def was_call_during_open_hours(connect_client, instance_id, queue_id, initiation_timestamp, initiation_method):
    """Check if the call was made during operating hours"""
    # Handle abandoned calls (no queue)
    if not queue_id:
        return {
            'during_hours': None,  # Using None to indicate abandoned call
            'timezone': 'UTC',
            'local_time': initiation_timestamp.isoformat(),
            'status': 'ABANDONED'
        }
    
    # Check if this is an external outbound call
    if initiation_method == 'EXTERNAL_OUTBOUND':
        return {
            'during_hours': True,
            'timezone': 'UTC',  # Default to UTC for outbound calls
            'local_time': initiation_timestamp.isoformat()
        }
    
    hours_of_operation = get_hours_of_operation(connect_client, instance_id, queue_id)
    if not hours_of_operation:
        raise ValueError(f"Could not retrieve hours of operation for queue {queue_id}")

    # Get timezone from hours of operation
    timezone_str = hours_of_operation.get('TimeZone')
    if not timezone_str:
        raise ValueError(f"No timezone specified for queue {queue_id}")
    
    try:
        # Convert UTC timestamp to queue's timezone
        timezone = pytz.timezone(timezone_str)
        # Make sure initiation_timestamp is timezone-aware
        if isinstance(initiation_timestamp, str):
            initiation_timestamp = datetime.fromisoformat(initiation_timestamp.replace('Z', '+00:00'))
        if initiation_timestamp.tzinfo is None:
            initiation_timestamp = pytz.utc.localize(initiation_timestamp)
        call_time = initiation_timestamp.astimezone(timezone)
        
        day_name = call_time.strftime('%A').upper()
        
        for entry in hours_of_operation['Config']:
            if entry['Day'] == day_name:
                # Check if this is a 24/7 queue
                is_24_7 = (entry['StartTime']['Hours'] == 0 and 
                          entry['StartTime']['Minutes'] == 0 and
                          entry['EndTime']['Hours'] == 0 and 
                          entry['EndTime']['Minutes'] == 0)
                
                if is_24_7:
                    return {
                        'during_hours': True,
                        'timezone': timezone_str,
                        'local_time': call_time.isoformat()
                    }
                
                # Create time objects for comparison
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
                
                if start_time <= call_time <= end_time:
                    return {
                        'during_hours': True,
                        'timezone': timezone_str,
                        'local_time': call_time.isoformat()
                    }
        
        return {
            'during_hours': False,
            'timezone': timezone_str,
            'local_time': call_time.isoformat()
        }
        
    except pytz.exceptions.PytzError as e:
        raise ValueError(f"Invalid timezone {timezone_str} for queue {queue_id}: {str(e)}")

# Add queue cache to avoid repeated API calls for the same queue
queue_cache = {}

def get_queue_name(connect_client, queue_id):
    """Get queue name from queue ID using cache"""
    if not queue_id:
        return None
        
    if queue_id in queue_cache:
        return queue_cache[queue_id]
        
    try:
        response = connect_client.describe_queue(
            InstanceId=instance_id,
            QueueId=queue_id
        )
        print(f"Queue response: {response}")
        queue_name = response.get('Queue', {}).get('Name')
        queue_cache[queue_id] = queue_name
        return queue_name
    except Exception as e:
        print(f"Error getting queue name for {queue_id}: {str(e)}")
        return None

def lambda_handler(event, context):

    # Define target IDs before using them
    target_eval_id = "7d582bdd-0ddc-445e-8396-0b139af768ba"
    target_contact_id = "a24b289a-426b-4a9e-a449-548dd52d3900"

    connect_client = boto3.client('connect')

    try:
        # Initialize BigQuery client
        if os.getenv('AWS_LAMBDA_FUNCTION_NAME') is None:
            load_dotenv()
        
        credentials_dict = json.loads(os.environ['GOOGLE_APPLICATION_CREDENTIALS_JSON'])
        credentials = service_account.Credentials.from_service_account_info(credentials_dict)
        client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
        table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

        # Get existing records from BigQuery
        query = f"""
            SELECT 
                EvaluationId,
                AgentName,
                QueueTimezone,
                LocalCallTime,
                InitiationMethod,
                DuringOperatingHours,
                CallStatus,
                ContactId,
                QueueId,
                QueueName
            FROM `{table_id}`
            WHERE ContactId = '{target_contact_id}'
            ORDER BY LocalCallTime ASC
        """

        query_result = client.query(query).result()  # Execute query and get all results
        existing_records = {row['ContactId']: row for row in query_result}
        print(f"Found {len(existing_records)} existing records in BigQuery")
        print(f"First record: {existing_records[target_contact_id]}")

        if len(existing_records) == 0:
            raise Exception(f"No records found in BigQuery for ContactId {target_contact_id}")

        # Find the matching record
        target_record = existing_records.get(target_contact_id)
        if not target_record:
            raise Exception(f"Target contact ID {target_contact_id} not found in BigQuery records")
            
        target_eval_id = target_record['EvaluationId']  # Get the actual evaluation ID from the record

        updates = []
        
        for date_str in DATES_TO_BACKFILL:
            print(f"\nProcessing date: {date_str}")
            
            try:
                # Get contact metadata directly instead of going through evaluation
                contact_metadata = connect_client.describe_contact(
                    InstanceId=instance_id,
                    ContactId=target_contact_id
                )
                print(f"Contact metadata: {contact_metadata}")
                
                if contact_metadata:
                    # Extract queue information from the correct path
                    contact_data = contact_metadata.get('Contact', {})
                    queue_info = contact_data.get('QueueInfo', {})
                    queue_id = queue_info.get('Id')
                    
                    # Exit immediately if queue_id is not found
                    if not queue_id:
                        print(f"No queue ID found for contact ID: {target_contact_id}")
                        return {
                            "statusCode": 400,
                            "body": json.dumps({
                                "error": f"No queue ID found for contact ID: {target_contact_id}"
                            })
                        }
                    
                    queue_name = get_queue_name(connect_client, queue_id)
                    # Exit immediately if queue_name is not found
                    if not queue_name:
                        print(f"No queue name found for queue ID: {queue_id}")
                        return {
                            "statusCode": 400,
                            "body": json.dumps({
                                "error": f"No queue name found for queue ID: {queue_id}"
                            })
                        }
                        
                    # Parse the initiation timestamp
                    initiation_timestamp = datetime.fromisoformat("2024-11-12 23:10:44.938000+00:00")
                    
                    # Get operating hours information
                    hours_result = was_call_during_open_hours(
                        connect_client,
                        instance_id,
                        queue_id,
                        initiation_timestamp,
                        contact_data.get('InitiationMethod')
                    )
                    
                    # Determine call status based on queue and agent information
                    agent_id = contact_data.get('AgentInfo', {}).get('Id')
                    call_status = 'ABANDONED'
                    if queue_id and agent_id:
                        call_status = 'HANDLED'
                    elif queue_id and not agent_id:
                        call_status = 'QUEUE_ABANDONED'
                    
                    # Verify queue information matches existing record
                    existing_queue_id = target_record.get('QueueId')
                    existing_queue_name = target_record.get('QueueName')
                    if existing_queue_id and existing_queue_name:  # Only verify if existing values are present
                        if queue_id != existing_queue_id or queue_name != existing_queue_name:
                            raise ValueError(f"Queue mismatch - Existing: {existing_queue_id}/{existing_queue_name} vs New: {queue_id}/{queue_name}")
                    
                    update = {
                        'EvaluationId': target_eval_id,
                        'QueueTimezone': hours_result.get('timezone'),
                        'LocalCallTime': hours_result.get('local_time'),
                        'InitiationMethod': contact_data.get('InitiationMethod'),
                        'DuringOperatingHours': hours_result.get('during_hours'),
                        'CallStatus': call_status,
                    }
                    updates.append(update)
                    print(f"Successfully processed contact")
                else:
                    print(f"No contact metadata found for contact ID: {target_contact_id}")

            except Exception as e:
                print(f"Error processing contact: {str(e)}")
                continue

        print(f"\nPrepared {len(updates)} records for update")

        if UPDATE_BIGQUERY:
            # Update BigQuery using DML
            for update in updates:
                update_query = f"""
                    UPDATE `{table_id}`
                    SET 
                        QueueTimezone = @queue_timezone,
                        LocalCallTime = @local_call_time,
                        InitiationMethod = @initiation_method,
                        DuringOperatingHours = @during_hours,
                        CallStatus = @call_status
                    WHERE EvaluationId = @eval_id
                """
                
                job_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("queue_timezone", "STRING", update['QueueTimezone']),
                        bigquery.ScalarQueryParameter("local_call_time", "TIMESTAMP", update['LocalCallTime']),
                        bigquery.ScalarQueryParameter("initiation_method", "STRING", update['InitiationMethod']),
                        bigquery.ScalarQueryParameter("during_hours", "BOOL", update['DuringOperatingHours']),
                        bigquery.ScalarQueryParameter("call_status", "STRING", update['CallStatus']),
                        bigquery.ScalarQueryParameter("eval_id", "STRING", update['EvaluationId'])
                    ]
                )
                
                client.query(update_query, job_config=job_config).result()
            
            print(f"Successfully updated {len(updates)} records in BigQuery")
        else:
            print("\nDRY RUN - BigQuery updates skipped. Sample of updates that would be made:")
            # Convert datetime to string for JSON serialization
            for update in updates[:5]:
                serializable_update = {
                    k: v.isoformat() if isinstance(v, datetime) else v 
                    for k, v in update.items()
                }
                print(json.dumps(serializable_update, indent=2))
            if len(updates) > 5:
                print(f"... and {len(updates) - 5} more records")

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Backfill process completed",
                "recordsProcessed": len(updates),
                "updatedBigQuery": UPDATE_BIGQUERY
            })
        }

    except Exception as e:
        print(f"Error in backfill process: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }