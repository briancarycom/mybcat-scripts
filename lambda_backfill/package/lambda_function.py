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
import time

# Configuration
UPDATE_BIGQUERY = True  # Changed to True to perform the update
DATES_TO_BACKFILL = ['2024-11-12']
TEST_MODE = False  # Keeping test mode on to process only the sample
SAMPLE_EVAL_ID = "7d582bdd-0ddc-445e-8396-0b139af768ba"
SAMPLE_CONTACT_ID = "a24b289a-426b-4a9e-a449-548dd52d3900"
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

    connect_client = boto3.client('connect')

    try:
        # Initialize BigQuery client
        if os.getenv('AWS_LAMBDA_FUNCTION_NAME') is None:
            load_dotenv()
        
        credentials_dict = json.loads(os.environ['GOOGLE_APPLICATION_CREDENTIALS_JSON'])
        credentials = service_account.Credentials.from_service_account_info(credentials_dict)
        client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
        table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

        # Modify query based on TEST_MODE
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
            {f"WHERE ContactId = '{SAMPLE_CONTACT_ID}'" if TEST_MODE else ""}
            ORDER BY LocalCallTime ASC
        """

        query_result = client.query(query).result()
        existing_records = {row['ContactId']: row for row in query_result}
        print(f"Found {len(existing_records)} existing records in BigQuery")
        
        if TEST_MODE:
            print(f"Running in TEST MODE with sample contact ID: {SAMPLE_CONTACT_ID}")

        updates = []
        
        for date_str in DATES_TO_BACKFILL:
            print(f"\nProcessing date: {date_str}")
            
            for contact_id, record in existing_records.items():
                try:
                    contact_metadata = connect_client.describe_contact(
                        InstanceId=instance_id,
                        ContactId=contact_id
                    )
                    print(f"Contact metadata: {contact_metadata}")
                    
                    if contact_metadata:
                        # Extract queue information from the correct path
                        contact_data = contact_metadata.get('Contact', {})
                        queue_info = contact_data.get('QueueInfo', {})
                        queue_id = queue_info.get('Id')
                        
                        # Exit immediately if queue_id is not found
                        if not queue_id:
                            print(f"No queue ID found for contact ID: {contact_id}")
                            return {
                                "statusCode": 400,
                                "body": json.dumps({
                                    "error": f"No queue ID found for contact ID: {contact_id}"
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
                        existing_queue_id = record.get('QueueId')
                        existing_queue_name = record.get('QueueName')
                        if existing_queue_id and existing_queue_name:  # Only verify if existing values are present
                            if queue_id != existing_queue_id or queue_name != existing_queue_name:
                                raise ValueError(f"Queue mismatch - Existing: {existing_queue_id}/{existing_queue_name} vs New: {queue_id}/{queue_name}")
                        
                        update = {
                            'EvaluationId': record['EvaluationId'],  # Use the current record
                            'QueueTimezone': hours_result.get('timezone'),
                            'LocalCallTime': hours_result.get('local_time'),
                            'InitiationMethod': contact_data.get('InitiationMethod'),
                            'DuringOperatingHours': hours_result.get('during_hours'),
                            'CallStatus': call_status,
                        }
                        updates.append(update)
                        print(f"Successfully processed contact")
                    else:
                        print(f"No contact metadata found for contact ID: {contact_id}")

                except Exception as e:
                    print(f"Error processing contact: {str(e)}")
                    continue

        print(f"\nPrepared {len(updates)} records for update")

        if UPDATE_BIGQUERY:
            print(f"\nUpdating {len(updates)} records in BigQuery with throttling...")
            
            # Process in batches of 10 with a small delay between batches
            BATCH_SIZE = 10
            for i in range(0, len(updates), BATCH_SIZE):
                batch = updates[i:i + BATCH_SIZE]
                print(f"Processing batch {i//BATCH_SIZE + 1} of {(len(updates) + BATCH_SIZE - 1)//BATCH_SIZE}")
                
                for update in batch:
                    update_query = f"""
                        UPDATE `{table_id}`
                        SET 
                            QueueTimezone = @queue_timezone,
                            LocalCallTime = FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%E6S%Ez', @local_call_time),
                            InitiationMethod = @initiation_method,
                            DuringOperatingHours = CAST(@during_hours AS STRING),
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
                    
                    # Execute query and wait for it to complete
                    query_job = client.query(update_query, job_config=job_config)
                    query_job.result()  # Wait for the job to complete
                
                # Add a small delay between batches (0.5 seconds)
                if i + BATCH_SIZE < len(updates):
                    time.sleep(0.5)
            
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