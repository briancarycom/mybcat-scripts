import os
import json
from datetime import datetime, date
import requests
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv
import csv
from io import StringIO

# Configuration variables
PROJECT_ID = "mybcat"
DATASET_ID = "amazon_connect_evaluations_test"
TABLE_ID = "user_evaluations"
API_ENDPOINT = "https://rmjs3kanak.execute-api.us-east-1.amazonaws.com/evaluations"

def clean_record(record):
    """Clean and transform record data before loading to BigQuery"""
    if 'Score' in record and record['Score']:
        try:
            # Convert Score to float and round to nearest integer
            record['Score'] = round(float(record['Score']))
        except (ValueError, TypeError):
            print(f"Warning: Invalid Score value: {record['Score']}, setting to None")
            record['Score'] = None
    return record

def lambda_handler(event, context):
    try:
        # print(f"Starting lambda execution with event: {event}")  # Commented out as it could contain large payloads
        
        # Add test_mode parameter check
        test_mode = event.get('queryStringParameters', {}).get('test_mode', '').lower() == 'true'
        if test_mode:
            print("Running in test mode - will only process first record")

        # Use current date if not specified
        query_date = date.today().strftime('%Y-%m-%d')
        if event.get('queryStringParameters', {}).get('date'):
            query_date = event.get('queryStringParameters', {}).get('date')
            print(f"Using provided date: {query_date}")
            
            # Validate date format
            try:
                datetime.strptime(query_date, '%Y-%m-%d')
            except ValueError:
                print(f"Invalid date format provided: {query_date}")
                return {
                    "statusCode": 400,
                    "body": json.dumps({"error": "Invalid date format. Use YYYY-MM-DD."})
                }
        
        print(f"Making API request to {API_ENDPOINT} with date {query_date}")
        # Fetch data from API with parameters
        params = {
            'date': query_date,
            'returnCsv': 'true'
        }
        response = requests.get(API_ENDPOINT, params=params)
        if response.status_code != 200:
            print(f"API request failed with status {response.status_code}: {response.text}")
            return {
                "statusCode": response.status_code,
                "body": json.dumps({"error": f"API request failed with status {response.status_code}"})
            }
        
        print("API request successful")
        # Parse CSV response data
        csv_data = StringIO(response.text)
        reader = csv.DictReader(csv_data)
        records = list(reader)
        print(f"Received {len(records)} records from API")
        
        if not records:
            print(f"No data found for date {query_date}")
            return {
                "statusCode": 404,
                "body": json.dumps({"error": f"No data found for date {query_date}"})
            }
        
        if test_mode and records:
            records = [records[0]]  # Keep only the first record in test mode
            print("Test mode: Processing only first record:")
            for key, value in records[0].items():
                print(f"{key}: {value}")

        # Clean records before processing
        records = [clean_record(record) for record in records]
        
        print(f"Initializing BigQuery client for table {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}")
        # Initialize BigQuery client with credentials
        if os.getenv('AWS_LAMBDA_FUNCTION_NAME') is None:
            # Only load .env file if not running in Lambda
            load_dotenv()
            
        credentials_dict = json.loads(os.environ['GOOGLE_APPLICATION_CREDENTIALS_JSON'])
        credentials = service_account.Credentials.from_service_account_info(credentials_dict)
        client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
        table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
        
        # debug by printing the last record
        # print(f"Last record: {records[-1]}")  # Commented out detailed record data
        # print("Last record key-value pairs:")  # Commented out verbose debugging
        # for key, value in records[-1].items():
        #     print(f"{key}: {value}")

        # Simplified existing records check
        evaluation_ids = [record.get('EvaluationId') for record in records]
        # print(f"EvaluationIds: {evaluation_ids}")  # Commented out potentially large array

        print(f"Checking for existing records")  # Simplified message
        
        query = f"""
            SELECT EvaluationId
            FROM `{table_id}`
            WHERE EvaluationId IN UNNEST(@ids)
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("ids", "STRING", evaluation_ids),
            ]
        )
        
        existing_ids = {row['EvaluationId'] for row in client.query(query, job_config=job_config)}
        # Simplified records comparison (no nested data handling needed)
        records_to_load = [record for record in records 
                          if record.get('EvaluationId') not in existing_ids]
        
        if not records_to_load:
            print("No new records to load")
            return {
                "statusCode": 200,
                "body": json.dumps({
                    "message": "No new records to load",
                    "date": query_date
                })
            }
            
        print(f"Loading {len(records_to_load)} new records")
        # Load data into BigQuery
        job = client.load_table_from_json(
            records_to_load,
            table_id,
            job_config=bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND"
            )
        )
        job.result()  # Wait for the job to complete
        print("BigQuery load job completed successfully")
        
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Data loaded successfully",
                "records_processed": len(records),
                "date": query_date
            })
        }
        
    except Exception as e:
        print(f"Error processing request: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
