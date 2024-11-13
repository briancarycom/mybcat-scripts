import os
import json
from datetime import datetime, date
import requests
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv

# Configuration variables
PROJECT_ID = "mybcat"
DATASET_ID = "amazon_connect_evaluations"
TABLE_ID = "user_evaluations"
API_ENDPOINT = "https://rmjs3kanak.execute-api.us-east-1.amazonaws.com/evaluations"

def lambda_handler(event, context):
    try:
        print(f"Starting lambda execution with event: {event}")
        
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
            'requestCsv': 'true'
        }
        response = requests.get(API_ENDPOINT, params=params)
        if response.status_code != 200:
            print(f"API request failed with status {response.status_code}: {response.text}")
            return {
                "statusCode": response.status_code,
                "body": json.dumps({"error": f"API request failed with status {response.status_code}"})
            }
        
        print("API request successful")
        # Parse response data
        api_data = response.json()
        print(f"Received {len(api_data.get('results', []))} records from API")
        if not api_data.get('results'):
            print(f"No data found for date {query_date}")
            return {
                "statusCode": 404,
                "body": json.dumps({"error": f"No data found for date {query_date}"})
            }
        
        print(f"Initializing BigQuery client for table {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}")
        # Initialize BigQuery client with credentials
        if os.getenv('AWS_LAMBDA_FUNCTION_NAME') is None:
            # Only load .env file if not running in Lambda
            load_dotenv()
            
        credentials_dict = json.loads(os.environ['GOOGLE_APPLICATION_CREDENTIALS_JSON'])
        credentials = service_account.Credentials.from_service_account_info(credentials_dict)
        client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
        table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
        
        # Check for existing records
        new_records = api_data['results']
        evaluation_ids = [record['evaluation_id'] for record in new_records]  # Adjust field name as needed
        
        query = f"""
            SELECT evaluation_id
            FROM `{table_id}`
            WHERE evaluation_id IN UNNEST(@ids)
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayParameter("ids", "STRING", evaluation_ids),
            ]
        )
        
        existing_ids = {row['evaluation_id'] for row in client.query(query, job_config=job_config)}
        records_to_load = [record for record in new_records if record['evaluation_id'] not in existing_ids]
        
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
                "records_processed": len(api_data['results']),
                "date": query_date
            })
        }
        
    except Exception as e:
        print(f"Error processing request: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
