import json
from datetime import datetime, date
import requests
from google.cloud import bigquery

# Configuration variables
PROJECT_ID = "mybcat"
DATASET_ID = "your_dataset"
TABLE_ID = "your_table"
API_ENDPOINT = "https://rmjs3kanak.execute-api.us-east-1.amazonaws.com/evaluations"

def lambda_handler(event, context):
    try:
        # Use current date if not specified
        query_date = date.today().strftime('%Y-%m-%d')
        if event.get('queryStringParameters', {}).get('date'):
            query_date = event.get('queryStringParameters', {}).get('date')
            
            # Validate date format
            try:
                datetime.strptime(query_date, '%Y-%m-%d')
            except ValueError:
                return {
                    "statusCode": 400,
                    "body": json.dumps({"error": "Invalid date format. Use YYYY-MM-DD."})
                }
        
        # Fetch data from API
        response = requests.get(f"{API_ENDPOINT}?date={query_date}")
        if response.status_code != 200:
            return {
                "statusCode": response.status_code,
                "body": json.dumps({"error": f"API request failed with status {response.status_code}"})
            }
        
        # Parse response data
        api_data = response.json()
        if not api_data.get('results'):
            return {
                "statusCode": 404,
                "body": json.dumps({"error": f"No data found for date {query_date}"})
            }
        
        # Initialize BigQuery client
        client = bigquery.Client()
        table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
        
        # Load data into BigQuery
        job = client.load_table_from_json(
            api_data['results'],
            table_id,
            job_config=bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND"
            )
        )
        job.result()  # Wait for the job to complete
        
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Data loaded successfully",
                "records_processed": len(api_data['results']),
                "date": query_date
            })
        }
        
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
