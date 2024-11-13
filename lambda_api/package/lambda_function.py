import boto3
import json
from datetime import datetime

s3_client = boto3.client('s3')
bucket_name = 'custom-reports'

def lambda_handler(event, context):
    try:
        # Parse the 'date' parameter from the API query
        date_str = event.get('queryStringParameters', {}).get('date')
        if not date_str:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "Date parameter is required (format: YYYY-MM-DD)"})
            }
        
        # Validate date format
        try:
            query_date = datetime.strptime(date_str, '%Y-%m-%d')
        except ValueError:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "Invalid date format. Use YYYY-MM-DD."})
            }
        
        # Define S3 prefix for the given date
        year = query_date.strftime('%Y')
        month = query_date.strftime('%m')
        day = query_date.strftime('%d')
        prefix = f"{year}/{month}/{day}/"
        
        # List objects in the S3 prefix
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        if 'Contents' not in response:
            return {
                "statusCode": 404,
                "body": json.dumps({"error": f"No files found for date {date_str}"})
            }
        
        # Collect results from all files
        combined_results = []
        for obj in response['Contents']:
            key = obj['Key']
            file_obj = s3_client.get_object(Bucket=bucket_name, Key=key)
            file_content = file_obj['Body'].read().decode('utf-8')
            json_data = json.loads(file_content)
            
            # Extract and append the results array
            combined_results.extend(json_data.get('results', []))
        
        # Return combined results
        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json"
            },
            "body": json.dumps({"date": date_str, "results": combined_results})
        }
    
    except Exception as e:
        return {
            "statusCode": 500,
            "headers": {
                "Content-Type": "application/json"
            },
            "body": json.dumps({"error": str(e)})
        }