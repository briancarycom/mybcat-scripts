import boto3
import json
from datetime import datetime
import csv
from io import StringIO

s3_client = boto3.client('s3')
bucket_name = 'custom-reports'

def lambda_handler(event, context):
    try:
        # Parse the parameters from the API query
        query_params = event.get('queryStringParameters', {})
        date_str = query_params.get('date')
        return_csv = query_params.get('returnCsv', '').lower() == 'true'
        
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
            
            # Extract results and remove EvaluationError from each item
            results = json_data.get('results', [])
            for result in results:
                result.pop('EvaluationError', None)
            combined_results.extend(results)
        
        # Handle CSV return if requested
        if return_csv:
            if not combined_results:
                return {
                    "statusCode": 404,
                    "body": "No data available for CSV export"
                }
            
            # Create CSV from flattened results
            output = StringIO()
            if combined_results:
                # Flatten nested structures and parse JSON values
                flattened_results = []
                for result in combined_results:
                    flat_item = {}
                    for key, value in result.items():
                        if key == 'ContactDetails' and isinstance(value, dict):
                            # Extract all fields from ContactDetails
                            for contact_key, contact_value in value.items():
                                flat_item[contact_key] = contact_value
                        elif key == 'EvaluationDetails' and isinstance(value, dict):
                            # Extract Score from EvaluationDetails
                            flat_item['Score'] = value.get('Score', '')
                        elif isinstance(value, (dict, list)):
                            # Handle other nested structures
                            try:
                                if isinstance(value, dict):
                                    flat_item[key] = list(value.values())[0] if value else ''
                                else:
                                    flat_item[key] = value[0] if value else ''
                            except (IndexError, KeyError):
                                flat_item[key] = ''
                        else:
                            flat_item[key] = value
                    flattened_results.append(flat_item)

                # Get headers from the first item's keys
                fieldnames = list(flattened_results[0].keys())
                writer = csv.DictWriter(output, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(flattened_results)

            return {
                "statusCode": 200,
                "headers": {
                    "Content-Type": "text/csv",
                    "Content-Disposition": f"attachment; filename=report_{date_str}.csv"
                },
                "body": output.getvalue()
            }
        
        # Return JSON if CSV not requested
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