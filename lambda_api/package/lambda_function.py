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
            
            # Extract results and process each item
            results = json_data.get('results', [])
            for result in results:
                # Remove EvaluationError
                result.pop('EvaluationError', None)
                
                # Process ContactDetails
                if 'ContactDetails' in result and isinstance(result['ContactDetails'], dict):
                    contact_details = result['ContactDetails']
                    result['ContactDetails'] = {
                        'ContactId': contact_details.get('ContactId', ''),
                        'AgentId': contact_details.get('AgentId', ''),
                        'AgentName': contact_details.get('AgentName', ''),
                        'QueueId': contact_details.get('QueueId', ''),
                        'QueueName': contact_details.get('QueueName', ''),
                        'InitiationTimestamp': contact_details.get('InitiationTimestamp', ''),
                        'DisconnectTimestamp': contact_details.get('DisconnectTimestamp', ''),
                        'DurationSeconds': contact_details.get('DurationSeconds', ''),
                        'DuringOperatingHours': contact_details.get('DuringOperatingHours', ''),
                        'QueueTimezone': contact_details.get('QueueTimezone', ''),
                        'LocalCallTime': contact_details.get('LocalCallTime', ''),
                        'InitiationMethod': contact_details.get('InitiationMethod', ''),
                        'CallStatus': contact_details.get('CallStatus', '')
                    }
                
                # Process EvaluationDetails
                if 'EvaluationDetails' in result and isinstance(result['EvaluationDetails'], dict):
                    eval_details = result['EvaluationDetails']
                    result['EvaluationDetails'] = {
                        'Score': eval_details.get('Score', ''),
                        'EvaluationId': eval_details.get('EvaluationId', '')
                    }
                
                # Handle other nested structures
                for key, value in result.items():
                    if isinstance(value, (dict, list)) and key not in ['ContactDetails', 'EvaluationDetails']:
                        if isinstance(value, dict):
                            result[key] = list(value.values())[0] if value else ''
                        else:
                            result[key] = value[0] if value else ''
                
                combined_results.append(result)
        
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
                            # Define all possible ContactDetails fields with default empty values
                            contact_fields = {
                                'ContactId': '',
                                'AgentId': '',
                                'AgentName': '',
                                'QueueId': '',
                                'QueueName': '',
                                'InitiationTimestamp': '',
                                'DisconnectTimestamp': '',
                                'DurationSeconds': '',
                                'DuringOperatingHours': '',
                                'QueueTimezone': '',
                                'LocalCallTime': '',
                                'InitiationMethod': '',
                                'CallStatus': ''
                            }
                            # Update with actual values if they exist
                            for field, default in contact_fields.items():
                                flat_item[field] = value.get(field, default)
                        elif key == 'EvaluationDetails' and isinstance(value, dict):
                            # Extract Score and EvaluationId from EvaluationDetails
                            flat_item['Score'] = value.get('Score', '')
                            flat_item['EvaluationId'] = value.get('EvaluationId', '')
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

                # Get headers from all possible fields across all items
                fieldnames = set()
                for item in flattened_results:
                    fieldnames.update(item.keys())
                fieldnames = sorted(list(fieldnames))  # Convert to sorted list for consistent column ordering
                
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