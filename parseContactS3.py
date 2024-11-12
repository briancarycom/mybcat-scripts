import boto3
import json
from collections import defaultdict

# Initialize S3 client
s3 = boto3.client('s3')

# Define your S3 bucket and prefix
bucket_name = 'amazon-connect-d77cd4560d16'
prefix = 'connect-data/2024/'

# Modified aggregation function to normalize scores and organize by timestamp
def aggregate_survey_results(survey_data):
    # Initialize time series dictionary
    time_series = defaultdict(lambda: defaultdict(list))
    
    # Process each survey response
    for record in survey_data:
        timestamp = record['LastUpdateTimestamp']
        # Add quality score processing
        if 'quality_score' in record:
            time_series[timestamp]['quality_score'].append(record['quality_score'])
            
        for key, value in record.items():
            if key.startswith("survey_result_"):
                # Normalize the score from 0-5 to 0-100%
                normalized_score = (int(value) / 5) * 100
                time_series[timestamp][key].append(normalized_score)
    
    # Calculate averages for each timestamp
    normalized_results = {}
    for timestamp, scores in sorted(time_series.items()):
        normalized_results[timestamp] = {
            k: sum(v) / len(v) for k, v in scores.items()
        }
    
    return normalized_results

# Function to parse and process files
def parse_s3_objects(bucket, prefix):
    survey_data = {}
    result_object = {
        "individual_responses": {},
        "time_series_results": {},
        "metadata": {
            "score_range": "0-100%",
            "original_range": "0-5"
        }
    }

    # List all objects under the prefix
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if 'Contents' not in response:
        print("No files found in the specified prefix.")
        return result_object

    for obj in response['Contents']:
        key = obj['Key']
        print(f"Processing file: {key}")

        # Get the object content
        file_content = s3.get_object(Bucket=bucket, Key=key)['Body'].read().decode('utf-8')

        # Handle concatenated or newline-delimited JSON
        for line in file_content.splitlines():
            try:
                record = json.loads(line)
                if 'Attributes' in record and 'ContactId' in record:
                    contact_id = record['ContactId']
                    survey_attributes = {
                        k: v for k, v in record['Attributes'].items() if k.startswith('survey_')
                    }
                    # Only store if we have survey data
                    if survey_attributes:
                        survey_attributes['LastUpdateTimestamp'] = record.get('LastUpdateTimestamp', '')
                        # Add quality score if available
                        if 'QualityMetrics' in record and 'Agent' in record['QualityMetrics']:
                            quality_score = record['QualityMetrics']['Agent'].get('Audio', {}).get('QualityScore')
                            if quality_score is not None:
                                survey_attributes['quality_score'] = quality_score
                        survey_data[contact_id] = survey_attributes
            except json.JSONDecodeError as e:
                print(f"Error parsing line: {line[:100]}... Error: {e}")

    # Store individual responses in result object
    result_object["individual_responses"] = survey_data

    # Update the aggregation section
    if survey_data:
        result_object["time_series_results"] = aggregate_survey_results(survey_data.values())
        
        print("\nTime Series Survey Results:")
        for timestamp, results in result_object["time_series_results"].items():
            print(f"\nTimestamp: {timestamp}")
            for key, value in results.items():
                print(f"{key}: {value:.2f}%")

    return result_object

# Modify the function call to capture the return value
result = parse_s3_objects(bucket_name, prefix)
print("\nFinal JSON result:")
print(json.dumps(result, indent=2))