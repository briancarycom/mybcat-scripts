import boto3
import pandas as pd
from datetime import datetime

# Load DynamoDB data from CSV and clean clientDID
dynamo_df = pd.read_csv('./inputs/dynamodb-data.csv')
dynamo_df['clientDID'] = dynamo_df['clientDID'].str.replace('+', '').str.replace("'", '').str.replace('-', '').str.replace(' ', '')

# Load Connect data from CSV and clean clientDID
connect_df = pd.read_csv('./inputs/connect_phone_numbers.csv')
connect_df['clientDID'] = connect_df['Phone Number'].str.replace('+', '').str.replace('-', '').str.replace(' ', '')
connect_df['queueName'] = connect_df['Contact flow/IVR'].fillna(connect_df['Description'])
connect_df = connect_df.rename(columns={'queueName': 'queueName'})

# Perform validation
comparison_df = pd.DataFrame(dynamo_df['queueName'])
comparison_df['clientDID_dynamodb'] = dynamo_df['clientDID']
comparison_df['clientDID_connect'] = None
comparison_df['connect_queue_name'] = None

# Check if DynamoDB phone numbers exist in Connect
for idx, row in comparison_df.iterrows():
    dynamo_number = row['clientDID_dynamodb']
    connect_match = connect_df[connect_df['clientDID'] == dynamo_number]
    if not connect_match.empty:
        comparison_df.at[idx, 'clientDID_connect'] = connect_match['clientDID'].iloc[0]
        comparison_df.at[idx, 'connect_queue_name'] = connect_match['queueName'].iloc[0]

# Define helper functions first
def get_validation_status(dynamo_number, connect_number, connect_queue):
    # Check phone number format
    if not is_valid_phone_number(dynamo_number):
        return f"Invalid DynamoDB number format: {get_number_error(dynamo_number)}"
    
    if connect_number is not None:
        if not is_valid_phone_number(connect_number):
            return f"Invalid Connect number format: {get_number_error(connect_number)}"
        return f"Number Found with Different Queue Name: {connect_queue}"
    
    return "Phone Number Not Found in Connect"

def is_valid_phone_number(number):
    # Remove '+', '-', and spaces, then check if remaining digits are exactly 11
    digits = number.replace('+', '').replace('-', '').replace(' ', '')
    return digits.isdigit() and len(digits) == 11

def get_number_error(number):
    digits = number.replace('+', '')
    if not digits.isdigit():
        return "Contains non-digit characters"
    if len(digits) > 11:
        return f"Too many digits ({len(digits)})"
    if len(digits) < 11:
        return f"Too few digits ({len(digits)})"
    return "Unknown error"

# Then use the functions in the comparison
comparison_df['match'] = comparison_df.apply(
    lambda row: (
        row['clientDID_dynamodb'] == row['clientDID_connect'] and 
        is_valid_phone_number(row['clientDID_dynamodb']) and 
        (is_valid_phone_number(row['clientDID_connect']) if pd.notna(row['clientDID_connect']) else True)
    ),
    axis=1
)

# Add phone number validity columns
comparison_df['dynamodb_number_valid'] = comparison_df['clientDID_dynamodb'].apply(is_valid_phone_number)
comparison_df['connect_number_valid'] = comparison_df['clientDID_connect'].apply(lambda x: is_valid_phone_number(x) if pd.notna(x) else None)
comparison_df['status'] = comparison_df.apply(
    lambda row: (
        "Exact Match" if row['match'] else
        get_validation_status(row['clientDID_dynamodb'], row['clientDID_connect'], row['connect_queue_name'])
    ),
    axis=1
)

# Suggestions for corrections
corrections = comparison_df[comparison_df['status'] != "Exact Match"]

# Sort the comparison DataFrame alphabetically by queue name before saving
comparison_df = comparison_df.sort_values('queueName')
corrections = corrections.sort_values('queueName')

# Get current timestamp
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

# Save results to CSV for review with timestamps
comparison_df.to_csv(f"dynamo_connect_comparison_{timestamp}.csv", index=False)
corrections.to_csv(f"dynamo_connect_corrections_{timestamp}.csv", index=False)

# Display results
print(f"Validation complete. Results saved to 'dynamo_connect_comparison_{timestamp}.csv'.")
print(f"Corrections suggested. Details saved to 'dynamo_connect_corrections_{timestamp}.csv'.")

# Add phone number comparison
print("\nComparing phone numbers between DynamoDB and Connect:")
print("=" * 80)

# Create sets for easy comparison
dynamo_numbers = set(dynamo_df['clientDID'])
connect_numbers = set(connect_df['clientDID'])

# Prepare data for CSV export
comparison_report = []

# Find numbers that are in DynamoDB but not in Connect
only_in_dynamo = dynamo_numbers - connect_numbers
if only_in_dynamo:
    print("\nNumbers in DynamoDB but not in Connect:")
    for num in sorted(only_in_dynamo):
        queue_name = dynamo_df[dynamo_df['clientDID'] == num]['queueName'].iloc[0]
        print(f"- {num} ({queue_name})")
        comparison_report.append({
            'phone_number': num,
            'queue_name': queue_name,
            'status': 'Only in DynamoDB'
        })

# Find numbers that are in Connect but not in DynamoDB
only_in_connect = connect_numbers - dynamo_numbers
if only_in_connect:
    print("\nNumbers in Connect but not in DynamoDB:")
    for num in sorted(only_in_connect):
        queue_name = connect_df[connect_df['clientDID'] == num]['queueName'].iloc[0]
        print(f"- {num} ({queue_name})")
        comparison_report.append({
            'phone_number': num,
            'queue_name': queue_name,
            'status': 'Only in Connect'
        })

# Add matching numbers to report
matching_numbers = dynamo_numbers.intersection(connect_numbers)
for num in sorted(matching_numbers):
    queue_name = connect_df[connect_df['clientDID'] == num]['queueName'].iloc[0]
    comparison_report.append({
        'phone_number': num,
        'queue_name': queue_name,
        'status': 'Present in Both'
    })

# Export comparison report to CSV
comparison_report_df = pd.DataFrame(comparison_report)
comparison_report_df = comparison_report_df.sort_values(['status', 'queue_name'])
comparison_report_df.to_csv(f"phone_number_comparison_{timestamp}.csv", index=False)

# Show matching numbers count
print(f"\nSummary:")
print(f"- Total matching numbers: {len(matching_numbers)}")
print(f"- Numbers only in DynamoDB: {len(only_in_dynamo)}")
print(f"- Numbers only in Connect: {len(only_in_connect)}")
print(f"\nPhone number comparison report saved to 'phone_number_comparison_{timestamp}.csv'")