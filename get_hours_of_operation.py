import boto3
import json
from datetime import datetime
import pytz

# Initialize the Amazon Connect client
client = boto3.client('connect', region_name='us-east-1')

# Instance ID for Amazon Connect
instance_id = "c3175ce9-154c-46cb-a559-94cdbbb3583a"

# List of HoursOfOperation IDs
hours_of_operation_ids = [
    "0228fe44-fc09-46f0-b844-0ea2afd9fe0c",
    "0d099dcc-acb0-4841-9639-2667dbd348ac",
    "18775782-333b-4195-9f34-3b78632606bb",
    "204343ac-966e-4552-aa1c-f8c050a1b52d",
    "218520d6-5930-4864-9788-6a6f38336d3d",
    "23812a0b-2b79-4f7a-abd9-4fe571580bfc",
    "2e87d8ed-d908-47ee-8c38-ec60823e4fd5",
    "3020a38d-3078-47e3-9616-3e752dab62f9",
    "398bf95b-19bb-4b17-a794-192a47db0cfa",
    "46c98484-0fcf-4b32-a939-907969d96fd4",
    "48688ec9-fda8-4fd5-bc3e-6e2d4e76d520",
    "58e24026-53f6-48ee-8858-7e7ba8fae296",
    "6264e6f0-4777-451f-af93-22c20e6a3d6b",
    "7db5fa90-7671-4825-a43e-34352dbf2d7c",
    "80151ade-a5c4-4387-b6f3-21a74a79c718",
    "8c922b6e-97db-4047-b917-5c24b1efe55b",
    "c17f4b6c-1a43-4530-8c74-9c7efc7eb532",
    "c348ad8d-61d0-4bc9-af4b-e80f1b68c63f",
    "c36b7ad2-b25a-4ec6-b5f2-d1b61474bf6a",
    "cb61c5f0-22c8-4b7a-a535-a062a0df2e72",
    "cddeacfa-b177-443e-94c2-47faa4dc83d3",
    "ce001a9b-4ab6-4d2f-843a-82c609e851bb",
    "d1cd6352-670f-4489-8c0e-a03cd24b422e",
    "e709c357-e3e9-4760-aff8-4cc86221ce7e",
    "e761d900-8402-4f05-a9e6-aa94cac3cb65",
    "f343c3da-bcfc-4719-a73a-ad2e24bc5000",
    "fcae60ff-fe7c-47a3-a6fb-ac60f5da6e2d"
]

# Container for results
result = []

# Retrieve hours of operation details for each ID
for hours_id in hours_of_operation_ids:
    response = client.describe_hours_of_operation(
        InstanceId=instance_id,
        HoursOfOperationId=hours_id
    )
    
    hours = response['HoursOfOperation']
    config = hours['Config']
    
    # Convert times to UTC for each day's configuration
    for day_config in config:
        local_tz = pytz.timezone(hours['TimeZone'])
        utc_tz = pytz.UTC
        
        # Create a datetime object for start time
        start_time = day_config['StartTime']
        start_dt = datetime.now(local_tz).replace(
            hour=int(start_time['Hours']),
            minute=int(start_time['Minutes']),
            second=0,
            microsecond=0
        )
        utc_start = start_dt.astimezone(utc_tz)
        
        # Create a datetime object for end time
        end_time = day_config['EndTime']
        end_dt = datetime.now(local_tz).replace(
            hour=int(end_time['Hours']),
            minute=int(end_time['Minutes']),
            second=0,
            microsecond=0
        )
        utc_end = end_dt.astimezone(utc_tz)
        
        # Add UTC times to the configuration
        day_config['UTCStartTime'] = {
            'Hours': utc_start.hour,
            'Minutes': utc_start.minute
        }
        day_config['UTCEndTime'] = {
            'Hours': utc_end.hour,
            'Minutes': utc_end.minute
        }
    
    entry = {
        "Name": hours['Name'],
        "Description": hours.get('Description', ""),
        "TimeZone": hours['TimeZone'],
        "Hours": config
    }
    result.append(entry)

# Save results to JSON file
output_file = 'hours_of_operation.json'
with open(output_file, 'w') as file:
    json.dump(result, file, indent=4)

print(f"Hours of operation saved to {output_file}")