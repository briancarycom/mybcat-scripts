import json
from package.lambda_function import lambda_handler

# Test date that can be modified in one place
TEST_DATE = "2024-11-14"

# Mock event data for JSON response
mock_event_json = {
    "queryStringParameters": {
        "date": TEST_DATE
    }
}

# Mock event data for CSV response
mock_event_csv = {
    "queryStringParameters": {
        "date": TEST_DATE,
        "returnCsv": "true"
    }
}

# Mock context class to simulate AWS Lambda environment
class MockContext:
    def __init__(self):
        self.function_name = "getCustomerEvaluationCSAT"
        self.memory_limit_in_mb = 128
        self.invoked_function_arn = "arn:aws:lambda:us-east-1:533267039664:function:getCustomerEvaluationsAPI"
        self.aws_request_id = "mock_request_id"

# Create a mock context object
mock_context = MockContext()

# Test JSON response
print("Testing JSON response:")
json_response = lambda_handler(mock_event_json, mock_context)
print(json.dumps(json_response, indent=2))

# Test CSV response
print("\nTesting CSV response:")
csv_response = lambda_handler(mock_event_csv, mock_context)
print(json.dumps(csv_response, indent=2))