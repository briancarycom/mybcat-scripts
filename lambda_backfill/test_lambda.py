import json
from package.lambda_function import lambda_handler

# Test dates that can be modified in one place
TEST_DATES = ['2024-11-12', '2024-11-13']

# Mock event data
mock_event = {
    # Empty event since the lambda function doesn't use event parameters
}

# Mock context class to simulate AWS Lambda environment
class MockContext:
    def __init__(self):
        self.function_name = "backfillEvaluationDetails"
        self.memory_limit_in_mb = 128
        self.invoked_function_arn = "arn:aws:lambda:us-east-1:533267039664:function:backfillEvaluationDetails"
        self.aws_request_id = "mock_request_id"

# Create a mock context object
mock_context = MockContext()

# Test the lambda function
print("Testing backfill lambda function:")
response = lambda_handler(mock_event, mock_context)
print(json.dumps(response, indent=2))
