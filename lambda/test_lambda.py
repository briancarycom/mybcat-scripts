import json
from package.lambda_function import lambda_handler

# Mock event data
mock_event = {
    # Add any necessary mock data if the function requires input.
}

# Mock context class to simulate AWS Lambda environment
class MockContext:
    def __init__(self):
        self.function_name = "getCustomerEvaluationCSAT"
        self.memory_limit_in_mb = 128
        self.invoked_function_arn = "arn:aws:lambda:us-east-1:533267039664:function:getCustomerEvaluationCSAT"
        self.aws_request_id = "mock_request_id"

# Create a mock context object
mock_context = MockContext()

# Invoke the lambda_handler function with the mock event and context
response = lambda_handler(mock_event, mock_context)

# Print the response
print(json.dumps(response, indent=2))
