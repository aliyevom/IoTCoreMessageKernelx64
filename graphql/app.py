import json
import os
from schema import schema  # Import the GraphQL schema
from graphql_server import lambda_http  # GraphQL over AWS Lambda

# Lambda handler for AWS Lambda
def lambda_handler(event, context):
    """
    AWS Lambda handler for processing GraphQL requests via API Gateway.
    """
    response = lambda_http.get_graphql_response(
        event=event, context=context, schema=schema, graphiql=True
    )

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(response)
    }
