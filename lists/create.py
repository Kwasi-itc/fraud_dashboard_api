import json
import boto3
from botocore.exceptions import ClientError
import os
from datetime import datetime

dynamodb = boto3.resource('dynamodb')
table_name = os.environ["FRAUD_LISTS_TABLE"]
table = dynamodb.Table(table_name)

ALLOWED_LIST_TYPES = ["BLACKLIST", "WATCHLIST", "STAFFLIST"]

def lambda_handler(event, context):
    try:
        body = json.loads(event['body'])
        list_type = body['list_type']
        channel = body['channel']
        entity_type = body['entity_type']
        entity_id = body['entity_id']

        if list_type not in ALLOWED_LIST_TYPES:
            return {
                'statusCode': 400,
                'body': json.dumps(f"Error: Invalid list_type. Allowed types are {', '.join(ALLOWED_LIST_TYPES)}"),
                'headers': {
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Credentials': True,
                },
            }

        partition_key = f"{list_type}-{channel}-{entity_type}"
        sort_key = entity_id

        response = table.put_item(
            Item={
                'PARTITION_KEY': partition_key,
                'SORT_KEY': sort_key,
                'created_at': str(datetime.now())
            }
        )

        print("The response after putting item on DB is ", response)

        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Item created successfully'}),
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Credentials': True,
            },
        }
    except ClientError as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'message': f"Error: {str(e)}"}),
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Credentials': True,
            },
        }

