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
        print("The event is ", event)
        body = json.loads(event['body'])
        print("The body is ", body)
        list_type = body['list_type']
        channel = body['channel']
        entity_type = body['entity_type']
        account_id = body['account_id']
        application_id = body['application_id']
        merchant_id = body['merchant_id']
        product_id = body['product_id']

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
        sort_key = ""

        sort_key = ""
        if entity_type == "ACCOUNT":
            sort_key = account_id
        elif entity_type == "APPLICATION":
            sort_key = application_id
        elif entity_type == "MERCHANT":
            sort_key = application_id + "__" + merchant_id
        elif entity_type == "PRODUCT":
            sort_key = application_id + "__" + merchant_id + "__" + product_id
        else:
            return {
            'statusCode': 500,
            'body': json.dumps({'message': "entity type must be ACCOUNT | APPLICATION | MERCHANT | PRODUCT"}),
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Credentials': True,
            },
        }

        response = table.update_item(
            Key={
                'PARTITION_KEY': partition_key,
                'SORT_KEY': sort_key
            },
            UpdateExpression="set updated_at = :val",
            ExpressionAttributeValues={
                ':val': str(datetime.now())
            },
            ReturnValues="UPDATED_NEW"
        )

        print("The response after updating item on DB is ", response)

        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Item updated successfully'}),
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Credentials': True,
            },
        }
    except ClientError as e:
        print("An error occured ", e)
        return {
            'statusCode': 500,
            'body': json.dumps({'message': f"Error: {str(e)}"}),
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Credentials': True,
            },
        }