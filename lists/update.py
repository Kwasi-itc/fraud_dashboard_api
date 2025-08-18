import json
import boto3
from botocore.exceptions import ClientError
import os
from datetime import datetime
from decimal import Decimal

dynamodb = boto3.resource('dynamodb')
table_name = os.environ["FRAUD_LISTS_TABLE"]
table = dynamodb.Table(table_name)

ALLOWED_LIST_TYPES = ["BLACKLIST", "WATCHLIST", "STAFFLIST"]

def decimal_default(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError

def response(status_code, body):
    response_message = "Operation Successful" if status_code == 200 else "Unsuccessful operation"
    body_to_send = {
        "responseCode": status_code,
        "responseMessage": response_message,
        "data": body
    }
    return {
        'statusCode': status_code,
        'body': json.dumps(body_to_send, default=decimal_default),
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Credentials': True,
        },
    }

def lambda_handler(event, context):
    try:
        print("The event is ", event)
        body = json.loads(event['body'])
        print("The body is ", body)
        list_type = body['list_type']
        channel = body['channel']
        channel = channel.lower()
        entity_type = body['entity_type']
        account_id = body.get("account_ref")#body.get('account_id')
        application_id = body.get('processor')#body.get('application_id')
        merchant_id = body.get('merchant_id')
        product_id = body.get('product_id')

        if list_type not in ALLOWED_LIST_TYPES:
            return response(400, f"Error: Invalid list_type. Allowed types are {', '.join(ALLOWED_LIST_TYPES)}")

        partition_key = f"{list_type}-{channel}-{entity_type}"
        sort_key = ""

        if entity_type == "ACCOUNT":
            sort_key = account_id
        elif entity_type == "APPLICATION" or entity_type == "PROCESSOR":
            sort_key = application_id
        elif entity_type == "MERCHANT":
            sort_key = application_id + "__" + merchant_id
        elif entity_type == "PRODUCT":
            sort_key = application_id + "__" + merchant_id + "__" + product_id
        else:
            return response(500, "entity type must be ACCOUNT | PROCESSOR | MERCHANT | PRODUCT")

        response_db = table.update_item(
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

        print("The response after updating item on DB is ", response_db)

        return response(200, {'message': 'Item updated successfully'})
    except ClientError as e:
        print("An error occurred ", e)
        return response(500, {'message': f"Error: {str(e)}"})

### Updates can only be done to the ids