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

def get_sort_key(entity_type, account_id, application_id, merchant_id, product_id):
    if entity_type == "ACCOUNT":
        return account_id
    elif entity_type == "APPLICATION" or entity_type == "PROCESSOR":
        return application_id
    elif entity_type == "MERCHANT":
        return f"{application_id}__{merchant_id}"
    elif entity_type == "PRODUCT":
        return f"{application_id}__{merchant_id}__{product_id}"
    else:
        raise ValueError("entity type must be ACCOUNT | PROCESSOR | MERCHANT | PRODUCT")

def lambda_handler(event, context):
    try:
        print("The event is ", event)
        body = json.loads(event['body'])
        print("The body is ", body)
        
        # Extract basic parameters
        list_type = body['list_type']
        channel = body['channel']
        entity_type = body['entity_type']
        
        # Validate list type
        if list_type not in ALLOWED_LIST_TYPES:
            return response(400, f"Error: Invalid list_type. Allowed types are {', '.join(ALLOWED_LIST_TYPES)}")

        # Extract current and new IDs
        current_ids = body.get('current_ids', {})
        new_ids = body.get('new_ids', {})
        
        if not current_ids or not new_ids:
            return response(400, "Both current_ids and new_ids are required")

        # Generate partition key
        partition_key = f"{list_type}-{channel}-{entity_type}"
        
        # Generate current and new sort keys
        try:
            current_sort_key = get_sort_key(
                entity_type,
                current_ids.get("account_id"),
                current_ids.get("application_id"),
                current_ids.get("merchant_id"),
                current_ids.get("product_id")
            )
            
            new_sort_key = get_sort_key(
                entity_type,
                new_ids.get("account_id"),
                new_ids.get("application_id"),
                new_ids.get("merchant_id"),
                new_ids.get("product_id")
            )
        except ValueError as e:
            return response(400, str(e))

        # First, get the existing item
        try:
            existing_item = table.get_item(
                Key={
                    'PARTITION_KEY': partition_key,
                    'SORT_KEY': current_sort_key
                }
            ).get('Item')
        except ClientError as e:
            print(f"Error getting existing item: {e}")
            return response(500, {'message': f"Error retrieving existing item: {str(e)}"})

        if not existing_item:
            return response(404, {'message': 'Item not found'})

        # Create new item with updated sort key
        new_item = {
            'PARTITION_KEY': partition_key,
            'SORT_KEY': new_sort_key,
            'created_at': str(datetime.now())
        }
        
        # Copy all attributes except PARTITION_KEY, SORT_KEY, and updated_at
        for key, value in existing_item.items():
            if key not in ['PARTITION_KEY', 'SORT_KEY', 'updated_at']:
                new_item[key] = value

        # Use a transaction to ensure atomicity
        try:
            transact_items = [
                {
                    'Delete': {
                        'TableName': table_name,
                        'Key': {
                            'PARTITION_KEY': {'S': partition_key},
                            'SORT_KEY': {'S': current_sort_key}
                        }
                    }
                },
                {
                    'Put': {
                        'TableName': table_name,
                        'Item': {k: {'S' if isinstance(v, str) else {'N': str(v)}} 
                                for k, v in new_item.items()}
                    }
                }
            ]
            
            dynamodb.meta.client.transact_write_items(TransactItems=transact_items)
            return response(200, {'message': 'Item updated successfully', 'new_item': new_item})
            
        except ClientError as e:
            print(f"Transaction error: {e}")
            return response(500, {'message': f"Error updating item: {str(e)}"})
            
    except ClientError as e:
        print("An error occurred ", e)
        return response(500, {'message': f"Error: {str(e)}"})
    except KeyError as e:
        print("Missing required field ", e)
        return response(400, {'message': f"Missing required field: {str(e)}"})
    except Exception as e:
        print("Unexpected error ", e)
        return response(500, {'message': f"Unexpected error: {str(e)}"})