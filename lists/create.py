import json
import boto3
from botocore.exceptions import ClientError
import os
from datetime import datetime
from decimal import Decimal

dynamodb = boto3.resource('dynamodb')
table_name = os.environ["FRAUD_LISTS_TABLE"]
table = dynamodb.Table(table_name)

# Partition key for list type definitions
LIST_TYPE_DEFINITION_PK = "LIST_TYPE_DEFINITIONS"

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

def get_all_available_list_types():
    """Get all available list types (both custom and reserved)"""
    try:
        # Reserved list types that are always available
        RESERVED_LIST_TYPES = ["BLACKLIST", "WATCHLIST", "STAFFLIST"]
        available_types = set(RESERVED_LIST_TYPES)
        
        # Query for custom list type definitions
        response = table.query(
            KeyConditionExpression=boto3.dynamodb.conditions.Key('PARTITION_KEY').eq(LIST_TYPE_DEFINITION_PK)
        )
        
        items = response.get('Items', [])
        
        # Handle pagination if there are more items
        while 'LastEvaluatedKey' in response:
            response = table.query(
                KeyConditionExpression=boto3.dynamodb.conditions.Key('PARTITION_KEY').eq(LIST_TYPE_DEFINITION_PK),
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            items.extend(response.get('Items', []))
        
        # Add custom list types that are active
        for item in items:
            if item.get('is_active', True):  # Only include active list types
                available_types.add(item.get('SORT_KEY', ''))
        
        return sorted(list(available_types))
    
    except ClientError as e:
        print(f"Error getting available list types: {str(e)}")
        # Fallback to reserved types if there's an error
        return ["BLACKLIST", "WATCHLIST", "STAFFLIST"]

def validate_list_type(list_type):
    """Validate if the provided list type is available"""
    available_types = get_all_available_list_types()
    return list_type.upper() in [lt.upper() for lt in available_types], available_types

def lambda_handler(event, context):
    try:
        print("The event is ", event)
        body = json.loads(event['body'])
        print("The body of the event is ", body)
        
        list_type = body['list_type'].upper()
        channel = body['channel']
        entity_type = body['entity_type']
        account_id = body.get("account_ref")
        application_id = body.get('processor')
        merchant_id = body.get('merchant_id')
        product_id = body.get('product_id')
        description = body.get('description', '')

        # Dynamically validate list type
        is_valid, available_types = validate_list_type(list_type)
        if not is_valid:
            return response(400, {
                'message': f"Error: Invalid list_type '{list_type}'. Available types are: {', '.join(available_types)}",
                'available_list_types': available_types
            })
        
        channel = channel.lower()
        partition_key = f"{list_type}-{channel}-{entity_type}"
        sort_key = ""
        
        if entity_type == "ACCOUNT":
            if not account_id:
                return response(400, {'message': 'account_ref is required for ACCOUNT entity type'})
            sort_key = account_id
        elif entity_type == "APPLICATION" or entity_type == "PROCESSOR":
            if not application_id:
                return response(400, {'message': 'processor is required for APPLICATION/PROCESSOR entity type'})
            sort_key = application_id
        elif entity_type == "MERCHANT":
            if not application_id or not merchant_id:
                return response(400, {'message': 'processor and merchant_id are required for MERCHANT entity type'})
            sort_key = application_id + "__" + merchant_id
        elif entity_type == "PRODUCT":
            if not application_id or not merchant_id or not product_id:
                return response(400, {'message': 'processor, merchant_id, and product_id are required for PRODUCT entity type'})
            sort_key = application_id + "__" + merchant_id + "__" + product_id
        else:
            return response(400, {'message': 'entity_type must be ACCOUNT | PROCESSOR | MERCHANT | PRODUCT'})
        
        # Check if item already exists
        try:
            existing_item = table.get_item(
                Key={
                    'PARTITION_KEY': partition_key,
                    'SORT_KEY': sort_key
                }
            )
            if 'Item' in existing_item:
                return response(409, {
                    'message': 'Item already exists in the list',
                    'existing_item': {
                        'PARTITION_KEY': partition_key,
                        'entity_id': sort_key,
                        'created_at': existing_item['Item'].get('created_at')
                    }
                })
        except ClientError as e:
            if e.response['Error']['Code'] != 'ResourceNotFoundException':
                raise e
        
        created_at = str(datetime.now())
        
        # Add the item to the list
        response_db = table.put_item(
            Item={
                'PARTITION_KEY': partition_key,
                'SORT_KEY': sort_key,
                'created_at': created_at,
                'list_type': list_type,
                'channel': channel,
                'entity_type': entity_type,
                'description': description
            }
        )

        print("The response after putting 1st item in DB is ", response_db)

        #Adding additional data item
        response_db_2 = table.put_item(
            Item={
                'PARTITION_KEY': "LIST_TYPE",
                'SORT_KEY': sort_key + "###" + partition_key,
                'description': description
            }
        )

        print("The response after putting 2nd item in DB is ", response_db_2)

        return response(200, {
            'message': f'Item added to {list_type} successfully',
            'item_details': {
                'PARTITION_KEY': partition_key,
                'entity_id': sort_key,
                'created_at': created_at,
                'list_type': list_type,
                'channel': channel,
                'entity_type': entity_type
            }
        })

    except json.JSONDecodeError:
        return response(400, {'message': 'Invalid JSON in request body'})
    except ClientError as e:
        print("An error occurred ", e)
        return response(500, {'message': f"Database error: {str(e)}"})
    except Exception as e:
        print("An error occurred ", e)
        return response(500, {'message': f"Error: {str(e)}"})