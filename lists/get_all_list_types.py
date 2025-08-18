import json
import boto3
from botocore.exceptions import ClientError
import os
from decimal import Decimal
import math

dynamodb = boto3.resource('dynamodb')
table_name = os.environ["FRAUD_LISTS_TABLE"]
table = dynamodb.Table(table_name)

PAGE_SIZE = 20

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
        "data": body.get('data', []),
        "metadata": body.get('metadata')
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

def format_response(items, page, per_page):
    """Format the response according to pagination structure"""
    total_records = len(items)
    total_pages = math.ceil(total_records / per_page) if total_records > 0 else 1
    
    start_idx = (page - 1) * per_page
    end_idx = start_idx + per_page
    
    return {
        'data': items[start_idx:end_idx],
        'metadata': {
            'page': page,
            'previous_page': page - 1 if page > 1 else None,
            'next_page': page + 1 if page < total_pages else None,
            'total_records': total_records,
            'pages': total_pages,
            'per_page': per_page,
            'from': start_idx + 1 if total_records > 0 else 0,
            'to': min(end_idx, total_records)
        }
    }

def get_all_list_types():
    """Get all list type definitions from FRAUD_LISTS_TABLE including reserved types"""
    try:
        # Reserved list types that should always be included
        RESERVED_LIST_TYPES = ["BLACKLIST", "WATCHLIST", "STAFFLIST"]
        
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
        
        # Transform custom list type items for response
        processed_items = []
        existing_list_types = set()
        
        for item in items:
            list_type_name = item.get('SORT_KEY', '')
            existing_list_types.add(list_type_name)
            
            processed_item = {
                'list_type': list_type_name,
                'description': item.get('description', ''),
                'is_active': item.get('is_active', True),
                'created_at': item.get('created_at', ''),
                'updated_at': item.get('updated_at', ''),
                'created_by': item.get('created_by', ''),
                'category': item.get('category', 'CUSTOM'),
                'allowed_entities': item.get('allowed_entities', ['ACCOUNT', 'PROCESSOR', 'MERCHANT', 'PRODUCT']),
                'allowed_channels': item.get('allowed_channels', ['online', 'mobile', 'pos', 'atm']),
                'metadata': item.get('metadata', {})
            }
            processed_items.append(processed_item)
        
        # Add reserved list types if they don't already exist in custom definitions
        for reserved_type in RESERVED_LIST_TYPES:
            if reserved_type not in existing_list_types:
                processed_items.append({
                    'list_type': reserved_type,
                    'description': f'System reserved {reserved_type.lower()} for fraud detection',
                    'is_active': True,
                    'created_at': 'System',
                    'updated_at': 'System',
                    'created_by': 'system',
                    'category': 'SYSTEM',
                    'allowed_entities': ['ACCOUNT', 'PROCESSOR', 'MERCHANT', 'PRODUCT'],
                    'allowed_channels': ['online', 'mobile', 'pos', 'atm'],
                    'metadata': {'reserved': True}
                })
        
        # Sort by list_type name
        processed_items.sort(key=lambda x: x['list_type'])
        
        return processed_items
    
    except ClientError as e:
        print(f"Error getting all list types: {str(e)}")
        raise e

def lambda_handler(event, context):
    try:
        print("The event is ", event)
        query_params = event.get('queryStringParameters') or {}
        print("The query params are ", query_params)

        page = int(query_params.get('page', 1))
        page_size = int(query_params.get('page_size', PAGE_SIZE))
        
        # Optional filters
        is_active = query_params.get('is_active')
        category = query_params.get('category')
        
        # Get all list types
        items = get_all_list_types()
        
        # Apply filters
        if is_active is not None:
            is_active_bool = is_active.lower() == 'true'
            items = [item for item in items if item['is_active'] == is_active_bool]
        
        if category:
            items = [item for item in items if item['category'].upper() == category.upper()]
        
        result = format_response(items, page, page_size)
        return response(200, result)

    except ClientError as e:
        print("A ClientError occurred ", e)
        return response(500, {'message': f"Database error: {str(e)}"})
    except Exception as e:
        print("An error occurred ", e)
        return response(500, {'message': f"Error: {str(e)}"})