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

# Reserved list types that cannot be created manually
RESERVED_LIST_TYPES = ["BLACKLIST", "WATCHLIST", "STAFFLIST"]

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

def validate_list_type_name(list_type):
    """Validate list type name format"""
    if not list_type:
        return False, "List type name cannot be empty"
    
    # Convert to uppercase for consistency
    list_type = list_type.upper()
    
    # Check if it's a reserved name
    if list_type in RESERVED_LIST_TYPES:
        return False, f"'{list_type}' is a reserved list type and cannot be created manually"
    
    # Check format (letters, numbers, underscores only)
    if not list_type.replace('_', '').replace('-', '').isalnum():
        return False, "List type name can only contain letters, numbers, underscores, and hyphens"
    
    # Check length
    if len(list_type) < 3 or len(list_type) > 50:
        return False, "List type name must be between 3 and 50 characters"
    
    return True, list_type

def create_list_type(list_type_data):
    """Create a new list type definition in FRAUD_LISTS_TABLE"""
    try:
        list_type = list_type_data['list_type'].upper()
        
        # Check if list type already exists
        try:
            existing_response = table.get_item(
                Key={
                    'PARTITION_KEY': LIST_TYPE_DEFINITION_PK,
                    'SORT_KEY': list_type
                }
            )
            if 'Item' in existing_response:
                return False, f"List type '{list_type}' already exists"
        except ClientError as e:
            if e.response['Error']['Code'] != 'ResourceNotFoundException':
                raise e
        
        # Prepare the item
        created_at = str(datetime.now())
        item = {
            'PARTITION_KEY': LIST_TYPE_DEFINITION_PK,
            'SORT_KEY': list_type,  # The list type name goes in SORT_KEY
            'description': list_type_data.get('description', ''),
            'is_active': list_type_data.get('is_active', True),
            'created_at': created_at,
            'updated_at': created_at,
            'created_by': list_type_data.get('created_by', 'system'),
            'category': list_type_data.get('category', 'CUSTOM'),
            'allowed_entities': list_type_data.get('allowed_entities', ['ACCOUNT', 'PROCESSOR', 'MERCHANT', 'PRODUCT']),
            'allowed_channels': list_type_data.get('allowed_channels', ['online', 'mobile', 'pos', 'atm']),
            'metadata': list_type_data.get('metadata', {})
        }
        
        # Validate allowed_entities
        valid_entities = ['ACCOUNT', 'PROCESSOR', 'MERCHANT', 'PRODUCT']
        if not all(entity.upper() in valid_entities for entity in item['allowed_entities']):
            return False, f"Invalid entity types. Allowed: {', '.join(valid_entities)}"
        
        # Create the list type
        table.put_item(Item=item)
        
        return True, item
    
    except ClientError as e:
        print(f"Error creating list type: {str(e)}")
        raise e

def lambda_handler(event, context):
    try:
        print("The event is ", event)
        body = json.loads(event['body'])
        print("The body of the event is ", body)
        
        # Validate required fields
        if 'list_type' not in body:
            return response(400, {'message': 'list_type is required'})
        
        # Validate list type name
        is_valid, validated_name_or_error = validate_list_type_name(body['list_type'])
        if not is_valid:
            return response(400, {'message': validated_name_or_error})
        
        # Update the body with validated name
        body['list_type'] = validated_name_or_error
        
        # Create the list type
        success, result = create_list_type(body)
        
        if success:
            return response(200, {
                'message': f"List type '{result['SORT_KEY']}' created successfully",
                'list_type_details': {
                    'list_type': result['SORT_KEY'],
                    'description': result['description'],
                    'is_active': result['is_active'],
                    'category': result['category'],
                    'allowed_entities': result['allowed_entities'],
                    'allowed_channels': result['allowed_channels'],
                    'created_at': result['created_at'],
                    'created_by': result['created_by'],
                    'partition_key': result['PARTITION_KEY'],
                    'sort_key': result['SORT_KEY']
                }
            })
        else:
            return response(409, {'message': result})  # Conflict

    except json.JSONDecodeError:
        return response(400, {'message': 'Invalid JSON in request body'})
    except ClientError as e:
        print("A ClientError occurred ", e)
        return response(500, {'message': f"Database error: {str(e)}"})
    except Exception as e:
        print("An error occurred ", e)
        return response(500, {'message': f"Error: {str(e)}"})