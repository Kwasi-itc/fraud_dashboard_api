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

# Reserved list types that cannot be updated
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
    
    # Check format (letters, numbers, underscores only)
    if not list_type.replace('_', '').replace('-', '').isalnum():
        return False, "List type name can only contain letters, numbers, underscores, and hyphens"
    
    # Check length
    if len(list_type) < 3 or len(list_type) > 50:
        return False, "List type name must be between 3 and 50 characters"
    
    return True, list_type

def update_list_entries_partition_keys(old_list_type, new_list_type):
    """Update all existing entries when list type name changes"""
    try:
        # Get all items with the old list type
        response = table.scan(
            FilterExpression=boto3.dynamodb.conditions.Attr('PARTITION_KEY').begins_with(f"{old_list_type.upper()}-")
        )
        
        items_to_update = response.get('Items', [])
        
        # Handle pagination
        while 'LastEvaluatedKey' in response:
            response = table.scan(
                FilterExpression=boto3.dynamodb.conditions.Attr('PARTITION_KEY').begins_with(f"{old_list_type.upper()}-"),
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            items_to_update.extend(response.get('Items', []))
        
        updated_count = 0
        
        if items_to_update:
            # Update items in batches
            for item in items_to_update:
                old_partition_key = item['PARTITION_KEY']
                # Replace the list type part in the partition key
                new_partition_key = old_partition_key.replace(f"{old_list_type.upper()}-", f"{new_list_type.upper()}-", 1)
                
                # Create new item with updated partition key
                new_item = item.copy()
                new_item['PARTITION_KEY'] = new_partition_key
                new_item['updated_at'] = str(datetime.now())
                
                # Put new item
                table.put_item(Item=new_item)
                
                # Delete old item
                table.delete_item(
                    Key={
                        'PARTITION_KEY': old_partition_key,
                        'SORT_KEY': item['SORT_KEY']
                    }
                )
                
                updated_count += 1
        
        return updated_count
    
    except ClientError as e:
        print(f"Error updating list entries: {str(e)}")
        raise e

def update_list_type(list_type, updates):
    """Update a list type definition in FRAUD_LISTS_TABLE"""
    try:
        list_type = list_type.upper()
        
        # Check if it's a reserved list type
        if list_type in RESERVED_LIST_TYPES:
            return False, f"Cannot update reserved list type '{list_type}'"
        
        # Check if list type exists
        get_response = table.get_item(
            Key={
                'PARTITION_KEY': LIST_TYPE_DEFINITION_PK,
                'SORT_KEY': list_type
            }
        )
        
        if 'Item' not in get_response:
            return False, f"List type '{list_type}' does not exist"
        
        current_item = get_response['Item']
        updated_at = str(datetime.now())
        
        # Check if list type name is being changed
        new_list_type = updates.get('list_type', list_type).upper()
        list_type_changed = new_list_type != list_type
        
        # If list type name is changing, validate the new name
        if list_type_changed:
            # Check if new name is reserved
            if new_list_type in RESERVED_LIST_TYPES:
                return False, f"Cannot rename to reserved list type '{new_list_type}'"
            
            # Validate new name format
            is_valid, validated_name_or_error = validate_list_type_name(new_list_type)
            if not is_valid:
                return False, validated_name_or_error
            
            # Check if new name already exists
            check_response = table.get_item(
                Key={
                    'PARTITION_KEY': LIST_TYPE_DEFINITION_PK,
                    'SORT_KEY': new_list_type
                }
            )
            if 'Item' in check_response:
                return False, f"List type '{new_list_type}' already exists"
        
        # Prepare updated item
        updated_item = current_item.copy()
        updated_item['updated_at'] = updated_at
        
        # Update allowed fields
        updatable_fields = [
            'description', 'is_active', 'category', 'allowed_entities', 
            'allowed_channels', 'metadata'
        ]
        
        for field in updatable_fields:
            if field in updates:
                updated_item[field] = updates[field]
        
        # Validate allowed_entities if provided
        if 'allowed_entities' in updates:
            valid_entities = ['ACCOUNT', 'PROCESSOR', 'MERCHANT', 'PRODUCT']
            if not all(entity.upper() in valid_entities for entity in updates['allowed_entities']):
                return False, f"Invalid entity types. Allowed: {', '.join(valid_entities)}"
        
        # Handle list type name change
        updated_entries_count = 0
        if list_type_changed:
            updated_item['SORT_KEY'] = new_list_type
            
            # Update all existing entries in the main table
            updated_entries_count = update_list_entries_partition_keys(list_type, new_list_type)
            
            # Delete old list type definition
            table.delete_item(
                Key={
                    'PARTITION_KEY': LIST_TYPE_DEFINITION_PK,
                    'SORT_KEY': list_type
                }
            )
            
            # Create new list type definition
            table.put_item(Item=updated_item)
        else:
            # Just update the existing item
            table.put_item(Item=updated_item)
        
        return True, {
            'message': f"List type updated successfully",
            'original_list_type': list_type,
            'updated_list_type': updated_item['SORT_KEY'],
            'list_type_changed': list_type_changed,
            'updated_entries_count': updated_entries_count,
            'updated_details': {
                'list_type': updated_item['SORT_KEY'],
                'description': updated_item.get('description'),
                'is_active': updated_item.get('is_active'),
                'category': updated_item.get('category'),
                'allowed_entities': updated_item.get('allowed_entities'),
                'allowed_channels': updated_item.get('allowed_channels'),
                'created_at': updated_item.get('created_at'),
                'updated_at': updated_item.get('updated_at'),
                'created_by': updated_item.get('created_by'),
                'partition_key': updated_item.get('PARTITION_KEY'),
                'sort_key': updated_item.get('SORT_KEY')
            }
        }
    
    except ClientError as e:
        print(f"Error updating list type: {str(e)}")
        raise e

def lambda_handler(event, context):
    try:
        print("The event is ", event)
        body = json.loads(event['body'])
        print("The body of the event is ", body)
        
        # Validate required fields
        if 'list_type' not in body:
            return response(400, {'message': 'list_type is required'})
        
        if 'updates' not in body:
            return response(400, {'message': 'updates section is required'})
        
        list_type = body['list_type'].upper()
        updates = body['updates']
        
        # Validate that there's at least one field to update
        updatable_fields = [
            'list_type', 'description', 'is_active', 'category', 
            'allowed_entities', 'allowed_channels', 'metadata'
        ]
        
        if not any(field in updates for field in updatable_fields):
            return response(400, {'message': f'At least one updatable field is required: {", ".join(updatable_fields)}'})
        
        # Update the list type
        success, result = update_list_type(list_type, updates)
        
        if success:
            return response(200, result)
        else:
            # Determine appropriate status code
            if "does not exist" in result:
                return response(404, {'message': result})
            elif "reserved" in result or "already exists" in result:
                return response(409, {'message': result})  # Conflict
            else:
                return response(400, {'message': result})

    except json.JSONDecodeError:
        return response(400, {'message': 'Invalid JSON in request body'})
    except ClientError as e:
        print("A ClientError occurred ", e)
        return response(500, {'message': f"Database error: {str(e)}"})
    except Exception as e:
        print("An error occurred ", e)
        return response(500, {'message': f"Error: {str(e)}"})