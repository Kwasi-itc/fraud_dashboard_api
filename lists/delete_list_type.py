import json
import boto3
from botocore.exceptions import ClientError
import os
from decimal import Decimal

dynamodb = boto3.resource('dynamodb')
table_name = os.environ["FRAUD_LISTS_TABLE"]
table = dynamodb.Table(table_name)

# Partition key for list type definitions
LIST_TYPE_DEFINITION_PK = "LIST_TYPE_DEFINITIONS"

# Reserved list types that cannot be deleted
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

def count_list_entries(list_type):
    """Count how many entries exist for this list type"""
    try:
        response = table.scan(
            FilterExpression=boto3.dynamodb.conditions.Attr('PARTITION_KEY').begins_with(f"{list_type.upper()}-"),
            Select='COUNT'
        )
        
        count = response.get('Count', 0)
        
        # Handle pagination to get accurate count
        while 'LastEvaluatedKey' in response:
            response = table.scan(
                FilterExpression=boto3.dynamodb.conditions.Attr('PARTITION_KEY').begins_with(f"{list_type.upper()}-"),
                Select='COUNT',
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            count += response.get('Count', 0)
        
        return count
    
    except ClientError as e:
        print(f"Error counting list entries: {str(e)}")
        return 0

def delete_all_list_entries(list_type):
    """Delete all entries associated with this list type"""
    try:
        # Get all items with this list type
        response = table.scan(
            FilterExpression=boto3.dynamodb.conditions.Attr('PARTITION_KEY').begins_with(f"{list_type.upper()}-")
        )
        
        items_to_delete = response.get('Items', [])
        
        # Handle pagination
        while 'LastEvaluatedKey' in response:
            response = table.scan(
                FilterExpression=boto3.dynamodb.conditions.Attr('PARTITION_KEY').begins_with(f"{list_type.upper()}-"),
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            items_to_delete.extend(response.get('Items', []))
        
        # Delete items in batches
        deleted_count = 0
        if items_to_delete:
            with table.batch_writer() as batch:
                for item in items_to_delete:
                    batch.delete_item(
                        Key={
                            'PARTITION_KEY': item['PARTITION_KEY'],
                            'SORT_KEY': item['SORT_KEY']
                        }
                    )
                    deleted_count += 1
        
        return deleted_count
    
    except ClientError as e:
        print(f"Error deleting list entries: {str(e)}")
        raise e

def delete_list_type(list_type, force_delete=False):
    """Delete a list type definition from FRAUD_LISTS_TABLE"""
    try:
        list_type = list_type.upper()
        
        # Check if it's a reserved list type
        if list_type in RESERVED_LIST_TYPES:
            return False, f"Cannot delete reserved list type '{list_type}'"
        
        # Check if list type exists
        get_response = table.get_item(
            Key={
                'PARTITION_KEY': LIST_TYPE_DEFINITION_PK,
                'SORT_KEY': list_type
            }
        )
        
        if 'Item' not in get_response:
            return False, f"List type '{list_type}' does not exist"
        
        list_type_item = get_response['Item']
        
        # Check if there are existing entries
        entry_count = count_list_entries(list_type)
        
        if entry_count > 0 and not force_delete:
            return False, f"Cannot delete list type '{list_type}' because it has {entry_count} existing entries. Use force_delete=true to delete anyway."
        
        # Delete all entries if force_delete is true
        deleted_entries = 0
        if entry_count > 0 and force_delete:
            deleted_entries = delete_all_list_entries(list_type)
        
        # Delete the list type definition
        table.delete_item(
            Key={
                'PARTITION_KEY': LIST_TYPE_DEFINITION_PK,
                'SORT_KEY': list_type
            }
        )
        
        return True, {
            'message': f"List type '{list_type}' deleted successfully",
            'deleted_list_type': list_type,
            'deleted_entries_count': deleted_entries,
            'list_type_details': {
                'list_type': list_type_item.get('SORT_KEY'),
                'description': list_type_item.get('description'),
                'category': list_type_item.get('category'),
                'created_at': list_type_item.get('created_at'),
                'created_by': list_type_item.get('created_by'),
                'partition_key': list_type_item.get('PARTITION_KEY'),
                'sort_key': list_type_item.get('SORT_KEY')
            }
        }
    
    except ClientError as e:
        print(f"Error deleting list type: {str(e)}")
        raise e

def lambda_handler(event, context):
    try:
        print("The event is ", event)
        body = json.loads(event['body'])
        print("The body of the event is ", body)
        
        # Validate required fields
        if 'list_type' not in body:
            return response(400, {'message': 'list_type is required'})
        
        list_type = body['list_type'].upper()
        force_delete = body.get('force_delete', False)
        
        # Delete the list type
        success, result = delete_list_type(list_type, force_delete)
        
        if success:
            return response(200, result)
        else:
            # Determine appropriate status code
            if "does not exist" in result:
                return response(404, {'message': result})
            elif "reserved" in result or "existing entries" in result:
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