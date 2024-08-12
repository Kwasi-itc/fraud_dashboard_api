import os
import json
import boto3
from boto3.dynamodb.conditions import Key
from datetime import datetime

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.environ['FRAUD_PROCESSED_TRANSACTIONS_TABLE'])

def lambda_handler(event, context):
    http_method = event['httpMethod']
    resource = event['resource']

    if http_method == 'POST' and resource == '/case':
        return create_case(event, context)
    elif http_method == 'PUT' and resource == '/case/status':
        return update_case_status(event, context)
    elif http_method == 'GET' and resource == '/case':
        return get_case(event, context)
    elif http_method == 'GET' and resource == '/cases/open':
        return get_open_cases(event, context)
    elif http_method == 'PUT' and resource == '/case/close':
        return close_case(event, context)
    else:
        return response(400, {'message': 'Invalid endpoint'})

def create_case(event, context):
    try:
        body = json.loads(event['body'])
        transaction_id = body.get('transaction_id')
        status = body.get('status')
        
        if not transaction_id:
            return response(400, {'message': 'transaction_id is required'})
        
        item = {
            'PARTITION_KEY': 'CASE',
            'SORT_KEY': transaction_id,
            'status': status,
            'created_at': datetime.now().isoformat()
        }
        
        table.put_item(Item=item)
        
        return response(200, {'message': 'Case created successfully', 'case_id': transaction_id})
    except Exception as e:
        print("An error occurred ", e)
        return response(500, {'message': str(e)})

def update_case_status(event, context):
    try:
        body = json.loads(event['body'])
        transaction_id = body.get('transaction_id')
        new_status = body.get('status')
        
        if not transaction_id or not new_status:
            return response(400, {'message': 'transaction_id and status are required'})
        
        table.update_item(
            Key={'PARTITION_KEY': 'CASE', 'SORT_KEY': transaction_id},
            UpdateExpression='SET #status = :status, updated_at = :updated_at',
            ExpressionAttributeNames={'#status': 'status'},
            ExpressionAttributeValues={':status': new_status, ':updated_at': datetime.now().isoformat()}
        )
        
        return response(200, {'message': 'Case status updated successfully'})
    except Exception as e:
        print("An error occurred ", e)
        return response(500, {'message': str(e)})

def get_case(event, context):
    try:
        params = event['queryStringParameters'] or {}
        transaction_id = params.get('transaction_id')
        
        if not transaction_id:
            return response(400, {'message': 'transaction_id is required'})
        
        result = table.get_item(Key={'PARTITION_KEY': 'CASE', 'SORT_KEY': transaction_id})
        
        if 'Item' not in result:
            return response(404, {'message': 'Case not found'})
        
        return response(200, result['Item'])
    except Exception as e:
        print("An error occurred ", e)
        return response(500, {'message': str(e)})

def get_open_cases(event, context):
    try:
        result = table.query(
            KeyConditionExpression=Key('PARTITION_KEY').eq('CASE'),
            FilterExpression=Key('status').eq('OPEN')
        )
        
        items = result.get('Items', [])
        return response(200, {'open_cases': items})
    except Exception as e:
        print("An error occurred ", e)
        return response(500, {'message': str(e)})

def close_case(event, context):
    try:
        body = json.loads(event['body'])
        transaction_id = body.get('transaction_id')
        
        if not transaction_id:
            return response(400, {'message': 'transaction_id is required'})
        
        # Get the existing case
        result = table.get_item(Key={'PARTITION_KEY': 'CASE', 'SORT_KEY': transaction_id})
        
        if 'Item' not in result:
            return response(404, {'message': 'Case not found'})
        
        case = result['Item']
        
        # Delete the existing case
        table.delete_item(Key={'PARTITION_KEY': 'CASE', 'SORT_KEY': transaction_id})
        
        # Create a new closed case
        closed_case = {
            'PARTITION_KEY': 'CLOSED_CASE',
            'SORT_KEY': transaction_id,
            'status': 'CLOSED',
            'created_at': case.get('created_at'),
            'closed_at': datetime.now().isoformat()
        }
        
        table.put_item(Item=closed_case)
        
        return response(200, {'message': 'Case closed successfully'})
    except Exception as e:
        print("An error occurred ", e)
        return response(500, {'message': str(e)})

def response(status_code, body):
    return {
        'statusCode': status_code,
        'body': json.dumps(body),
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Credentials': True,
        },
    }

