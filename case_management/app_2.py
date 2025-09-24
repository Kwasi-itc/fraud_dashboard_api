import os
import json
import boto3
from boto3.dynamodb.conditions import Key
from datetime import datetime
from decimal import Decimal
import uuid


dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.environ['FRAUD_PROCESSED_TRANSACTIONS_TABLE'])

def lambda_handler(event, context):
    http_method = event['httpMethod']
    resource = event['resource']
    print("The event is ", event)

    # Case and Report Management
    if http_method == 'POST' and resource == '/case':
        return create_case(event, context)
    if http_method == 'POST' and resource == '/report':
        return create_report(event, context)
    elif http_method == 'PUT' and resource == '/case/status':
        return update_case_status(event, context)
    elif http_method == 'GET' and resource == '/case':
        return get_case(event, context)
    elif http_method == 'GET' and resource == '/cases/open':
        return get_open_cases(event, context)
    elif http_method == 'GET' and resource == '/cases/closed':
        return get_closed_cases(event, context)
    elif http_method == 'GET' and resource == '/reports':
        return get_all_case_reports(event, context)
    elif http_method == 'PUT' and resource == '/case/close':
        return close_case(event, context)
    elif http_method == 'PUT' and resource == '/report':
        return edit_report(event, context)
    elif http_method == 'DELETE' and resource == '/report':
        return delete_report(event, context)
        
    # Investigator Management
    elif http_method == 'POST' and resource == '/investigator':
        return create_investigator(event, context)
    elif http_method == 'GET' and resource == '/investigator':
        return get_investigator(event, context)
    elif http_method == 'GET' and resource == '/investigators':
        return get_all_investigators(event, context)
    elif http_method == 'PUT' and resource == '/investigator':
        return update_investigator(event, context)
    elif http_method == 'DELETE' and resource == '/investigator':
        return delete_investigator(event, context)
    else:
        return response(400, {'message': 'Invalid endpoint'})

def create_case(event, context):
    try:
        body = json.loads(event['body'])
        print("The body is ", body)
        transaction_id = body.get('transaction_id')
        assigned_to = body.get('assigned_to')
        status = body.get('status')
        
        if not transaction_id:
            return response(400, {'message': 'transaction_id is required'})
        
        item = {
            'PARTITION_KEY': 'CASE',
            'SORT_KEY': transaction_id,
            'status': status,
            'assigned_to': assigned_to,
            'created_at': datetime.now().isoformat()
        }
        
        table.put_item(Item=item)
        
        return response(200, {'message': 'Case created successfully', 'case_id': transaction_id})
    except Exception as e:
        print("An error occurred ", e)
        return response(500, {'message': str(e)})

def create_report(event, context):
    try:
        body = json.loads(event['body'])
        print("The body is ", body)
        transaction_id = body.get('transaction_id')
        report = body.get('report', "")
        title = body.get('title', "")
        assigned_by = body.get('assigned_by', {})
        extra_sort_key = str(uuid.uuid4())
        
        if not transaction_id:
            return response(400, {'message': 'transaction_id is required'})
        
        item = {
            'PARTITION_KEY': 'CASE_REPORT',
            'SORT_KEY': transaction_id + '#' + extra_sort_key,
            'report': report,
            'created_at': datetime.now().isoformat(),
            'title': title,
            'assigned_by': assigned_by
        }
        
        table.put_item(Item=item)
        
        return response(200, {'message': 'Report created successfully', 'report_id': transaction_id + '#' + extra_sort_key})
    except Exception as e:
        print("An error occurred ", e)
        return response(500, {'message': str(e)})

def edit_report(event, context):
    try:
        body = json.loads(event['body'])
        print("The body is ", body)
        
        sort_key = body.get('SORT_KEY')
        title = body.get('title')
        report_content = body.get('report')
        
        # Validate required field
        if not sort_key:
            return response(400, {'message': 'sort_key is required'})
            
        # Validate that at least one field to update is provided
        if not title and not report_content:
            return response(400, {'message': 'At least one field (title or report) must be provided'})
            
        # Build update expression dynamically based on provided fields
        update_expression = 'SET updated_at = :updated_at'
        expression_values = {
            ':updated_at': datetime.now().isoformat()
        }
        
        if title is not None:
            update_expression += ', title = :title'
            expression_values[':title'] = title
            
        if report_content is not None:
            update_expression += ', report = :report'
            expression_values[':report'] = report_content
            
        # Update the item
        result = table.update_item(
            Key={
                'PARTITION_KEY': 'CASE_REPORT',
                'SORT_KEY': sort_key
            },
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_values,
            ReturnValues='ALL_NEW'  # Returns the item's new state
        )
        
        # Return the updated item
        updated_item = remove_partition_key(result['Attributes'])
        return response(200, {'message': 'Report updated successfully', 'report': updated_item})
        
    except Exception as e:
        print("An error occurred ", e)
        return response(500, {'message': str(e)})

def delete_report(event, context):
    try:
        params = event.get('queryStringParameters', {}) or {}
        sort_key = params.get('report_id')
        
        if not sort_key:
            return response(400, {'message': 'report_id is required'})
            
        # First verify the report exists
        result = table.get_item(
            Key={
                'PARTITION_KEY': 'CASE_REPORT',
                'SORT_KEY': sort_key
            }
        )
        
        if 'Item' not in result:
            return response(404, {'message': 'Report not found'})
            
        # Delete the report
        table.delete_item(
            Key={
                'PARTITION_KEY': 'CASE_REPORT',
                'SORT_KEY': sort_key
            }
        )
        
        return response(200, {'message': 'Report deleted successfully'})
        
    except Exception as e:
        print("An error occurred ", e)
        return response(500, {'message': str(e)})

def remove_partition_key(item):
    """Remove PARTITION_KEY from an item"""
    return {k: v for k, v in item.items() if k != 'PARTITION_KEY'}

def get_all_case_reports(event, context):
    try:
        # Optional query parameters
        query_params = event.get('queryStringParameters', {}) or {}
        limit = int(query_params.get('limit', 100))  # Default limit of 100 items
        last_evaluated_key = query_params.get('last_evaluated_key')
        transaction_id = query_params.get('transaction_id')  # Optional filter by transaction_id
        
        # Base query parameters with PARTITION_KEY condition
        key_condition = Key('PARTITION_KEY').eq('CASE_REPORT')
        
        # If transaction_id is provided, add begins_with condition for SORT_KEY
        if transaction_id:
            key_condition = key_condition & Key('SORT_KEY').begins_with(f"{transaction_id}#")
        
        query_params = {
            'KeyConditionExpression': key_condition,
            'Limit': limit,
            'ScanIndexForward': False  # Sort in descending order (newest first)
        }
        
        # Add pagination if last_evaluated_key is provided
        if last_evaluated_key:
            try:
                query_params['ExclusiveStartKey'] = json.loads(last_evaluated_key)
            except json.JSONDecodeError:
                return response(400, {'message': 'Invalid last_evaluated_key format'})
        
        # Execute the query
        result = table.query(**query_params)
        
        # Prepare the response
        reports = [remove_partition_key(item) for item in result.get('Items', [])]

        
        response_body = {
            'reports': reports,
            'count': len(reports),
            'total_count': result.get('Count', 0)
        }
        
        # Add pagination token if there are more items
        if 'LastEvaluatedKey' in result:
            response_body['last_evaluated_key'] = json.dumps(result['LastEvaluatedKey'])
            response_body['has_more'] = True
        else:
            response_body['has_more'] = False
            
        return response(200, reports)
        
    except Exception as e:
        print("An error occurred:", str(e))
        return response(500, {'message': 'Internal server error'})


def update_case_status(event, context):
    try:
        body = json.loads(event['body'])
        print("The body is ", body)
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
        print("The params are ", params)
        transaction_id = params.get('transaction_id')
        
        if not transaction_id:
            return response(400, {'message': 'transaction_id is required'})
        
        result = table.get_item(Key={'PARTITION_KEY': 'CASE', 'SORT_KEY': transaction_id})
        
        if 'Item' not in result:
            return response(404, {'message': 'Case not found'})
        
        if 'report' in result['Item']:
            return response(404, {'message': 'Case not found'})
        
        return response(200, result['Item'])
    except Exception as e:
        print("An error occurred ", e)
        return response(500, {'message': str(e)})

def get_open_cases(event, context):
    """
    Paginated retrieval of open cases.

    Query params accepted:
      - transaction_id  (optional)
      - status          (optional)
      - limit           (optional, default 100)
      - last_evaluated_key (optional) – JSON string returned from a previous call
    """
    try:
        query_params = event.get("queryStringParameters", {}) or {}
        transaction_id = query_params.get("transaction_id")
        status = query_params.get("status")
        limit = int(query_params.get("limit", 100))
        last_evaluated_key_param = query_params.get("last_evaluated_key")

        # Build key condition
        key_condition = Key("PARTITION_KEY").eq("CASE")
        if transaction_id:
            key_condition = key_condition & Key("SORT_KEY").eq(transaction_id)

        dynamo_query_params = {
            "KeyConditionExpression": key_condition,
            "Limit": limit,
            "ScanIndexForward": False,
        }

        if last_evaluated_key_param:
            try:
                dynamo_query_params["ExclusiveStartKey"] = json.loads(
                    last_evaluated_key_param
                )
            except json.JSONDecodeError:
                return response(400, {"message": "Invalid last_evaluated_key format"})

        result = table.query(**dynamo_query_params)

        items = result.get("Items", [])
        filtered_items = []

        # Post-query filtering
        for item in items:
            if "report" in item:
                continue
            if not item.get("assigned_to"):
                continue
            if status and item.get("status") != status:
                continue
            filtered_items.append(item)

        response_body = {
            "open_cases": filtered_items,
            "count": len(filtered_items),
            "total_count": result.get("Count", 0),
        }

        if "LastEvaluatedKey" in result:
            response_body["last_evaluated_key"] = json.dumps(result["LastEvaluatedKey"])
            response_body["has_more"] = True
        else:
            response_body["has_more"] = False

        return response(200, response_body)
    except Exception as e:
        print("An error occurred ", e)
        return response(500, {'message': str(e)})

def get_closed_cases(event, context):
    try:
        # Get query parameters
        query_params = event.get('queryStringParameters', {}) or {}
        transaction_id = query_params.get('transaction_id')
        status = query_params.get('status')
        
        # Base query condition
        key_condition = Key('PARTITION_KEY').eq('CLOSED_CASE')
        
        # If transaction_id is provided, add it to the query
        if transaction_id:
            key_condition = key_condition & Key('SORT_KEY').eq(transaction_id)
            
        # Execute the query
        result = table.query(
            KeyConditionExpression=key_condition
        )
        
        items = result.get('Items', [])
        print("The items initially retrieved are ", items)
        filtered_items = []
        
        for item in items:
            # Only include items with assigned_to
            if not item.get('assigned_to'):
                continue
                
            # Filter by status if provided
            if status and item.get('status') != status:
                continue
                
            filtered_items.append(item)
            
        return response(200, {'closed_cases': filtered_items})
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
            'status': case.get('status'),#'CLOSED',
            'assigned_to': case.get('assigned_to'),
            'created_at': case.get('created_at'),
            'closed_at': datetime.now().isoformat()
        }
        
        table.put_item(Item=closed_case)
        
        return response(200, {'message': 'Case closed successfully'})
    except Exception as e:
        print("An error occurred ", e)
        return response(500, {'message': str(e)})

def create_investigator(event, context):
    try:
        body = json.loads(event['body'])
        investigator_name = body.get('name')
        
        if not investigator_name:
            return response(400, {'message': 'Investigator name is required'})
            
        investigator_id = str(uuid.uuid4())
        
        item = {
            'PARTITION_KEY': 'INVESTIGATOR',
            'SORT_KEY': investigator_id,
            'name': investigator_name,
            'created_at': datetime.now().isoformat()
        }
        
        table.put_item(Item=item)
        
        return response(200, {'message': 'Investigator created successfully', 'investigator_id': investigator_id})
    except Exception as e:
        print("An error occurred: ", e)
        return response(500, {'message': str(e)})

def get_investigator(event, context):
    try:
        params = event.get('queryStringParameters', {}) or {}
        investigator_id = params.get('investigator_id')
        
        if not investigator_id:
            return response(400, {'message': 'investigator_id is required'})
            
        result = table.get_item(Key={'PARTITION_KEY': 'INVESTIGATOR', 'SORT_KEY': investigator_id})
        
        if 'Item' not in result:
            return response(404, {'message': 'Investigator not found'})
            
        return response(200, result['Item'])
    except Exception as e:
        print("An error occurred: ", e)
        return response(500, {'message': str(e)})

def get_all_investigators(event, context):
    try:
        result = table.query(
            KeyConditionExpression=Key('PARTITION_KEY').eq('INVESTIGATOR')
        )
        
        items = result.get('Items', [])
        
        return response(200, {'investigators': items})
    except Exception as e:
        print("An error occurred: ", e)
        return response(500, {'message': str(e)})

def update_investigator(event, context):
    try:
        body = json.loads(event['body'])
        investigator_id = body.get('investigator_id')
        investigator_name = body.get('name')
        
        if not investigator_id or not investigator_name:
            return response(400, {'message': 'investigator_id and name are required'})
            
        table.update_item(
            Key={'PARTITION_KEY': 'INVESTIGATOR', 'SORT_KEY': investigator_id},
            UpdateExpression='SET #name = :name, updated_at = :updated_at',
            ExpressionAttributeNames={'#name': 'name'},
            ExpressionAttributeValues={':name': investigator_name, ':updated_at': datetime.now().isoformat()}
        )
        
        return response(200, {'message': 'Investigator updated successfully'})
    except Exception as e:
        print("An error occurred: ", e)
        return response(500, {'message': str(e)})

def delete_investigator(event, context):
    try:
        params = event.get('queryStringParameters', {}) or {}
        investigator_id = params.get('investigator_id')
        
        if not investigator_id:
            return response(400, {'message': 'investigator_id is required'})
            
        table.delete_item(Key={'PARTITION_KEY': 'INVESTIGATOR', 'SORT_KEY': investigator_id})
        
        return response(200, {'message': 'Investigator deleted successfully'})
    except Exception as e:
        print("An error occurred: ", e)
        return response(500, {'message': str(e)})

def response(status_code, body):
    response_message = ""
    if status_code == 200:
        response_message = "Operation Successful"
    else:
        response_message = "Unsuccessful operation"
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


def decimal_default(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError
