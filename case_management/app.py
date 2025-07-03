import os
import json
import boto3
from boto3.dynamodb.conditions import Key
from datetime import datetime
from decimal import Decimal
import uuid
import logging


dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.environ['FRAUD_PROCESSED_TRANSACTIONS_TABLE'])

# -------- structured logging -------- #
logger = logging.getLogger()
logger.setLevel(logging.INFO)

ALLOWED_STATUSES = {"OPEN", "IN_PROGRESS", "CLOSED"}
# Next-status rules
STATUS_TRANSITIONS = {
    "OPEN": {"IN_PROGRESS", "CLOSED"},
    "IN_PROGRESS": {"CLOSED"},
}

def lambda_handler(event, context):
    http_method = event['httpMethod']
    resource = event['resource']
    print("The event is ", event)

    if http_method == 'POST' and resource == '/case':
        return create_case(event, context)
    elif http_method == 'PUT' and resource == '/case/status':
        return update_case_status(event, context)
    elif http_method == 'GET' and resource == '/case':
        return get_case(event, context)
    elif http_method == 'GET' and resource == '/cases/open':
        return get_open_cases(event, context)
    elif http_method == 'GET' and resource == '/cases/closed':
        return get_closed_cases(event, context)
    elif http_method == 'PUT' and resource == '/case/close':
        return close_case(event, context)
    elif http_method == 'POST' and resource == '/report':
        return create_report(event, context)
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
        extra_sort_key = str(uuid.uuid4())
        
        if not transaction_id:
            return response(400, {'message': 'transaction_id is required'})
        
        item = {
            'PARTITION_KEY': 'CASE',
            'SORT_KEY': transaction_id + '#' + extra_sort_key,
            'created_at': datetime.now().isoformat()
        }
        
        table.put_item(Item=item)
        
        return response(200, {'message': 'Report created successfully', 'report_id': transaction_id + '#' + extra_sort_key})
    except Exception as e:
        print("An error occurred ", e)
        return response(500, {'message': str(e)})

def update_case_status(event, context):
    """
    Validate and update the status / assignee of an existing case item.
    Enforces allowable status transitions defined in `STATUS_TRANSITIONS`.
    """
    try:
        body = json.loads(event["body"])
        logger.info("update_case_status body: %s", body)

        transaction_id = body.get("transaction_id")
        new_assigned_to = body.get("assigned_to")
        new_status = body.get("status")

        if not transaction_id or not new_status:
            return response(400, {"message": "transaction_id and status are required"})

        if new_status not in ALLOWED_STATUSES:
            return response(400, {"message": f"Invalid status '{new_status}'"})

        # Fetch current item to validate transition
        result = table.get_item(
            Key={"PARTITION_KEY": "CASE", "SORT_KEY": transaction_id}
        )
        if "Item" not in result:
            return response(404, {"message": "Case not found"})

        current_status = result["Item"].get("status", "OPEN")
        if (
            current_status in STATUS_TRANSITIONS
            and new_status not in STATUS_TRANSITIONS[current_status]
        ):
            return response(
                400,
                {
                    "message": f"Illegal transition: {current_status} -> {new_status}"
                },
            )

        table.update_item(
            Key={"PARTITION_KEY": "CASE", "SORT_KEY": transaction_id},
            UpdateExpression="SET #status = :status, updated_at = :updated_at, assigned_to = :assigned_to",
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={
                ":status": new_status,
                ":updated_at": datetime.now().isoformat(),
                ":assigned_to": new_assigned_to,
            },
        )

        return response(200, {"message": "Case status updated successfully"})
    except Exception as e:
        logger.error("update_case_status error: %s", e, exc_info=True)
        return response(500, {"message": str(e)})

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
        else:
            result["Item"]["transaction_id"] = result["Item"]["SORT_KEY"]
            result["Item"].pop("PARTITION_KEY", None)
            result["Item"].pop("SORT_KEY", None)
        
        return response(200, result['Item'])
    except Exception as e:
        print("An error occurred ", e)
        return response(500, {'message': str(e)})

def get_open_cases(event, context):
    """
    Return open cases with optional pagination.

    Query-string params:
        limit (int)                   – max items (default 25)
        last_evaluated_key (json str) – pass DynamoDB key from previous page
    """
    try:
        params = event.get("queryStringParameters") or {}
        limit = int(params.get("limit", 25))

        lek_param = params.get("last_evaluated_key")
        exclusive_start_key = json.loads(lek_param) if lek_param else None

        query_kwargs = {
            "KeyConditionExpression": Key("PARTITION_KEY").eq("CASE"),
            "Limit": limit,
        }
        if exclusive_start_key:
            query_kwargs["ExclusiveStartKey"] = exclusive_start_key

        result = table.query(**query_kwargs)

        items = []
        for item in result.get("Items", []):
            item["transaction_id"] = item.pop("SORT_KEY")
            item.pop("PARTITION_KEY", None)
            items.append(item)

        response_body = {
            "open_cases": items,
            "last_evaluated_key": result.get("LastEvaluatedKey"),
        }
        return response(200, response_body)
    except Exception as e:
        logger.error("get_open_cases error: %s", e, exc_info=True)
        return response(500, {"message": str(e)})

def get_closed_cases(event, context):
    """
    Return closed cases with optional pagination.
    Same query-string params as `get_open_cases`.
    """
    try:
        params = event.get("queryStringParameters") or {}
        limit = int(params.get("limit", 25))
        lek_param = params.get("last_evaluated_key")
        exclusive_start_key = json.loads(lek_param) if lek_param else None

        query_kwargs = {
            "KeyConditionExpression": Key("PARTITION_KEY").eq("CLOSED_CASE"),
            "Limit": limit,
        }
        if exclusive_start_key:
            query_kwargs["ExclusiveStartKey"] = exclusive_start_key

        result = table.query(**query_kwargs)

        items = result.get("Items", [])
        response_body = {
            "closed_cases": items,
            "last_evaluated_key": result.get("LastEvaluatedKey"),
        }
        return response(200, response_body)
    except Exception as e:
        logger.error("get_closed_cases error: %s", e, exc_info=True)
        return response(500, {"message": str(e)})

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
            'created_at': case.get('created_at'),
            'closed_at': datetime.now().isoformat()
        }
        
        table.put_item(Item=closed_case)
        
        return response(200, {'message': 'Case closed successfully'})
    except Exception as e:
        print("An error occurred ", e)
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


##Assigned investigator coming with the transaction
##Affected transactions
##Pagination
