import os
import json
import boto3
from boto3.dynamodb.conditions import Key
from datetime import datetime
from decimal import Decimal
import uuid
import logging
import math


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
    else:
        return response(400, {'message': 'Invalid endpoint'})

def create_case(event, context):
    try:
        body = json.loads(event['body'])
        print("The body is ", body)
        transaction_id = body.get('transaction_id')
        assigned_to = body.get('assigned_to')
        status = body.get('status')
        #status = body.get('status')
        
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


# --------------------------------------------------------------------------- #
# Pagination helpers – identical to evaluated_transactions implementation
# --------------------------------------------------------------------------- #
def create_pagination_token(last_evaluated_key, current_page):
    if not last_evaluated_key:
        return None
    return json.dumps({"lek": last_evaluated_key, "page": current_page + 1})


def parse_pagination_token(token):
    if not token:
        return None, None
    try:
        payload = json.loads(token)
        return payload.get("lek"), {"page": payload.get("page", 2)}
    except json.JSONDecodeError:
        return None, None


def format_paginated_response(
    items, current_page, per_page, next_pagination_token=None, total_records=None
):
    total_pages = (
        math.ceil(total_records / per_page) if total_records is not None else None
    )

    from_record = ((current_page - 1) * per_page) + 1 if items else 0
    to_record = from_record + len(items) - 1 if items else 0

    next_page = current_page + 1 if next_pagination_token else None

    return {
        "data": items,
        "metadata": {
            "page": current_page,
            "previous_page": current_page - 1 if current_page > 1 else None,
            "next_page": next_page,
            "total_records": total_records,
            "pages": total_pages,
            "per_page": per_page,
            "from": from_record,
            "to": to_record,
            "pagination_token": next_pagination_token,
        },
    }

def get_all_case_reports(event, context):
    """
    Paginated retrieval of case reports.

    Query params:
      - transaction_id   (optional)
      - page             (optional, default 1) – only for initial call
      - per_page         (optional, default 20)
      - pagination_token (optional) – opaque token returned by a previous call
    """
    try:
        query_params = event.get("queryStringParameters", {}) or {}
        transaction_id = query_params.get("transaction_id")
        per_page = int(query_params.get("per_page", 20))
        pagination_token = query_params.get("pagination_token")

        exclusive_start_key = None
        current_page = int(query_params.get("page", 1))
        if pagination_token:
            try:
                token_payload = json.loads(pagination_token)
                exclusive_start_key = token_payload.get("lek")
                current_page = token_payload.get("page", current_page)
            except json.JSONDecodeError:
                return response(400, {"message": "Invalid pagination_token format"})

        key_condition = Key("PARTITION_KEY").eq("CASE_REPORT")
        if transaction_id:
            key_condition = key_condition & Key("SORT_KEY").begins_with(f"{transaction_id}#")

        dynamo_query_params = {
            "KeyConditionExpression": key_condition,
            "Limit": per_page,
            "ScanIndexForward": False,
        }
        if exclusive_start_key:
            dynamo_query_params["ExclusiveStartKey"] = exclusive_start_key

        result = table.query(**dynamo_query_params)
        reports = [remove_partition_key(item) for item in result.get("Items", [])]

        response_body = {
            "reports": reports,
            "page": current_page,
            "per_page": per_page,
            "count": len(reports),
        }

        if "LastEvaluatedKey" in result:
            next_token = {
                "lek": result["LastEvaluatedKey"],
                "page": current_page + 1,
            }
            response_body["pagination_token"] = json.dumps(next_token)
            response_body["has_more"] = True
        else:
            response_body["has_more"] = False

        return response(200, response_body)

    except Exception as e:
        logger.error("get_all_case_reports error: %s", e, exc_info=True)
        return response(500, {"message": str(e)})


def update_case_status(event, context):
    """
    Validate and update the status / assignee of an existing case item.
    Enforces allowable status transitions defined in `STATUS_TRANSITIONS`.
    """
    try:
        body = json.loads(event['body'])
        print("The body is ", body)
        transaction_id = body.get('transaction_id')
        new_status = body.get('status')
        
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
            Key={'PARTITION_KEY': 'CASE', 'SORT_KEY': transaction_id},
            UpdateExpression='SET #status = :status, updated_at = :updated_at',
            ExpressionAttributeNames={'#status': 'status'},
            ExpressionAttributeValues={':status': new_status, ':updated_at': datetime.now().isoformat()}
        )
        
        return response(200, {'message': 'Case status updated successfully'})
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

        # Switch to evaluated-transactions style pagination
        per_page = int(query_params.get("per_page", 20))
        pagination_token = query_params.get("pagination_token")

        exclusive_start_key, token_meta = parse_pagination_token(pagination_token)
        current_page = int(query_params.get("page", 1))
        if token_meta:
            current_page = token_meta.get("page", current_page)

        # Build key condition
        key_condition = Key("PARTITION_KEY").eq("CASE")
        if transaction_id:
            key_condition = key_condition & Key("SORT_KEY").eq(transaction_id)

        dynamo_query_params = {
            "KeyConditionExpression": key_condition,
            "Limit": per_page,
            "ScanIndexForward": False,
        }

        if exclusive_start_key:
            dynamo_query_params["ExclusiveStartKey"] = exclusive_start_key

        result = table.query(**dynamo_query_params)

        items = result.get("Items", [])
        filtered_items: list[dict] = []

        # Post-query filtering
        for item in items:
            if "report" in item:
                continue
            if not item.get("assigned_to"):
                continue
            if status and item.get("status") != status:
                continue
            filtered_items.append(item)

        last_evaluated_key = result.get("LastEvaluatedKey")
        next_token = (
            create_pagination_token(last_evaluated_key, current_page)
            if last_evaluated_key
            else None
        )

        return response(
            200,
            format_paginated_response(
                filtered_items, current_page, per_page, next_token
            ),
        )
    except Exception as e:
        logger.error("get_open_cases error: %s", e, exc_info=True)
        return response(500, {"message": str(e)})

def get_closed_cases(event, context):
    """
    Paginated retrieval of closed cases.

    Query params:
      - transaction_id      (optional)
      - status              (optional)
      - limit               (optional, default 100)
      - last_evaluated_key  (optional) JSON string from previous call
    """
    try:
        query_params = event.get("queryStringParameters", {}) or {}
        transaction_id = query_params.get("transaction_id")
        status = query_params.get("status")
        limit = int(query_params.get("limit", 100))
        last_evaluated_key_param = query_params.get("last_evaluated_key")

        key_condition = Key("PARTITION_KEY").eq("CLOSED_CASE")
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
        filtered_items: list[dict] = []
        for item in items:
            if not item.get("assigned_to"):
                continue
            if status and item.get("status") != status:
                continue
            filtered_items.append(item)

        response_body = {
            "closed_cases": filtered_items,
            "count": len(filtered_items),
            "total_count": result.get("Count", 0),
        }

        if "LastEvaluatedKey" in result:
            response_body["last_evaluated_key"] = json.dumps(
                result["LastEvaluatedKey"]
            )
            response_body["has_more"] = True
        else:
            response_body["has_more"] = False

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
            'assigned_to': case.get('assigned_to'),
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
        "data": body.get("data", []),
        "metadata": body.get("metadata"),
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
