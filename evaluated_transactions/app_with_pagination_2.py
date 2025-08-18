import os
import json
import boto3
from boto3.dynamodb.conditions import Key, Attr
from datetime import datetime, timedelta
import re
import math
import base64

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.environ['FRAUD_PROCESSED_TRANSACTIONS_TABLE'])

PAGE_SIZE = 20  # Default page size

def parse_key(key, account_id, application_id, merchant_id, product_id):
    parts = key.split('-')
    channel = parts[1]
    entities = parts[2].split('_')
    time_info = parts[-1].split('-')
    
    year = ""
    period = ""
    if "MONTH" in key:
        period = "MONTH"
        year = key.split("MONTH")[1].split("-")[1]
    elif "WEEK" in key:
        period = "WEEK"
        year = key.split("WEEK")[1].split("-")[1]
    elif "DAY" in key:
        period = "DAY"
        year = key.split("DAY")[1].split("-")[1]
    elif "HOUR" in key:
        period = "HOUR"
        year = key.split("HOUR")[1].split("-")[1]
        
    result = {
        "channel": channel,
        'account_ref': account_id,
        "processor": application_id,
        "merchant_id": merchant_id,
        "product_id": product_id,
        "period": period,
        "year": year,
        "month": "",
        "week": "",
        "day": "",
        "hour": ""
    }
    
    if result["period"] == "MONTH":
        result["month"] = key.split("MONTH")[1].split("-")[2]
    elif result["period"] == "WEEK":
        result["week"] = key.split("WEEK")[1].split("-")[2]
    elif result["period"] == "DAY":
        result["month"] = key.split("DAY")[1].split("-")[2]
        result["day"] = key.split("DAY")[1].split("-")[3]
    elif result["period"] == "HOUR":
        result["month"] = key.split("HOUR")[1].split("-")[2]
        result["day"] = key.split("HOUR")[1].split("-")[3]
        result["hour"] = key.split("HOUR")[1].split("-")[4]
    return result

def transform_aggregates(relevant_aggregates, account_id, application_id, merchant_id, product_id):
    result = {}
    for key, value in relevant_aggregates.items():
        parsed = parse_key(key, account_id, application_id, merchant_id, product_id)
        category = ""
        if "ACCOUNT" in key:
            category = "ACCOUNT"
        if "APPLICATION" in key:
            category = "ACCOUNT_APPLICATION"
        if "MERCHANT" in key:
            category = "ACCOUNT_APPLICATION_MERCHANT"
        if "PRODUCT" in key:
            category = "ACCOUNT_APPLICATION_MERCHANT_PRODUCT"
        
        if category == "ACCOUNT":
            parsed["processor"] = ""
            parsed['merchant_id'] = ""
            parsed['product_id'] = ""
        elif category == "ACCOUNT_APPLICATION":
            parsed['merchant_id'] = ""
            parsed['product_id'] = ""
        elif category == "ACCOUNT_APPLICATION_MERCHANT":
            parsed['product_id'] = ""

        if category not in result:
            result[category] = []
        
        entry = {
            "COUNT": value["COUNT"],
            "VERSION": value["VERSION"],
            "SUM": value["SUM"],
            "account_ref": parsed["account_ref"],
            "processor": parsed["processor"],
            "merchant_id": parsed["merchant_id"],
            "product_id": parsed["product_id"],
            "period": parsed["period"],
            "year": parsed["year"],
            "month": parsed["month"],
            "week": parsed["week"],
            "day": parsed["day"],
            "hour": parsed["hour"],
            "channel": parsed["channel"]
        }
        
        result[category].append(entry)
    return result

def create_pagination_token(last_evaluated_key):
    """Create a pagination token from DynamoDB's LastEvaluatedKey"""
    if not last_evaluated_key:
        return None
    return base64.b64encode(json.dumps(last_evaluated_key).encode()).decode()

def parse_pagination_token(token):
    """Parse pagination token back to DynamoDB's ExclusiveStartKey"""
    if not token:
        return None
    try:
        return json.loads(base64.b64decode(token).decode())
    except:
        return None

def lambda_handler(event, context):
    try:
        print("The event is ", event)
        query_params = event['queryStringParameters'] or {}
        print("The query params are ", query_params)

        page = int(query_params.get('page', 1))
        page_size = int(query_params.get('page_size', PAGE_SIZE))
        pagination_token = query_params.get('pagination_token', None)
        
        start_date = query_params.get('start_date')
        end_date = query_params.get('end_date')
        list_type = query_params.get('list_type', '')
        entity_type = query_params.get('entity_type', '')
        channel = query_params.get('channel', '')
        query_type = query_params.get('query_type', '')
        
        if query_type != "single" and (not start_date or not end_date):
            return response(400, {'message': 'start_date and end_date are required'})

        start_timestamp = int(datetime.strptime(start_date, '%Y-%m-%d').timestamp())
        end_timestamp = int((datetime.strptime(end_date, '%Y-%m-%d') + timedelta(days=1)).timestamp() - 1)
        
        partition_key = construct_partition_key(query_params)
        result = {}
        
        if query_type == 'entity_list':
            result = query_transactions_by_entity_and_list(start_timestamp, 
                                                         end_timestamp, 
                                                         list_type, 
                                                         entity_type, 
                                                         query_type, 
                                                         channel,
                                                         page,
                                                         page_size,
                                                         pagination_token)
        elif query_type == 'single':
            items = query_transaction_by_id(partition_key, query_params)
            result = format_single_response(items, page, page_size)
        else:
            result = query_transactions(partition_key,
                                      start_timestamp,
                                      end_timestamp,
                                      query_params, 
                                      channel, 
                                      query_type,
                                      page,
                                      page_size,
                                      pagination_token)
        
        return response(200, result)
    
    except Exception as e:
        print("An error occurred ", e)
        return response(500, {'message': str(e)})

def construct_partition_key(params):
    query_type = params.get('query_type', 'all')
    channel = params.get('channel', '')
    list_type = params.get('list_type', '')
    
    if query_type == 'all' or query_type == 'normal' or query_type == 'affected' or query_type == 'single':
        return 'EVALUATED'
    elif query_type == 'account':
        return f"EVALUATED-{channel}-ACCOUNT-{params.get('account_ref', '')}"
    elif query_type == 'processor':
        return f"EVALUATED-{channel}-APPLICATION-{params.get('processor', '')}"
    elif query_type == 'merchant':
        return f"EVALUATED-{channel}-MERCHANT-{params.get('processor', '')}__{params.get('merchant_id', '')}"
    elif query_type == 'product':
        return f"EVALUATED-{channel}-PRODUCT-{params.get('processor', '')}__{params.get('merchant_id', '')}__{params.get('product_id', '')}"
    elif query_type in ['blacklist', 'watchlist', 'stafflist', 'limit', 'card-diff-country-6h']:
        if query_type == 'stafflist':
            return "EVALUATED-STAFF"
        return f"EVALUATED-{query_type.upper()}"
    elif query_type == 'entity_list':
        return f"EVALUATED-{list_type.upper()}"
    else:
        raise ValueError('Invalid query type')

def format_single_response(items, page, per_page):
    """Format response for single item queries"""
    return {
        'data': items,
        'metadata': {
            'page': 1,
            'previous_page': None,
            'next_page': None,
            'total_records': len(items),
            'pages': 1,
            'per_page': per_page,
            'from': 1 if items else 0,
            'to': len(items),
            'pagination_token': None
        }
    }

def format_paginated_response(items, page, per_page, next_pagination_token=None, total_records=None):
    """Format the response with proper pagination metadata"""
    
    # If we don't have total_records (because we're using cursor pagination),
    # we can't calculate total pages accurately
    if total_records is not None:
        total_pages = math.ceil(total_records / per_page)
    else:
        # For cursor-based pagination, we don't know total pages
        total_pages = None
    
    from_record = ((page - 1) * per_page) + 1 if items else 0
    to_record = from_record + len(items) - 1 if items else 0
    
    return {
        'data': items,
        'metadata': {
            'page': page,
            'previous_page': page - 1 if page > 1 else None,
            'next_page': page + 1 if next_pagination_token else None,
            'total_records': total_records,  # This might be None for cursor pagination
            'pages': total_pages,
            'per_page': per_page,
            'from': from_record,
            'to': to_record,
            'pagination_token': next_pagination_token
        }
    }

def query_transactions(partition_key, start_timestamp, end_timestamp, query_params, channel, query_type, page, per_page, pagination_token=None):
    """Query transactions with proper DynamoDB pagination"""
    print("Starting query_transactions with proper pagination")
    
    start_sk = f"{start_timestamp}_"
    end_sk = f"{end_timestamp}_z"
    
    # Build query parameters
    query_kwargs = {
        'KeyConditionExpression': Key('PARTITION_KEY').eq(partition_key) & 
                                Key('SORT_KEY').between(start_sk, end_sk),
        'ScanIndexForward': False,
        'Limit': per_page
    }
    
    # Add pagination token if provided
    if pagination_token:
        exclusive_start_key = parse_pagination_token(pagination_token)
        if exclusive_start_key:
            query_kwargs['ExclusiveStartKey'] = exclusive_start_key
    
    # Query DynamoDB
    response = table.query(**query_kwargs)
    items = response.get('Items', [])
    last_evaluated_key = response.get('LastEvaluatedKey')
    
    # Process items
    processed_items = []
    for item in items:
        processed_transaction = json.loads(item["processed_transaction"]) 
        original_transaction = processed_transaction["original_transaction"]
        evaluation = processed_transaction.get('evaluation', {})
        
        account_id = original_transaction['account_id']
        application_id = original_transaction['application_id']
        merchant_id = original_transaction['merchant_id']
        product_id = original_transaction['product_id']
        assigned_person = assigned_status(original_transaction['transaction_id'])
        
        processed_item = {
            'account_ref': account_id,
            'processor': application_id,
            'merchant_id': merchant_id,
            'product_id': product_id,
            'transaction_id': original_transaction['transaction_id'],
            'date': original_transaction['date'],
            'amount': original_transaction['amount'],
            'currency': original_transaction['currency'],
            'country': original_transaction['country'],
            'channel': original_transaction['channel'],
            'evaluation': transform_keys(evaluation),
            'assigned_to': assigned_person,
            'relevant_aggregates': transform_aggregates(
                processed_transaction.get('aggregates', {}), 
                account_id, 
                application_id, 
                merchant_id, 
                product_id
            )
        }
        
        # Apply filtering based on query_type and channel
        should_include = False
        
        if query_type == "all" or query_type == "limit":
            should_include = True
        elif channel and channel == processed_item['channel']:
            should_include = True
        elif not channel:
            should_include = True
            
        if should_include:
            # Additional filtering based on query_type
            if query_type == 'normal' and processed_item["evaluation"] != {}:
                should_include = False
            elif query_type == 'affected' and processed_item["evaluation"] == {}:
                should_include = False
                
        if should_include:
            processed_items.append(processed_item)
    
    # Create next pagination token
    next_token = create_pagination_token(last_evaluated_key) if last_evaluated_key else None
    
    return format_paginated_response(processed_items, page, per_page, next_token)

def query_transaction_by_id(partition_key, params):
    """Query a single transaction by ID"""
    response = table.query(
        KeyConditionExpression=Key('PARTITION_KEY').eq(partition_key) & 
        Key('SORT_KEY').eq(params.get("transaction_id"))
    )
    
    items = []
    if 'Items' in response:
        items = response['Items']
    if 'Item' in response:
        items = [response['Item']]

    processed_items = []
    for item in items:
        processed_transaction = json.loads(item["processed_transaction"]) 
        original_transaction = processed_transaction["original_transaction"]
        evaluation = processed_transaction.get('evaluation', {})
        account_id = original_transaction['account_id']
        application_id = original_transaction['application_id']
        merchant_id = original_transaction['merchant_id']
        product_id = original_transaction['product_id']
        
        processed_item = {
            'account_ref': account_id,
            'processor': application_id,
            'merchant_id': merchant_id,
            'product_id': product_id,
            'transaction_id': original_transaction['transaction_id'],
            'date': original_transaction['date'],
            'amount': original_transaction['amount'],
            'currency': original_transaction['currency'],
            'country': original_transaction['country'],
            'channel': original_transaction['channel'],
            'evaluation': transform_keys(evaluation),
            'relevant_aggregates': transform_aggregates(
                processed_transaction.get('aggregates', {}),
                account_id,
                application_id,
                merchant_id,
                product_id
            )
        }
        processed_items.append(processed_item)
    
    return processed_items

def query_transactions_by_entity_and_list(start_timestamp, end_timestamp, list_type, entity_type, query_type, channel, page, per_page, pagination_token=None):
    """Query transactions by entity and list with proper pagination"""
    partition_key = f"EVALUATED-{list_type.upper()}"
    start_sk = f"{start_timestamp}_"
    end_sk = f"{end_timestamp}_z"
    
    # Build query parameters
    query_kwargs = {
        'KeyConditionExpression': Key('PARTITION_KEY').eq(partition_key) & 
                                Key('SORT_KEY').between(start_sk, end_sk),
        'ScanIndexForward': False,
        'Limit': per_page * 2  # Get more items since we'll filter them
    }
    
    # Add pagination token if provided
    if pagination_token:
        exclusive_start_key = parse_pagination_token(pagination_token)
        if exclusive_start_key:
            query_kwargs['ExclusiveStartKey'] = exclusive_start_key
    
    # Query DynamoDB
    response = table.query(**query_kwargs)
    items = response.get('Items', [])
    last_evaluated_key = response.get('LastEvaluatedKey')
    
    # Filter and process items
    processed_items = []
    for item in items:
        if len(processed_items) >= per_page:
            break
            
        processed_transaction = json.loads(item["processed_transaction"])
        original_transaction = processed_transaction["original_transaction"]
        
        # Apply entity type filter
        entity_match = False
        if entity_type == 'account' and original_transaction.get('account_id'):
            entity_match = True
        elif entity_type == 'application' and original_transaction.get('application_id'):
            entity_match = True
        elif entity_type == 'merchant' and original_transaction.get('merchant_id'):
            entity_match = True
        elif entity_type == 'product' and original_transaction.get('product_id'):
            entity_match = True
        
        # Apply channel filter
        channel_match = not channel or original_transaction.get('channel') == channel
        
        if entity_match and channel_match:
            evaluation = processed_transaction.get('evaluation', {})
            account_id = original_transaction['account_id']
            application_id = original_transaction['application_id']
            merchant_id = original_transaction['merchant_id']
            product_id = original_transaction['product_id']
            assigned_person = assigned_status(original_transaction['transaction_id'])
            
            processed_item = {
                'account_ref': account_id,
                'processor': application_id,
                'merchant_id': merchant_id,
                'product_id': product_id,
                'transaction_id': original_transaction['transaction_id'],
                'date': original_transaction['date'],
                'amount': original_transaction['amount'],
                'currency': original_transaction['currency'],
                'country': original_transaction['country'],
                'channel': original_transaction['channel'],
                'evaluation': transform_keys(evaluation),
                'assigned_to': assigned_person,
                'relevant_aggregates': transform_aggregates(
                    processed_transaction.get('aggregates', {}),
                    account_id,
                    application_id,
                    merchant_id,
                    product_id
                )
            }
            processed_items.append(processed_item)
    
    # Create next pagination token only if we have more items
    next_token = None
    if last_evaluated_key and len(processed_items) == per_page:
        next_token = create_pagination_token(last_evaluated_key)
    
    return format_paginated_response(processed_items, page, per_page, next_token)

def transform_keys(dictionary):
    """Transform keys in dictionary, replacing 'application' with 'processor'"""
    transformed_dict = {}
    for key, value in dictionary.items():
        new_key = key.replace('application', 'processor')
        transformed_dict[new_key] = value
    return transformed_dict

def assigned_status(transaction_id):
    """Get assigned status for transaction"""
    #print("The transaction id is ", transaction_id)
    if transaction_id == "":
        return ""
        
    response = table.query(
        KeyConditionExpression=Key('PARTITION_KEY').eq("CASE") & Key('SORT_KEY').eq(transaction_id)
    )
    
    items = response['Items']
    #print("The case items are ", items)
    if len(items) == 0:
        return {}
    else:
        try:
            return items[0]["assigned_to"]
        except Exception as e:
            print("Found an error in the retrieval of a case's email ", e)
            return {}

def response(status_code, body):
    """Format API response"""
    response_message = "Operation Successful" if status_code == 200 else "Unsuccessful operation"
    
    body_to_send = {
        "responseCode": status_code,
        "responseMessage": response_message,
        "data": body.get('data', []),
        "metadata": body.get('metadata')
    }
    
    return {
        'statusCode': status_code,
        'body': json.dumps(body_to_send),
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Credentials': True,
        },
    }