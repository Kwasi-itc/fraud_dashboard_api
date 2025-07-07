import os
import json
import boto3
from boto3.dynamodb.conditions import Key, Attr
from datetime import datetime, timedelta
import re
import base64

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.environ['FRAUD_PROCESSED_TRANSACTIONS_TABLE'])

PAGE_SIZE = 50  # Default page size

def encode_pagination_token(last_evaluated_key):
    if not last_evaluated_key:
        return None
    return base64.b64encode(json.dumps(last_evaluated_key).encode()).decode()

def decode_pagination_token(pagination_token):
    if not pagination_token:
        return None
    try:
        return json.loads(base64.b64decode(pagination_token.encode()).decode())
    except:
        raise ValueError('Invalid pagination token')


def parse_key(key, account_id, application_id, merchant_id, product_id):
    #print("I am here 2c")
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
        #"account_id": account_id,
        'account_ref': account_id,
        #"application_id": application_id,
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
    #print("I am here 2d")
    return result

def transform_aggregates(relevant_aggregates, account_id, application_id, merchant_id, product_id):
    print("I am here 2b")
    result = {}
    for key, value in relevant_aggregates.items():
        parsed = parse_key(key, account_id, application_id, merchant_id, product_id)
        #print("I am here 2e")
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
            #parsed["application_id"] = ""
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
            #"account_id": parsed["account_id"],
            "account_ref": parsed["account_ref"],#parsed["account_id"],
            #"application_id": parsed["application_id"],
            "processor": parsed["processor"],#parsed["application_id"],
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
    print("I am here 2f")
    return result




def lambda_handler(event, context):
    try:
        print("The event is ", event)
        query_params = event['queryStringParameters'] or {}
        print("The query params are ", query_params)

        page_size = int(query_params.get('page_size', PAGE_SIZE))
        pagination_token = query_params.get('pagination_token', None)
        
        start_date = query_params.get('start_date')
        end_date = query_params.get('end_date')
        list_type = query_params.get('list_type', '')
        entity_type = query_params.get('entity_type', '')
        channel = query_params.get('channel', '')
        query_type = query_params.get('query_type', '')
        
        #if not start_date or not end_date and query_type != "single":
        #    return response(400, {'message': 'start_date and end_date are required'})
        if query_type != "single" and (not start_date or not end_date):
            return response(400, {'message': 'start_date and end_date are required'})

        start_timestamp = int(datetime.strptime(start_date, '%Y-%m-%d').timestamp())
        end_timestamp = int((datetime.strptime(end_date, '%Y-%m-%d') + timedelta(days=1)).timestamp() - 1)
        #end_timestamp = int(datetime.strptime(end_date, '%Y-%m-%d').timestamp())
        
        partition_key = construct_partition_key(query_params)
        items = []
        result = {}
        if query_type == 'entity_list':
            result = query_transactions_by_entity_and_list(start_timestamp, 
                                                          end_timestamp, 
                                                          list_type, 
                                                          entity_type, 
                                                          query_type, 
                                                          channel,
                                                            page_size,
                                                            pagination_token)
        elif query_type == 'single':
            items = query_transaction_by_id(partition_key, query_params)
            result = {
                'items': items,
                'next_token': None
            }
        else:
            result = query_transactions(partition_key,
                                        start_timestamp,
                                        end_timestamp,
                                        query_params, 
                                        channel, 
                                        query_type,
                                        page_size,
                                        pagination_token)
        
        return response(200, result)
    
    except Exception as e:
        print("An error occured ", e)
        return response(500, {'message': str(e)})

def construct_partition_key(params):
    query_type = params.get('query_type', 'all')
    channel = params.get('channel', '')
    list_type = params.get('list_type', '')
    #entity_type = params.get('entity_type', '')
    
    if query_type == 'all' or query_type == 'normal' or query_type == 'affected' or query_type == 'single':
        return 'EVALUATED'
    elif query_type == 'account':
        return f"EVALUATED-{channel}-ACCOUNT-{params.get('account_ref', '')}"
    #elif query_type == 'application':
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

def query_transaction_by_id(partition_key, params):
    response = table.query(
        KeyConditionExpression=Key('PARTITION_KEY').eq(partition_key) & Key('SORT_KEY').eq(params.get("transaction_id"))
    )
    
    items = []
    if 'Items' in response:
        items = response['Items']
    if 'Item' in response:
        items = response['Item']
        items = [items]
    print("The items are ", items)
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
            #'account_id': account_id,
            'account_ref': account_id,
            #'application_id': application_id,
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
            'relevant_aggregates': transform_aggregates(processed_transaction.get('aggregates', {}), account_id, application_id, merchant_id, product_id)
            #'relevant_aggregates': get_relevant_aggregates(processed_transaction.get('aggregates', {}), original_transaction, evaluation)
        }
        processed_items.append(processed_item)
    return processed_items

def query_transactions_by_entity_and_list(start_timestamp, end_timestamp, list_type, entity_type, query_type, channel, limit, pagination_token):
    partition_key = f"EVALUATED-{list_type.upper()}"
    start_sk = f"{start_timestamp}_"
    end_sk = f"{end_timestamp}_z"
    
    # Prepare query parameters
    query_params = {
        'KeyConditionExpression': Key('PARTITION_KEY').eq(partition_key) & Key('SORT_KEY').between(start_sk, end_sk),
        'Limit': limit,
        'ScanIndexForward': False
    }

    if pagination_token:
        last_evaluated_key = decode_pagination_token(pagination_token)
        if last_evaluated_key:
            query_params['ExclusiveStartKey'] = last_evaluated_key
    
    response = table.query(**query_params)
    
    items = response.get('Items', [])
    last_evaluated_key = response.get('LastEvaluatedKey')
    
    
    filtered_items = []
    for item in items:
        processed_transaction = json.loads(item["processed_transaction"])
        original_transaction = processed_transaction["original_transaction"]
        
        # Check if the transaction matches the entity type and channel
        if (entity_type == 'account' and original_transaction.get('account_id')) or \
           (entity_type == 'application' and original_transaction.get('application_id')) or \
           (entity_type == 'merchant' and original_transaction.get('merchant_id')) or \
           (entity_type == 'product' and original_transaction.get('product_id')):
            if not channel or original_transaction.get('channel') == channel:
                filtered_items.append(item)
    
    processed_items = []
    for item in filtered_items:
        processed_transaction = json.loads(item["processed_transaction"]) 
        original_transaction = processed_transaction["original_transaction"]
        evaluation = processed_transaction.get('evaluation', {})
        account_id = original_transaction['account_id']
        application_id = original_transaction['application_id']
        merchant_id = original_transaction['merchant_id']
        product_id = original_transaction['product_id']
        assigned_person = assigned_status(original_transaction['transaction_id'])
        
        
        processed_item = {
            #'account_id': account_id,
            'account_ref': account_id,
            #'application_id': application_id,
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
            'relevant_aggregates': transform_aggregates(processed_transaction.get('aggregates', {}), account_id, application_id, merchant_id, product_id)
            #'relevant_aggregates': get_relevant_aggregates(processed_transaction.get('aggregates', {}), original_transaction, evaluation)
        }
        if query_type == "all":
            processed_items.append(processed_item)
        else:
            if channel == processed_item['channel']:
                processed_items.append(processed_item)

    return {
        'items': processed_items,
        'next_token': encode_pagination_token(last_evaluated_key) if last_evaluated_key else None
    }


def query_transactions(partition_key, start_timestamp, end_timestamp, query_params, channel, query_type, limit, pagination_token):
    print("I am here 1")
    start_sk = f"{start_timestamp}_"
    end_sk = f"{end_timestamp}_z"

    print("The query_params are ", query_params)
    
    # Prepare query parameters
    query_params = {
        'KeyConditionExpression': Key('PARTITION_KEY').eq(partition_key) & Key('SORT_KEY').between(start_sk, end_sk),
        'Limit': limit
    }

    
    print("Partition key is ", partition_key)
    print("Start and end timestamp is ", start_sk, end_sk)
    print("The limit is ", limit)

    response = None
    if pagination_token:
        last_evaluated_key = decode_pagination_token(pagination_token)
        if last_evaluated_key:
            query_params['ExclusiveStartKey'] = last_evaluated_key
            response = table.query(KeyConditionExpression=Key('PARTITION_KEY').eq(partition_key) & Key('SORT_KEY').between(start_sk, end_sk),
                                    Limit=limit,
                                    ExclusiveStartKey=last_evaluated_key,
                                    ScanIndexForward=False)
    else:
        response = table.query(KeyConditionExpression=Key('PARTITION_KEY').eq(partition_key) & Key('SORT_KEY').between(start_sk, end_sk), Limit=limit,
                                    ScanIndexForward=False)
            
    print("The response from the db is ", response)
    ##Testing stuff
    #test_response = table.query(
    #    KeyConditionExpression=Key('PARTITION_KEY').eq(partition_key) & Key('SORT_KEY').between(start_sk, end_sk)
    #    )
    #print("The test response is ", test_response)

    #response = table.query(**query_params)
    #response = table.query(KeyConditionExpression=Key('PARTITION_KEY').eq(partition_key) & Key('SORT_KEY').between(start_sk, end_sk), Limit=limit)
    last_evaluated_key = response.get('LastEvaluatedKey')

    
    items = []
    if "Item" in response:
        items = response['Item']
        items = [items]
    if "Items" in response:
        items = response['Items']
    
    print("The items are ", items)

    filtered_items = [
        item for item in items
        if start_timestamp <= int(item['SORT_KEY'].split('_')[0]) <= end_timestamp
    ]

    print("The filtered items are ", filtered_items)
    print("I am here 2")
    
    # Process items to return only necessary information
    processed_items = []
    for item in filtered_items:
        processed_transaction = json.loads(item["processed_transaction"]) 
        original_transaction = processed_transaction["original_transaction"]
        evaluation = processed_transaction.get('evaluation', {})
        account_id = original_transaction['account_id']
        application_id = original_transaction['application_id']
        merchant_id = original_transaction['merchant_id']
        product_id = original_transaction['product_id']
        assigned_person = assigned_status(original_transaction['transaction_id'])
        
        
        processed_item = {
            #'account_id': account_id,
            'account_ref': account_id,
            #'application_id': application_id,
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
            'relevant_aggregates': transform_aggregates(processed_transaction.get('aggregates', {}), account_id, application_id, merchant_id, product_id)
            #'relevant_aggregates': get_relevant_aggregates(processed_transaction.get('aggregates', {}), original_transaction, evaluation)
        }
        if query_type == "all" or query_type == "limit":
            processed_items.append(processed_item)
        else:
            if channel == processed_item['channel']:
                processed_items.append(processed_item)
        ##The query comes with channel all the time, so if channel is 
    print("I am here 3")
    print("The query params when trying to get the items are ", query_params)
    query_type = query_params.get('query_type', 'all')

    if query_type == 'normal':
        possible_processed_items = []
        for processed_item in processed_items:
            if processed_item["evaluation"] == {}:
                possible_processed_items.append(processed_item)
        processed_items = possible_processed_items
    
    if query_type == 'affected':
        possible_processed_items = []
        for processed_item in processed_items:
            if processed_item["evaluation"] != {}:
                possible_processed_items.append(processed_item)
        processed_items = possible_processed_items

    return {
        'items': processed_items,
        'next_token': encode_pagination_token(last_evaluated_key) if last_evaluated_key else None
    }


def assigned_status(transaction_id):
    print("The transaction id is ", transaction_id)
    if transaction_id == "":
        return ""
    response = table.query(
        KeyConditionExpression=Key('PARTITION_KEY').eq("CASE") & Key('SORT_KEY').eq(transaction_id)
    )
    
    items = response['Items']
    print("The case items are ", items)
    if len(items) == 0:
        return {}
    else:
        try:
            return items[0]["assigned_to"]
        except Exception as e:
            print("Found an error in the retrieval of a case's email ", e)
            return {}            

def transform_keys(dictionary):
    # Create a new dictionary with transformed keys
    transformed_dict = {}
    
    for key, value in dictionary.items():
        # Replace 'application' with 'processor' in the key
        new_key = key.replace('application', 'processor')
        transformed_dict[new_key] = value
        
    return transformed_dict

def get_relevant_aggregates(aggregates, transaction, evaluation):
    relevant_aggregates = {}
    
    channel = transaction['channel']
    account_id = transaction['account_id']
    application_id = transaction['application_id']
    merchant_id = transaction['merchant_id']
    product_id = transaction['product_id']
    
    date = datetime.strptime(transaction['date'], '%Y-%m-%dT%H:%M:%S')
    month_key = f"MONTH-{date.strftime('%Y-%m')}"
    week_key = f"WEEK-{date.strftime('%Y-%W')}"
    day_key = f"DAY-{date.strftime('%Y-%m-%d')}"
    hour_key = f"HOUR-{date.strftime('%Y-%m-%d-%H:00:00')}"
    
    # Helper function to add relevant aggregate
    def add_aggregate(key, period):
        if key in aggregates:
            relevant_aggregates[f"{period}_aggregate"] = {
                'sum': aggregates[key]['SUM'],
                'count': aggregates[key]['COUNT']
            }
    
    for reason in evaluation.keys():
        if any(list_type in reason for list_type in ['blacklist', 'watchlist', 'stafflist']):
            # For list-related reasons, we don't need to add specific aggregates
            continue
        
        if 'amount_exceeded' in reason or 'sum_exceeded' in reason or 'count_exceeded' in reason:
            level = None
            if 'account_application_merchant_product' in reason:
                level = f"ACCOUNT_APPLICATION_MERCHANT_PRODUCT-{account_id}__{application_id}__{merchant_id}__{product_id}"
            elif 'account_application_merchant' in reason:
                level = f"ACCOUNT_APPLICATION_MERCHANT-{account_id}__{application_id}__{merchant_id}"
            elif 'account_application' in reason:
                level = f"ACCOUNT_APPLICATION-{account_id}__{application_id}"
            elif 'account' in reason:
                level = f"ACCOUNT-{account_id}"
            
            if level:
                base_key = f"AGGREGATION-{channel}-{level}-"
                add_aggregate(base_key + month_key, 'MONTHLY')
                add_aggregate(base_key + week_key, 'WEEKLY')
                add_aggregate(base_key + day_key, 'DAILY')
                add_aggregate(base_key + hour_key, 'HOURLY')
    
    return relevant_aggregates

def response(status_code, body):
    response_message = "Operation Successful" if status_code == 200 else "Unsuccessful operation"
    
    # Handle both paginated and non-paginated responses
    response_data = body.get('items', []) if isinstance(body, dict) else body
    next_token = body.get('next_token') if isinstance(body, dict) else None
    
    body_to_send = {
        "responseCode": status_code,
        "responseMessage": response_message,
        "data": response_data,
        "pagination": {
            "nextToken": next_token
        } if next_token is not None else None
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