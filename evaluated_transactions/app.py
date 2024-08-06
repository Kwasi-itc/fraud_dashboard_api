import os
import json
import boto3
from boto3.dynamodb.conditions import Key, Attr
from datetime import datetime, timedelta
import re

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.environ['FRAUD_PROCESSED_TRANSACTIONS_TABLE'])


def parse_key(key, account_id, application_id, merchant_id, product_id):
    print("I am here 1")
    print("The key is ", key)
    print("The type of key is ", type(key))
    parts = key.split('-')
    print("The parts are ", parts)
    print("The number of parts are ", len(parts))
    channel = parts[1]
    print("Channel is ", channel)
    entities = parts[2].split('_')
    print("The entities are ", entities)
    print("The number of entities are ", len(entities))
    time_info = parts[-1].split('-')
    print("I am here 2")

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
        "account_id": account_id,
        "application_id": application_id,
        "merchant_id": merchant_id,
        "product_id": product_id,
        "period": period,
        "year": year,
        "month": "",
        "week": "",
        "day": "",
        "hour": ""
    }
    print("I am here 3")


    #for entity in entities:
    #    if entity.startswith("ACCOUNT"):
    #        result["account_id"] = parts[3].split('_')[0]
    #    elif entity == "APPLICATION":
    #        result["application_id"] = parts[3].split('_')[1]
    #    elif entity == "MERCHANT":
    #        result["merchant_id"] = parts[3].split('_')[2]
    #    elif entity == "PRODUCT":
    #        result["product_id"] = parts[3].split('_')[3]
    print("The current result is ", result)
    print("I am here 4")
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
    print("Starting")
    print("The relevant aggregates are ", relevant_aggregates)
    for key, value in relevant_aggregates.items():
        parsed = parse_key(key, account_id, application_id, merchant_id, product_id)
        category = '_'.join([entity for entity in ["ACCOUNT", "APPLICATION", "MERCHANT", "PRODUCT"] 
                             if parsed[f"{entity.lower()}_id"]])
        
        if category not in result:
            result[category] = []
        
        entry = {
            "COUNT": value["COUNT"],
            "VERSION": value["VERSION"],
            "SUM": value["SUM"],
            "account_id": parsed["account_id"],
            "application_id": parsed["application_id"],
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




def lambda_handler(event, context):
    try:
        print("The event is ", event)
        query_params = event['queryStringParameters'] or {}
        print("The query params are ", query_params)
        
        start_date = query_params.get('start_date')
        end_date = query_params.get('end_date')
        list_type = query_params.get('list_type', '')
        entity_type = query_params.get('entity_type', '')
        channel = query_params.get('channel', '')
        query_type = query_params.get('query_type', '')
        
        if not start_date or not end_date:
            return response(400, {'message': 'start_date and end_date are required'})
        
        start_timestamp = int(datetime.strptime(start_date, '%Y-%m-%d').timestamp())
        end_timestamp = int(datetime.strptime(end_date, '%Y-%m-%d').timestamp())
        
        partition_key = construct_partition_key(query_params)
        items = []
        if query_type == 'entity_list':
            items = query_transactions_by_entity_and_list(start_timestamp, end_timestamp, list_type, entity_type, channel)
        else:
            items = query_transactions(partition_key, start_timestamp, end_timestamp, query_params)
        
        return response(200, {'items': items})
    
    except Exception as e:
        print("An error occured ", e)
        return response(500, {'message': str(e)})

def construct_partition_key(params):
    query_type = params.get('query_type', 'all')
    channel = params.get('channel', '')
    list_type = params.get('list_type', '')
    #entity_type = params.get('entity_type', '')
    
    if query_type == 'all' or query_type == 'normal':
        return 'EVALUATED'
    elif query_type == 'account':
        return f"EVALUATED-{channel}-ACCOUNT-{params.get('account_id', '')}"
    elif query_type == 'application':
        return f"EVALUATED-{channel}-APPLICATION-{params.get('application_id', '')}"
    elif query_type == 'merchant':
        return f"EVALUATED-{channel}-MERCHANT-{params.get('application_id', '')}__{params.get('merchant_id', '')}"
    elif query_type == 'product':
        return f"EVALUATED-{channel}-PRODUCT-{params.get('application_id', '')}__{params.get('merchant_id', '')}__{params.get('product_id', '')}"
    elif query_type in ['blacklist', 'watchlist', 'staff', 'limit', 'card-diff-country-6h']:
        return f"EVALUATED-{query_type.upper()}"
    elif query_type == 'entity_list':
        return f"EVALUATED-{list_type.upper()}"
    else:
        raise ValueError('Invalid query type')

def query_transactions_by_entity_and_list(start_timestamp, end_timestamp, list_type, entity_type, channel=''):
    partition_key = f"EVALUATED-{list_type.upper()}"
    start_sk = f"{start_timestamp}_"
    end_sk = f"{end_timestamp}_z"
    
    response = table.query(
        KeyConditionExpression=Key('PARTITION_KEY').eq(partition_key) & Key('SORT_KEY').between(start_sk, end_sk)
    )
    
    items = response['Items']
    
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

        
        
        processed_item = {
            'account_id': account_id,
            'application_id': application_id,
            'merchant_id': merchant_id,
            'product_id': product_id,
            'transaction_id': original_transaction['transaction_id'],
            'date': original_transaction['date'],
            'amount': original_transaction['amount'],
            'currency': original_transaction['currency'],
            'country': original_transaction['country'],
            'channel': original_transaction['channel'],
            'evaluation': evaluation,
            'relevant_aggregates': transform_aggregates(processed_transaction.get('aggregates', {}), account_id, application_id, merchant_id, product_id)
            #'relevant_aggregates': get_relevant_aggregates(processed_transaction.get('aggregates', {}), original_transaction, evaluation)
        }
        processed_items.append(processed_item)

    return processed_items


def query_transactions(partition_key, start_timestamp, end_timestamp, query_params):
    start_sk = f"{start_timestamp}_"
    end_sk = f"{end_timestamp}_z"
    
    response = table.query(
        KeyConditionExpression=Key('PARTITION_KEY').eq(partition_key) & Key('SORT_KEY').between(start_sk, end_sk)
    )
    
    items = response['Items']
    print("The items are ", items)

    filtered_items = [
        item for item in items
        if start_timestamp <= int(item['SORT_KEY'].split('_')[0]) <= end_timestamp
    ]

    print("The filtered items are ", filtered_items)
    
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

        
        
        processed_item = {
            'account_id': account_id,
            'application_id': application_id,
            'merchant_id': merchant_id,
            'product_id': product_id,
            'transaction_id': original_transaction['transaction_id'],
            'date': original_transaction['date'],
            'amount': original_transaction['amount'],
            'currency': original_transaction['currency'],
            'country': original_transaction['country'],
            'channel': original_transaction['channel'],
            'evaluation': evaluation,
            'relevant_aggregates': transform_aggregates(processed_transaction.get('aggregates', {}), account_id, application_id, merchant_id, product_id)
            #'relevant_aggregates': get_relevant_aggregates(processed_transaction.get('aggregates', {}), original_transaction, evaluation)
        }
        processed_items.append(processed_item)
    
    print("The query params when trying to get the items are ", query_params)
    query_type = query_params.get('query_type', 'all')

    if query_type == 'normal':
        possible_processed_items = []
        for processed_item in processed_items:
            if processed_item["evaluation"] == {}:
                possible_processed_items.append(processed_item)
        processed_items = possible_processed_items

    return processed_items
            


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
    return {
        'statusCode': status_code,
        'body': json.dumps(body),
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Credentials': True,
        },
    }