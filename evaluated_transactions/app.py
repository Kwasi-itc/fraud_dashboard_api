import os
import json
import boto3
from boto3.dynamodb.conditions import Key, Attr
from datetime import datetime, timedelta

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.environ['FRAUD_PROCESSED_TRANSACTIONS_TABLE'])

def lambda_handler(event, context):
    try:
        print("The event is ", event)
        query_params = event['queryStringParameters'] or {}
        print("The query params are ", query_params)
        
        start_date = query_params.get('start_date')
        end_date = query_params.get('end_date')
        
        if not start_date or not end_date:
            return response(400, {'message': 'start_date and end_date are required'})
        
        start_timestamp = int(datetime.strptime(start_date, '%Y-%m-%d').timestamp())
        end_timestamp = int(datetime.strptime(end_date, '%Y-%m-%d').timestamp())
        
        partition_key = construct_partition_key(query_params)
        
        items = query_transactions(partition_key, start_timestamp, end_timestamp, query_params)
        
        return response(200, {'items': items})
    
    except Exception as e:
        print("An error occured ", e)
        return response(500, {'message': str(e)})

def construct_partition_key(params):
    query_type = params.get('query_type', 'all')
    channel = params.get('channel', '')
    
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
    elif query_type in ['blacklist', 'watchlist', 'staff', 'limit', 'card-diff-country-gh']:
        return f"EVALUATED-{query_type.upper()}"
    else:
        raise ValueError('Invalid query type')



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
            'relevant_aggregates': processed_transaction.get('aggregates', {})
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