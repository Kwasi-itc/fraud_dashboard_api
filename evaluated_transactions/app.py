import os
import json
import boto3
from boto3.dynamodb.conditions import Key, Attr
from datetime import datetime, timedelta

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.environ['FRAUD_PROCESSED_TRANSACTIONS_TABLE'])

def lambda_handler(event, context):
    try:
        query_params = event['queryStringParameters'] or {}
        
        start_date = query_params.get('start_date')
        end_date = query_params.get('end_date')
        
        if not start_date or not end_date:
            return response(400, {'message': 'start_date and end_date are required'})
        
        start_timestamp = int(datetime.strptime(start_date, '%Y-%m-%d').timestamp())
        end_timestamp = int(datetime.strptime(end_date, '%Y-%m-%d').timestamp())
        
        partition_key = construct_partition_key(query_params)
        
        items = query_transactions(partition_key, start_timestamp, end_timestamp)
        
        return response(200, {'items': items})
    
    except Exception as e:
        return response(500, {'message': str(e)})

def construct_partition_key(params):
    query_type = params.get('query_type', 'all')
    channel = params.get('channel', '')
    
    if query_type == 'all':
        return 'EVALUATED'
    elif query_type == 'account':
        return f"EVALUATED-{channel}-ACCOUNT-{params.get('account_id', '')}"
    elif query_type == 'application':
        return f"EVALUATED-{channel}-APPLICATION-{params.get('application_id', '')}"
    elif query_type == 'merchant':
        return f"EVALUATED-{channel}-MERCHANT-{params.get('application_id', '')}_{params.get('merchant_id', '')}"
    elif query_type == 'product':
        return f"EVALUATED-{channel}-PRODUCT-{params.get('application_id', '')}_{params.get('merchant_id', '')}_{params.get('product_id', '')}"
    elif query_type in ['blacklist', 'watchlist', 'staff', 'limit']:
        return f"EVALUATED-{query_type.upper()}"
    else:
        raise ValueError('Invalid query type')

def query_transactions(partition_key, start_timestamp, end_timestamp):
    response = table.query(
        KeyConditionExpression=Key('PK').eq(partition_key) & Key('SK').between(start_timestamp, end_timestamp)
    )
    
    items = response['Items']
    
    # Process items to return only necessary information
    processed_items = []
    for item in items:
        original_transaction = item['original_transaction']
        evaluation = item['evaluation']
        
        processed_item = {
            'transaction_id': original_transaction['transaction_id'],
            'date': original_transaction['date'],
            'amount': original_transaction['amount'],
            'currency': original_transaction['currency'],
            'country': original_transaction['country'],
            'channel': original_transaction['channel'],
            'evaluation_result': evaluation['result'],
            'evaluation_reason': evaluation.get('reason', ''),
            'relevant_aggregates': get_relevant_aggregates(item['aggregates'], original_transaction, evaluation)
        }
        processed_items.append(processed_item)
    
    return processed_items

def get_relevant_aggregates(aggregates, transaction, evaluation):
    relevant_aggregates = {}
    reason = evaluation.get('reason', '')
    result = evaluation['result']
    
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
    
    # Check reason and add relevant aggregates
    if 'list' in reason.lower():
        # For list-related reasons, we don't need to add specific aggregates
        pass
    elif 'amount_exceeded' in reason.lower() or 'sum_exceeded' in reason.lower() or 'count_exceeded' in reason.lower():
        level = None
        if 'account_application_merchant_product' in reason.lower():
            level = f"ACCOUNT_APPLICATION_MERCHANT_PRODUCT-{account_id}__{application_id}__{merchant_id}__{product_id}"
        elif 'account_application_merchant' in reason.lower():
            level = f"ACCOUNT_APPLICATION_MERCHANT-{account_id}__{application_id}__{merchant_id}"
        elif 'account_application' in reason.lower():
            level = f"ACCOUNT_APPLICATION-{account_id}__{application_id}"
        elif 'account' in reason.lower():
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