import os
import json
import boto3
from boto3.dynamodb.conditions import Key
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
        end_timestamp = int((datetime.strptime(end_date, '%Y-%m-%d') + timedelta(days=1)).timestamp() - 1)

        #end_timestamp = int(datetime.strptime(end_date, '%Y-%m-%d').timestamp())
        
        summary = {
            'blacklist': {'count': 0, 'sum': 0},
            'watchlist': {'count': 0, 'sum': 0},
            'stafflist': {'count': 0, 'sum': 0},
            'limits': {'count': 0, 'sum': 0},
            'normal': {'count': 0, 'sum': 0}
        }
        
        for list_type in ['BLACKLIST', 'WATCHLIST', 'STAFF', 'LIMIT']:
            items = query_transactions(f"EVALUATED-{list_type}", start_timestamp, end_timestamp)
            for item in items:
                processed_transaction = json.loads(item["processed_transaction"])
                original_transaction = processed_transaction["original_transaction"]
                amount = float(original_transaction['amount'])
                
                if list_type == 'STAFF':
                    list_type = 'stafflist'
                elif list_type == 'LIMIT':
                    list_type = 'limits'
                
                summary[list_type.lower()]['count'] += 1
                summary[list_type.lower()]['sum'] += amount
        
        # Query for normal transactions
        normal_items = query_transactions("EVALUATED", start_timestamp, end_timestamp)
        for item in normal_items:
            processed_transaction = json.loads(item["processed_transaction"])
            if not processed_transaction.get('evaluation'):  # No evaluation means it's a normal transaction
                original_transaction = processed_transaction["original_transaction"]
                amount = float(original_transaction['amount'])
                summary['normal']['count'] += 1
                summary['normal']['sum'] += amount
        
        return response(200, summary)
    
    except Exception as e:
        print("An error occurred ", e)
        return response(500, {'message': str(e)})

def query_transactions(partition_key, start_timestamp, end_timestamp):
    start_sk = f"{start_timestamp}_"
    end_sk = f"{end_timestamp}_z"
    
    response = table.query(
        KeyConditionExpression=Key('PARTITION_KEY').eq(partition_key) & Key('SORT_KEY').between(start_sk, end_sk)
    )
    
    return response['Items']

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