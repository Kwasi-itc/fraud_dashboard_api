import json
from utils import response, get_dynamodb_table, decimal_default
import boto3

def read_limit(event, limit_type):
    table = get_dynamodb_table()
    params = event['queryStringParameters'] or {}
    
    partition_key, sort_key = construct_keys(params, limit_type)
    
    if not partition_key:
        return response(400, "Missing required parameters")
    
    if sort_key:
        response = table.get_item(Key={'PARTITION_KEY': partition_key, 'SORT_KEY': sort_key})
        item = response.get('Item')
    else:
        response = table.query(KeyConditionExpression=boto3.dynamodb.conditions.Key('PARTITION_KEY').eq(partition_key))
        item = response.get('Items')
    
    return response(200, json.dumps(item, default=decimal_default)) if item else response(404, "Limit not found")

def construct_keys(params, limit_type):
    channel = params.get('channel')
    if not channel:
        return None, None
    
    partition_key = f"LIMITS-{channel}-{limit_type}"
    sort_key = "__".join(filter(None, [
        params.get('account_id'),
        params.get('application_id'),
        params.get('merchant_id'),
        params.get('product_id')
    ]))
    
    return partition_key, sort_key