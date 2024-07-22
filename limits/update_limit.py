import json
from decimal import Decimal
from utils import response, get_dynamodb_table

def create_limit(event, limit_type):
    table = get_dynamodb_table()
    params = event['queryStringParameters'] or {}
    body = json.loads(event['body'])
    
    partition_key, sort_key = construct_keys(params, limit_type)
    
    if not partition_key:
        return response(400, "Missing required parameters")
    
    required_attributes = [
        'AMOUNT', 'HOURLY_SUM', 'DAILY_SUM', 'WEEKLY_SUM', 'MONTHLY_SUM',
        'HOURLY_COUNT', 'DAILY_COUNT', 'WEEKLY_COUNT', 'MONTHLY_COUNT'
    ]
    
    if not all(attr in body for attr in required_attributes):
        return response(400, f"Missing required attributes. Required: {', '.join(required_attributes)}")
    
    item = {
        'PK': partition_key,
        'SK': sort_key,
        **{attr: Decimal(str(body[attr])) for attr in required_attributes}
    }
    
    table.put_item(Item=item)
    return response(200, "Limit updated successfully")

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