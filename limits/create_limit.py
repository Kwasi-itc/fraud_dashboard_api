import json
from decimal import Decimal
from utils import response_lambda, get_dynamodb_table

def create_limit(event, limit_type):
    try:
        table = get_dynamodb_table()
        params = event['queryStringParameters'] or {}
        body = json.loads(event['body'])
        print("The body is ", body)
        partition_key, sort_key = construct_keys(params, limit_type)
    
        if not partition_key:
            return response_lambda(400, {"message": "Missing required parameters"})
    
        required_attributes = [
            'AMOUNT', 'HOURLY_SUM', 'DAILY_SUM', 'WEEKLY_SUM', 'MONTHLY_SUM',
            'HOURLY_COUNT', 'DAILY_COUNT', 'WEEKLY_COUNT', 'MONTHLY_COUNT'
        ]

        print("I am here 1")
    
        if not all(attr in body for attr in required_attributes):
            return response_lambda(400, {"message" : f"Missing required attributes. Required: {', '.join(required_attributes)}"})
        
        print("I am here 2")
        item = {
            'PARTITION_KEY': partition_key,
            'SORT_KEY': sort_key,
            **{attr: Decimal(str(body[attr])) for attr in required_attributes}
        }
        print("I am here 3")
    
        table.put_item(Item=item)
        print("I am here 4")
        return response_lambda(200, {"message": "Limit created successfully"})
    except Exception as e:
        print("An error occured ", e)
        return response_lambda(500, {"message": "An error occured " + e})

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