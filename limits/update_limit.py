import json
from decimal import Decimal
from utils import response_lambda, get_dynamodb_table

def update_limit(event, limit_type):
    try:
        table = get_dynamodb_table()
        params = event['queryStringParameters'] or {}
        params["application_id"] = params.get("processor")
        params["account_id"] = params.get("account_ref")
        body = json.loads(event['body'])
    
        partition_key, sort_key = construct_keys(params, limit_type)
    
        if not partition_key:
            return response_lambda(400, {"message": "Missing required parameters"})
    
        required_attributes = [
            'AMOUNT', 'HOURLY_SUM', 'DAILY_SUM', 'WEEKLY_SUM', 'MONTHLY_SUM',
            'HOURLY_COUNT', 'DAILY_COUNT', 'WEEKLY_COUNT', 'MONTHLY_COUNT'
        ]
    
        if not all(attr in body for attr in required_attributes):
            return response_lambda(400, {"message": f"Missing required attributes. Required: {', '.join(required_attributes)}"})
    
        item = {
            'PARTITION_KEY': partition_key,
            'SORT_KEY': sort_key,
            **{attr: Decimal(str(body[attr])) for attr in required_attributes}
        }
    
        response = table.put_item(Item=item)
        print("The response after putting item in the table is ", response)
        return response_lambda(200, {"message": "Limit updated successfully"})
    except Exception as e:
        print("An error occured ", e)
        return response_lambda(500, {"message": "An error occured " + e})

def construct_keys(params, limit_type):
    channel = params.get('channel')
    channel = channel.lower()
    if not channel:
        return None, None
    
    partition_key = f"LIMITS-{channel}-{limit_type}"
    sort_key = "__".join(filter(None, [
        #params.get('account_id'),
        params.get('application_id'),
        params.get('merchant_id'),
        params.get('product_id')
    ]))

    if limit_type.lower() == "account":
        sort_key = "-"
    
    return partition_key, sort_key