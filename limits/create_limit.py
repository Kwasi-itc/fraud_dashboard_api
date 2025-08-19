import json
from decimal import Decimal
from utils import response_lambda, get_dynamodb_table

def create_limit(event, limit_type):
    try:
        print("I am here 4")
        table = get_dynamodb_table()
        params = event['queryStringParameters'] or {}
        params["account_id"] = params.get("account_ref")
        params["application_id"] = params.get("processor")
        body = json.loads(event['body'])
        print("The body is ", body)
        print("I am here 6")
        partition_key, sort_key = construct_keys(params, limit_type)
    
        if not partition_key:
            return response_lambda(400, {"message": "Missing required parameters"})
    
        required_attributes = [
            'AMOUNT', 'HOURLY_SUM', 'DAILY_SUM', 'WEEKLY_SUM', 'MONTHLY_SUM',
            'HOURLY_COUNT', 'DAILY_COUNT', 'WEEKLY_COUNT', 'MONTHLY_COUNT'
        ]

    
        if not all(attr in body for attr in required_attributes):
            return response_lambda(400, {"message" : f"Missing required attributes. Required: {', '.join(required_attributes)}"})
        
        # ------------------------------------------------------------------
        # Check whether a limit with the same partition & sort key exists.
        # If it does, abort and return an error to the caller.
        # ------------------------------------------------------------------
        try:
            existing_item = table.get_item(
                Key={
                    "PARTITION_KEY": partition_key,
                    "SORT_KEY": sort_key
                }
            ).get("Item")
        except Exception as db_err:
            # Unexpected error while querying – surface to caller
            print("Error while checking for existing limit:", db_err)
            return response_lambda(500, {"message": "Failed to verify existing limit"})
        
        if existing_item:
            return response_lambda(400, {"message": "Limit already exists"})
        
        item = {
            'PARTITION_KEY': partition_key,
            'SORT_KEY': sort_key,
            **{attr: Decimal(str(body[attr])) for attr in required_attributes}
        }
    
        table.put_item(Item=item)
        print("I am here 9")
        return response_lambda(200, {"message": "Limit created successfully"})
    except Exception as e:
        print("An error occured ", e)
        return response_lambda(500, {"message": "An error occured " + str(e)})

def construct_keys(params, limit_type):
    channel = params.get('channel')
    channel = channel.lower()
    if not channel:
        return None, None
    print("I am here 7")
    partition_key = f"LIMITS-{channel}-{limit_type}"
    sort_key = "__".join(filter(None, [
        params.get('application_id'),
        params.get('merchant_id'),
        params.get('product_id')
    ]))

    print("I am here 8")
    if limit_type.lower() == "account":
        sort_key = "-"
    
    return partition_key, sort_key
