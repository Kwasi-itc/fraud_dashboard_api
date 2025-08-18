from utils import response_lambda, get_dynamodb_table

def delete_limit(event, limit_type):
    try:
        table = get_dynamodb_table()
        params = event['queryStringParameters'] or {}
        params["application_id"] = params.get("processor")
        params["account_id"] = params.get("account_ref")
    
        partition_key, sort_key = construct_keys(params, limit_type)
    
        if not partition_key or not sort_key:
            return response_lambda(400, {"message": "Missing required parameters"})
    
        table.delete_item(Key={'PARTITION_KEY': partition_key, 'SORT_KEY': sort_key})
        return response_lambda(200, {"message": "Limit deleted successfully"})
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