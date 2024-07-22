from utils import response, get_dynamodb_table

def delete_limit(event, limit_type):
    table = get_dynamodb_table()
    params = event['queryStringParameters'] or {}
    
    partition_key, sort_key = construct_keys(params, limit_type)
    
    if not partition_key or not sort_key:
        return response(400, "Missing required parameters")
    
    table.delete_item(Key={'PARTITION_KEY': partition_key, 'SORT_KEY': sort_key})
    return response(200, "Limit deleted successfully")

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