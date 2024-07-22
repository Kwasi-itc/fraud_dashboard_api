from utils import response_lambda, get_dynamodb_table, decimal_default
import json
import boto3

def read_limit(event, limit_type):
    try:
        table = get_dynamodb_table()
        params = event['queryStringParameters'] or {}
        print("The params are ", params)

        print("I am here 1")
        partition_key, sort_key = construct_keys(params, limit_type)

        print("The partition key and sort key are ", partition_key, sort_key)
        print("I am here 2")

        if not partition_key:
            return response_lambda(400, {"message": "Missing required parameters"})
    
        if sort_key:
            response = table.get_item(Key={'PARTITION_KEY': partition_key, 'SORT_KEY': sort_key})
            item = response.get('Item')
        else:
            response = table.query(KeyConditionExpression=boto3.dynamodb.conditions.Key('PARTITION_KEY').eq(partition_key))
            item = response.get('Items')

        print("The item is ", item)
        print("I am here 3")
        return response(200, json.dumps(item, default=decimal_default)) if item else response(404, "Limit not found")
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