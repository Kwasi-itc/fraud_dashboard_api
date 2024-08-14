from utils import response_lambda, get_dynamodb_table, decimal_default, alternate_response_lambda
import json
import boto3

def read_limit(event, limit_type):
    try:
        table = get_dynamodb_table()
        params = event['queryStringParameters'] or {}
        print("The params are ", params)

        partition_key, sort_key = construct_keys(params, limit_type)

        print("The partition key and sort key are ", partition_key, sort_key)

        if not partition_key:
            return response_lambda(400, {"message": "Missing required parameters"})
        
        stuff_to_send = []
        if sort_key:
            response = table.get_item(Key={'PARTITION_KEY': partition_key, 'SORT_KEY': sort_key})
            item = response.get('Item')
            if len(item) != 1:
                item = [item]
            for an_item in item:
                current_item = an_item
                current_item["account_id"] = ""
                current_item["application_id"] = ""
                current_item["merchant_id"] = ""
                current_item["product_id"] = ""
                if limit_type.lower() == "account":
                    current_item["account_id"] = current_item["SORT_KEY"]
                elif limit_type.lower() == "account-application":
                    current_item["account_id"] = current_item["SORT_KEY"].split("__")[0]
                    current_item["application_id"] = current_item["SORT_KEY"].split("__")[1]
                elif limit_type.lower() == "account-application-merchant":
                    current_item["account_id"] = current_item["SORT_KEY"].split("__")[0]
                    current_item["application_id"] = current_item["SORT_KEY"].split("__")[1]
                    current_item["merchant_id"] = current_item["SORT_KEY"].split("__")[2]
                elif limit_type.lower() == "account-application-merchant-product":
                    current_item["account_id"] = current_item["SORT_KEY"].split("__")[0]
                    current_item["application_id"] = current_item["SORT_KEY"].split("__")[1]
                    current_item["merchant_id"] = current_item["SORT_KEY"].split("__")[2]
                    current_item["product_id"] = current_item["SORT_KEY"].split("__")[3]
                stuff_to_send.append(current_item)                    

        else:
            response = table.query(KeyConditionExpression=boto3.dynamodb.conditions.Key('PARTITION_KEY').eq(partition_key))
            item = response.get('Items')
            for an_item in item:
                current_item = an_item
                current_item["account_id"] = ""
                current_item["application_id"] = ""
                current_item["merchant_id"] = ""
                current_item["product_id"] = ""
                if limit_type.lower() == "account":
                    current_item["account_id"] = current_item["SORT_KEY"]
                elif limit_type.lower() == "account-application":
                    current_item["account_id"] = current_item["SORT_KEY"].split("__")[0]
                    current_item["application_id"] = current_item["SORT_KEY"].split("__")[1]
                elif limit_type.lower() == "account-application-merchant":
                    current_item["account_id"] = current_item["SORT_KEY"].split("__")[0]
                    current_item["application_id"] = current_item["SORT_KEY"].split("__")[1]
                    current_item["merchant_id"] = current_item["SORT_KEY"].split("__")[2]
                elif limit_type.lower() == "account-application-merchant-product":
                    current_item["account_id"] = current_item["SORT_KEY"].split("__")[0]
                    current_item["application_id"] = current_item["SORT_KEY"].split("__")[1]
                    current_item["merchant_id"] = current_item["SORT_KEY"].split("__")[2]
                    current_item["product_id"] = current_item["SORT_KEY"].split("__")[3]
                stuff_to_send.append(current_item)

        print("The item is ", stuff_to_send)
        return alternate_response_lambda(200, json.dumps(stuff_to_send, default=decimal_default)) if stuff_to_send else response_lambda(404, {"message": "Limit not found"})
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