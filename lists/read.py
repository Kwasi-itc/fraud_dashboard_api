import json
import boto3
from botocore.exceptions import ClientError
from datetime import datetime
import os
from boto3.dynamodb.conditions import Key, Attr

dynamodb = boto3.resource('dynamodb')
table_name = os.environ["FRAUD_LISTS_TABLE"]
table = dynamodb.Table(table_name)

def lambda_handler(event, context):
    try:
        print("The event is ", event)
        path = event['path']
        print("The path is ", path)
        method = event['httpMethod']
        
        if method != 'GET':
            return response(400, "Only GET method is supported")

        if path == '/lists':
            return handle_specific_query(event)
        elif path == '/lists/by-list-type':
            return handle_list_type_query(event)
        elif path == '/lists/by-channel':
            return handle_channel_query(event)
        elif path == '/lists/by-entity-type':
            return handle_entity_type_query(event)
        elif path == '/lists/by-date-range':
            return handle_date_range_query(event)
        elif path == '/lists/by-list-type-and-entity-type':
            return handle_list_type_and_entity_type_query(event)
        else:
            return response(404, "Not Found")

    except Exception as e:
        print("An error occured ", e)
        return response(500, {"message": f"Error: {str(e)}"})

def handle_specific_query(event):
    params = event['queryStringParameters'] or {}
    list_type = params.get('list_type')
    channel = params.get('channel')
    entity_type = params.get('entity_type')
    account_id = params.get('account_id')
    application_id = params.get('application_id')
    merchant_id = params.get('merchant_id')
    product_id = params.get('product_id')
    
    entity_id = ""
    if entity_type == "ACCOUNT":
        entity_id = account_id
    elif entity_type == "APPLICATION":
        entity_id = application_id
    elif entity_type == "MERCHANT":
        entity_id = application_id + "__" + merchant_id
    elif entity_type == "PRODUCT":
        entity_id = application_id + "__" + merchant_id + "__" + product_id
    else:
        return response(400, json.dumps({"error": "Entity type must be ACCOUNT | APPLICATION | MERCHANT | PRODUCT"}))

    if not all([list_type, channel, entity_type]):
        return response(400, "Missing required parameters")

    items = query_specific(list_type, channel, entity_type, entity_id)
    return response(200, json.dumps(items))

def handle_list_type_query(event):
    params = event['queryStringParameters'] or {}
    list_type = params.get('list_type')

    if not list_type:
        return response(400, "List type is required")

    items = query_by_list_type(list_type)
    return response(200, json.dumps(items))

def handle_channel_query(event):
    params = event['queryStringParameters'] or {}
    channel = params.get('channel')

    if not channel:
        return response(400, "Channel is required")

    items = query_by_channel(channel)
    return response(200, json.dumps(items))

def handle_entity_type_query(event):
    params = event['queryStringParameters'] or {}
    entity_type = params.get('entity_type')

    if not entity_type:
        return response(400, "Entity type is required")

    items = query_by_entity_type(entity_type)
    return response(200, json.dumps(items))

def handle_list_type_and_entity_type_query(event):
    params = event['queryStringParameters'] or {}
    entity_type = params.get('entity_type')
    list_type = params.get('list_type')

    if not entity_type and not list_type:
        return response(400, "Both entity type and list type are required")
    
    items = query_by_list_and_entity_type(list_type, entity_type)
    return response(200, json.dumps(items))

def handle_date_range_query(event):
    params = event['queryStringParameters'] or {}
    start_date = params.get('start_date')
    end_date = params.get('end_date')

    if not start_date or not end_date:
        return response(400, "Both start_date and end_date are required")

    items = query_by_date_range(start_date, end_date)
    return response(200, json.dumps(items))

def query_specific(list_type, channel, entity_type, entity_id):
    partition_key = f"{list_type}-{channel}-{entity_type}"
    if entity_id:
        response = table.get_item(Key={'PARTITION_KEY': partition_key, 'SORT_KEY': entity_id})
        return [response.get('Item')] if 'Item' in response else []
    else:
        response = table.query(KeyConditionExpression=boto3.dynamodb.conditions.Key('PARTITION_KEY').eq(partition_key))
        return response.get('Items', [])

def query_by_list_type(list_type):
    response = table.scan(FilterExpression=boto3.dynamodb.conditions.Attr('PARTITION_KEY').begins_with(f"{list_type}-"))
    return response.get('Items', [])

def query_by_channel(channel):
    response = table.scan(FilterExpression=boto3.dynamodb.conditions.Attr('PARTITION_KEY').contains(f"-{channel}-"))
    return response.get('Items', [])

def query_by_entity_type(entity_type):
    response = table.scan(FilterExpression=boto3.dynamodb.conditions.Attr('PARTITION_KEY').contains(f"-{entity_type}"))
    items = response.get('Items', [])
    items_to_send = []
    for item in items:
        current_item = item
        if "ACCOUNT" in item["PARTITION_KEY"]:
            current_item["account_id"] = item["SORT_KEY"]
        elif "APPLICATION" in item["PARTITION_KEY"]:
            current_item["application_id"] = item["SORT_KEY"]
        elif "MERCHANT" in item["PARTITION_KEY"]:
            current_item["application_id"] = item["SORT_KEY"].split("__")[0]
            current_item["merchant_id"] = item["SORT_KEY"].split("__")[1]
        elif "PRODUCT" in item["PARTITION_KEY"]:
            current_item["application_id"] = item["SORT_KEY"].split("__")[0]
            current_item["merchant_id"] = item["SORT_KEY"].split("__")[1]
            current_item["product_id"] = item["SORT_KEY"].split("__")[2]
        items_to_send.append(current_item)
        
    return items_to_send

def query_by_list_and_entity_type(list_type, entity_type):
    possible_items = query_by_list_type(list_type)
    items = []
    for item in possible_items:
        if entity_type in item["PARTITION_KEY"]:
            items.append(item)
    items_to_send = []
    for item in items:
        current_item = item
        if "ACCOUNT" in item["PARTITION_KEY"]:
            current_item["account_id"] = item["SORT_KEY"]
        elif "APPLICATION" in item["PARTITION_KEY"]:
            current_item["application_id"] = item["SORT_KEY"]
        elif "MERCHANT" in item["PARTITION_KEY"]:
            current_item["application_id"] = item["SORT_KEY"].split("__")[0]
            current_item["merchant_id"] = item["SORT_KEY"].split("__")[1]
        elif "PRODUCT" in item["PARTITION_KEY"]:
            current_item["application_id"] = item["SORT_KEY"].split("__")[0]
            current_item["merchant_id"] = item["SORT_KEY"].split("__")[1]
            current_item["product_id"] = item["SORT_KEY"].split("__")[2]
        items_to_send.append(current_item)
        
    return items_to_send



def query_by_date_range(start_date, end_date):
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    response = table.scan(
        FilterExpression=boto3.dynamodb.conditions.Attr('created_at').between(start.isoformat(), end.isoformat())
    )
    return response.get('Items', [])

def response(status_code, body):
    return {
        'statusCode': status_code,
        'body': body,
        'headers': {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Credentials': True,
        },
    }