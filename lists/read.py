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
    print("The params are ", params)
    if params == {}:
        return response(200, query_items_in_all_lists_sorted_by_date())
    list_type = params.get('list_type')
    channel = params.get('channel')
    if channel is not None:
        channel = channel.lower()
    entity_type = params.get('entity_type')
    account_id = params.get('account_ref')
    application_id = params.get('processor')
    merchant_id = params.get('merchant_id')
    product_id = params.get('product_id')
    
    entity_id = ""
    if entity_type == "ACCOUNT":
        entity_id = account_id
    elif entity_type == "APPLICATION" or entity_type == "PROCESSOR":
        entity_id = application_id
    elif entity_type == "MERCHANT":
        if application_id is not None and merchant_id is not None:
            entity_id = application_id + "__" + merchant_id
        else:
            entity_id = None
    elif entity_type == "PRODUCT":
        if application_id is not None and merchant_id is not None and product_id is not None:
            entity_id = application_id + "__" + merchant_id + "__" + product_id
        else:
            entity_id = None
    else:
        return response(400, {"error": "Entity type must be ACCOUNT | PROCESSOR | MERCHANT | PRODUCT"})

    if not all([list_type, channel, entity_type]):
        return response(400, "Missing required parameters")

    items = query_specific(list_type, channel, entity_type, entity_id)
    return response(200, items)


def handle_list_type_query(event):
    params = event['queryStringParameters'] or {}
    list_type = params.get('list_type')

    if not list_type:
        return response(400, "List type is required")

    items = query_by_list_type(list_type)
    return response(200, transform_items(items))

def handle_channel_query(event):
    params = event['queryStringParameters'] or {}
    channel = params.get('channel')
    entity_type = params.get('entity_type', "")

    if not channel:
        return response(400, "Channel is required")

    items = query_by_channel(channel, entity_type)
    return response(200, transform_items(items))

def handle_entity_type_query(event):
    params = event['queryStringParameters'] or {}
    entity_type = params.get('entity_type')

    if not entity_type:
        return response(400, "Entity type is required")

    items = query_by_entity_type(entity_type)
    return response(200, transform_items(items))

def handle_list_type_and_entity_type_query(event):
    params = event['queryStringParameters'] or {}
    entity_type = params.get('entity_type')
    list_type = params.get('list_type')

    if not entity_type and not list_type:
        return response(400, "Both entity type and list type are required")
    
    items = query_by_list_and_entity_type(list_type, entity_type)
    return response(200, items)

def handle_date_range_query(event):
    params = event['queryStringParameters'] or {}
    start_date = params.get('start_date')
    end_date = params.get('end_date')

    if not start_date or not end_date:
        return response(400, "Both start_date and end_date are required")
    try:
        # Parse dates and set time components
        start = datetime.strptime(start_date, "%Y-%m-%d")
        start = start.replace(hour=0, minute=0, second=0, microsecond=0)
        
        end = datetime.strptime(end_date, "%Y-%m-%d")
        end = end.replace(hour=23, minute=59, second=59, microsecond=999999)
        
        items = query_by_date_range(start, end)
        return response(200, items)
    except ValueError as e:
        return response(400, f"Invalid date format. Use YYYY-MM-DD: {str(e)}")


def query_items_in_all_lists_sorted_by_date():
    """
    Query all items from BLACKLIST, WATCHLIST, and STAFFLIST,
    transform them, and sort by created_at in descending order.
    
    Returns:
        list: Sorted list of transformed items
    """
    all_items = []
    list_types = ["BLACKLIST", "WATCHLIST", "STAFFLIST"]
    
    # Fetch items for each list type
    
    for list_type in list_types:
        list_type = list_type.upper()
        response = table.scan(
            FilterExpression=boto3.dynamodb.conditions.Attr('PARTITION_KEY').begins_with(f"{list_type}-")
        )
        items = response.get('Items', [])
        new_items = []
        for item in items:
            new_item = item
            #new_item["PARTITION_KEY"] = item["PARTITION_KEY"]
            #new_item["SORT_KEY"] = item["SORT_KEY"]
            if "created_at" not in item:
                new_item["created_at"] = "2020-01-01 00:00:00.000000"
            else:
                new_item["created_at"] = item["created_at"]
            new_items.append(new_item)
        all_items.extend(new_items)
    
    # Sort items by created_at in descending order
    sorted_items = sorted(
        all_items,
        key=lambda x: datetime.strptime(x['created_at'], "%Y-%m-%d %H:%M:%S.%f"),
        reverse=True
    )
    
    # Transform items (rename SORT_KEY to entity_id and remove PARTITION_KEY)
    return transform_items(sorted_items)

def query_specific(list_type, channel, entity_type, entity_id):
    partition_key = f"{list_type}-{channel}-{entity_type}"
    if entity_id:
        response = table.get_item(Key={'PARTITION_KEY': partition_key, 'SORT_KEY': entity_id})
        return transform_items([response.get('Item')]) if 'Item' in response else []
    else:
        response = table.query(KeyConditionExpression=boto3.dynamodb.conditions.Key('PARTITION_KEY').eq(partition_key))
        items = response.get('Items', [])
        return transform_items(items)

def query_by_list_type(list_type):
    response = table.scan(FilterExpression=boto3.dynamodb.conditions.Attr('PARTITION_KEY').begins_with(f"{list_type}-"))
    items = response.get('Items', [])
    return transform_items(items)

def query_by_channel(channel, entity_type):
    response = table.scan(FilterExpression=boto3.dynamodb.conditions.Attr('PARTITION_KEY').contains(f"-{channel}-"))
    if entity_type == "":
        return response.get('Items', [])        
    all_items = response.get('Items', [])
    filtered_items = []
    for item in all_items:
        if entity_type.upper() in item["PARTITION_KEY"]:
            filtered_items.append(item)
    return transform_items(filtered_items)#response.get('Items', [])

def query_by_entity_type(entity_type):
    response = table.scan(FilterExpression=boto3.dynamodb.conditions.Attr('PARTITION_KEY').contains(f"-{entity_type}"))
    items = response.get('Items', [])
    items_to_send = []
    for item in items:
        current_item = item
        if "ACCOUNT" in item["PARTITION_KEY"]:
            #current_item["account_id"] = item["SORT_KEY"]
            current_item["account_ref"] = item["SORT_KEY"]
        elif "APPLICATION" in item["PARTITION_KEY"]:
            #current_item["application_id"] = item["SORT_KEY"]
            current_item["processor"] = item["SORT_KEY"]
        elif "MERCHANT" in item["PARTITION_KEY"]:
            #current_item["application_id"] = item["SORT_KEY"].split("__")[0]
            current_item["processor"] = item["SORT_KEY"]
            current_item["merchant_id"] = item["SORT_KEY"].split("__")[1]
        elif "PRODUCT" in item["PARTITION_KEY"]:
            #current_item["application_id"] = item["SORT_KEY"].split("__")[0]
            current_item["processor"] = item["SORT_KEY"]
            current_item["merchant_id"] = item["SORT_KEY"].split("__")[1]
            current_item["product_id"] = item["SORT_KEY"].split("__")[2]
        items_to_send.append(current_item)
        
    return transform_items(items_to_send)

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
            current_item["account_ref"] = item["SORT_KEY"]
        elif "APPLICATION" in item["PARTITION_KEY"]:
            #current_item["application_id"] = item["SORT_KEY"]
            current_item["processor"] = item["SORT_KEY"]
        elif "MERCHANT" in item["PARTITION_KEY"]:
            #current_item["application_id"] = item["SORT_KEY"].split("__")[0]
            current_item["processor"] = item["SORT_KEY"]
            current_item["merchant_id"] = item["SORT_KEY"].split("__")[1]
        elif "PRODUCT" in item["PARTITION_KEY"]:
            #current_item["application_id"] = item["SORT_KEY"].split("__")[0]
            current_item["processor"] = item["SORT_KEY"]
            current_item["merchant_id"] = item["SORT_KEY"].split("__")[1]
            current_item["product_id"] = item["SORT_KEY"].split("__")[2]
        items_to_send.append(current_item)
        
    return transform_items(items_to_send)



def query_by_date_range(start_date, end_date):
    start_str = start_date.strftime("%Y-%m-%d %H:%M:%S.%f")
    end_str = end_date.strftime("%Y-%m-%d %H:%M:%S.%f")
    
    response = table.scan(
        FilterExpression=Attr('created_at').between(start_str, end_str)
    )
    items = response.get('Items', [])
    return transform_items(items)

def transform_items(items):
    """
    Transform a list of DynamoDB items by renaming SORT_KEY to entity_id.
    
    Args:
        items (list): List of dictionaries containing DynamoDB items
        
    Returns:
        list: New list of transformed items
    """
    transformed_items = []
    
    for item in items:
        # Create a new dictionary for the transformed item
        transformed_item = item.copy()
        transformed_item["account_ref"] = ""
        transformed_item["processor"] = ""
        transformed_item["merchant_id"] = ""
        transformed_item["product_id"] = ""
        
        # Get the SORT_KEY value and remove the original key
        if 'SORT_KEY' in transformed_item:
            transformed_item['entity_id'] = transformed_item.pop('SORT_KEY')
        if 'PARTITION_KEY' in transformed_item:
            partition_key = transformed_item["PARTITION_KEY"]
            transformed_item["list_type"] = partition_key.split("-")[0]
            transformed_item["channel"] = partition_key.split("-")[1]
            transformed_item["entity_type"] = partition_key.split("-")[2]
            if transformed_item["entity_type"] == "ACCOUNT":
                transformed_item["account_ref"] = transformed_item["entity_id"]
            if transformed_item["entity_type"] == "APPLICATION":
                transformed_item["entity_type"] = "PROCESSOR"
                transformed_item["processor"] = transformed_item["entity_id"]
            if transformed_item["entity_type"] == "MERCHANT":
                transformed_item["processor"] = transformed_item["entity_id"].split("__")[0]
                transformed_item["merchant_id"] = transformed_item["entity_id"].split("__")[1]
            if transformed_item["entity_type"] == "PRODUCT":
                transformed_item["processor"] = transformed_item["entity_id"].split("__")[0]
                transformed_item["merchant_id"] = transformed_item["entity_id"].split("__")[1]
                transformed_item["product_id"] = transformed_item["entity_id"].split("__")[2]
            
            transformed_item.pop("PARTITION_KEY")   
        transformed_items.append(transformed_item)
        
    return transformed_items


def response(status_code, body):
    response_message = "Operation Successful" if status_code == 200 else "Unsuccessful operation"
    body_to_send = {
        "responseCode": status_code,
        "responseMessage": response_message,
        "data": body
    }
    return {
        'statusCode': status_code,
        'body': json.dumps(body_to_send),
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Credentials': True,
        },
    }


### "remove partition_key" and then change sort_key into entity_id
### total lists limited by number normal hitting of API
### return back after adding/updating
### assigned_to return entire json