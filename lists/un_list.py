import json
import requests
import xml.etree.ElementTree as ET
import boto3
import os
import datetime

# Get the DynamoDB table name from an environment variable
table_name = os.environ["FRAUD_LISTS_TABLE"]
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(table_name)

def lambda_handler(event, context):
    """
    Main Lambda handler to route requests based on HTTP method.
    """
    http_method = event.get('httpMethod')

    if http_method == 'POST':
        return handle_post()
    elif http_method == 'GET':
        return handle_get(event)
    else:
        return {
            'statusCode': 405,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'message': 'Method Not Allowed'})
        }

def handle_post():
    """
    Handles POST requests: fetches XML, parses it, and stores two records
    per individual into DynamoDB.
    """
    url = "https://scsanctions.un.org/resources/xml/en/consolidated.xml"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'message': f'Failed to download XML: {str(e)}'})
        }

    root = ET.fromstring(response.content)
    total_items_added = 0

    # Use a DynamoDB batch writer for efficient writes
    with table.batch_writer() as batch:
        for individual in root.findall(".//INDIVIDUAL"):
            firstname = individual.findtext("FIRST_NAME", default="").strip()
            lastname = individual.findtext("SECOND_NAME", default="").strip()
            
            # --- Description gathering logic (same as your script) ---
            description = {}
            for child in individual:
                key = child.tag.lower()
                if len(child) > 0:
                    subdict = {}
                    for sub in child:
                        if sub.text and sub.text.strip():
                            subdict[sub.tag.lower()] = sub.text.strip()
                    if subdict:
                        description[key] = subdict
                elif child.text and child.text.strip():
                    description[key] = child.text.strip()
            
            # --- Create the two dictionary items ---
            partition_key_main = "UNLIST-ALL-ACCOUNT"
            sort_key_main = f"{firstname.lower()}-{lastname.lower()}"

            item1 = {
                "PARTITION_KEY": partition_key_main,
                "SORT_KEY": sort_key_main,
                "created_at": str(datetime.datetime.now()),
                "list_type": "UNLIST",
                "channel": "ALL",
                "entity_type": "ACCOUNT",
                "description": description
            }
            
            item2 = {
                "PARTITION_KEY": "LIST_TYPE",
                "SORT_KEY": sort_key_main + "###" + partition_key_main,
                "description": description
            }
            
            # Add both items to the batch
            batch.put_item(Item=item1)
            batch.put_item(Item=item2)
            total_items_added += 1

    return {
        'statusCode': 201,
        'headers': {'Content-Type': 'application/json'},
        'body': json.dumps({'message': f'Successfully added {total_items_added} items to DynamoDB.'})
    }

def handle_get(event):
    """
    Handles GET requests to retrieve data from DynamoDB based on PARTITION_KEY.
    """
    #query_params = event.get('queryStringParameters')
    
    #if not query_params or 'PARTITION_KEY' not in query_params:
    #    return {
    #        'statusCode': 400,
    #        'headers': {'Content-Type': 'application/json'},
    #        'body': json.dumps({'message': 'Missing required query parameter: PARTITION_KEY'})
    #    }

    partition_key = "UNLIST-ALL-ACCOUNT"  # Default partition key for this example

    try:
        response = table.query(
            KeyConditionExpression=boto3.dynamodb.conditions.Key('PARTITION_KEY').eq(partition_key)
        )
        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps(response.get('Items', []))
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'message': f'Error querying DynamoDB: {str(e)}'})
        }
