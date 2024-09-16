import boto3
from decimal import Decimal
import os
import json

def get_dynamodb_table():
    dynamodb = boto3.resource('dynamodb')
    table_name = os.environ["FRAUD_LIMITS_TABLE"]
    return dynamodb.Table(table_name)

def response_lambda(status_code, body):
    response_message = ""
    if status_code == 200:
        response_message = "Operation Successful"
    else:
        response_message = "Unsuccessful operation"
    body_to_send = {
        "responseCode": status_code,
        "responseMessage": response_message,
        "data": body
    }
    dictionary = {
        'statusCode': status_code,
        'body': json.dumps(body_to_send),
        'headers': {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Credentials': True,
        }
    }
    return dictionary


def alternate_response_lambda(status_code, body):
    response_message = ""
    if status_code == 200:
        response_message = "Operation Successful"
    else:
        response_message = "Unsuccessful operation"
    body_to_send = {
        "responseCode": status_code,
        "responseMessage": response_message,
        "data": body
    }
    dictionary = {
        'statusCode': status_code,
        'body': json.dumps(body_to_send, default=decimal_default),
        'headers': {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Credentials': True,
        }
    }
    return dictionary

def decimal_default(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError