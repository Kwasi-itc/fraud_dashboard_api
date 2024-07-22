import boto3
from decimal import Decimal
import os

def get_dynamodb_table():
    dynamodb = boto3.resource('dynamodb')
    table_name = os.environ["FRAUD_LIMITS_TABLE"]
    return dynamodb.Table(table_name)

def response_lambda(status_code, body):
    return {
        'statusCode': status_code,
        'body': body,
        'headers': {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Credentials': True,
        },
    }

def decimal_default(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError