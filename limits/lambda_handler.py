import json
from create_limit import create_limit
from read_limit import read_limit
from update_limit import update_limit
from delete_limit import delete_limit
from utils import response_lambda

def lambda_handler(event, context):
    try:
        path = event['path']
        method = event['httpMethod']
        
        if method not in ['GET', 'POST', 'PUT', 'DELETE']:
            return response_lambda(400, {"message": "Unsupported HTTP method"})

        if path == '/limits/account':
            return handle_limit(event, method, 'ACCOUNT')
        elif path == '/limits/account-application':
            return handle_limit(event, method, 'ACCOUNT_APPLICATION')
        elif path == '/limits/account-application-merchant':
            return handle_limit(event, method, 'ACCOUNT_APPLICATION_MERCHANT')
        elif path == '/limits/account-application-merchant-product':
            return handle_limit(event, method, 'ACCOUNT_APPLICATION_MERCHANT_PRODUCT')
        else:
            return response_lambda(404, {"message": "Not Found"})

    except Exception as e:
        print("An error occured ", e)
        return response_lambda(500, {"message": f"Error: {str(e)}"})

def handle_limit(event, method, limit_type):
    if method == 'GET':
        return read_limit(event, limit_type)
    elif method == 'POST':
        return create_limit(event, limit_type)
    elif method == 'PUT':
        return update_limit(event, limit_type)
    elif method == 'DELETE':
        return delete_limit(event, limit_type)