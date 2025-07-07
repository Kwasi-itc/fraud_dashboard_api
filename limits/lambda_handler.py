import json
from create_limit import create_limit
from read_limit import read_limit
from delete_limit import delete_limit
from utils import response_lambda
from update_limit import update_limit

def lambda_handler(event, context):
    try:
        print("The event is ", event)
        path = event['path']
        print("The path is ", path)
        method = event['httpMethod']
        
        if method not in ['GET', 'POST', 'PUT', 'DELETE']:
            return response_lambda(400, {"message": "Unsupported HTTP method"})
        
        #path = path.replace("processor", "application")

        print("I am here 1")
        if path == '/limits/account':
            print("I am here 2")
            return handle_limit(event, method, 'ACCOUNT')
        elif path == '/limits/account-processor':
            return handle_limit(event, method, 'ACCOUNT_APPLICATION')
        elif path == '/limits/account-processor-merchant':
            return handle_limit(event, method, 'ACCOUNT_APPLICATION_MERCHANT')
        elif path == '/limits/account-processor-merchant-product':
            return handle_limit(event, method, 'ACCOUNT_APPLICATION_MERCHANT_PRODUCT')
        else:
            return response_lambda(404, {"message": "Not Found"})

    except Exception as e:
        print("An error occured ", e)
        return response_lambda(500, {"message": f"Error: {str(e)}"})

def handle_limit(event, method, limit_type):
    print("I am here 3")
    if method == 'GET':
        return read_limit(event, limit_type)
    elif method == 'POST':
        return create_limit(event, limit_type)
    elif method == 'PUT':
        return update_limit(event, limit_type)
    elif method == 'DELETE':
        return delete_limit(event, limit_type)


