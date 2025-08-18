import json
import os
import boto3
from botocore.exceptions import ClientError
from decimal import Decimal

# Initialize the DynamoDB client
dynamodb = boto3.resource('dynamodb')

# Get the DynamoDB table name from an environment variable
TABLE_NAME = os.environ.get('MERCHANT_PRODUCT_TABLE_NAME', 'FraudPyV1MerchantProductNotificationTable')
table = dynamodb.Table(TABLE_NAME)

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError ("Type %s not serializable" % type(obj))

def lambda_handler(event, context):
    """
    This function is triggered by an EventBridge event for merchant products.
    It extracts the product details and saves them to a DynamoDB table.
    """
    print(f"Received event: {json.dumps(event)}")

    try:
        # 1. Extract the merchant product data from the event's 'detail' field
        product_data = event.get('detail')

        if not product_data or 'merchantId' not in product_data or 'productId' not in product_data:
            print("Error: Event detail is missing or does not contain required IDs.")
            return {
                'statusCode': 400,
                'body': json.dumps('Invalid event payload: Missing detail or required IDs.')
            }

        merchant_id = product_data['merchantId']
        product_id = product_data['productId']

        # 2. Construct the item to be saved in DynamoDB
        #    This structure matches the merchant product table design.
        item_to_save = {
            'PK': f'MERCHANT_PRODUCT#{merchant_id}',
            'SK': f'PRODUCT#{product_id}',
            'merchantProductId': product_data.get('merchantProductId'),
            'merchantId': merchant_id,
            'productId': product_id,
            'merchantProductName': product_data.get('name'), # Mapping 'name' from event to 'merchantProductName'
            'description': product_data.get('description'),
            'productName': product_data.get('productName'),
            'productCode': product_data.get('productCode'),
            'merchantProductCode': product_data.get('merchantProductCode'),
            'merchantName': product_data.get('merchantName'),
            'merchantCode': product_data.get('merchantCode'),
            'canSettle': product_data.get('canSettle'),
            'status': product_data.get('status'),
            'alias': product_data.get('alias'),
            'serviceCode': product_data.get('serviceCode'),
            'configuration': product_data.get('configuration'),
            'createdAt': product_data.get('createdAt'),
            'updatedAt': event.get('time') # Using the event time as the update time
        }
        
        # Handle the 'tags' attribute. DynamoDB String Sets cannot be empty.
        tags = product_data.get('tags')
        if tags and isinstance(tags, list) and len(tags) > 0:
            item_to_save['tags'] = set(tags)

        # Remove any keys with None values to keep the DynamoDB item clean
        item_to_save = {k: v for k, v in item_to_save.items() if v is not None}

        # 3. Save the item to the DynamoDB table
        print(f"Attempting to save item to DynamoDB: {json.dumps(item_to_save, default=json_serial)}")
        table.put_item(Item=item_to_save)
        
        print(f"Successfully saved merchant product {product_data.get('merchantProductId')} to table {TABLE_NAME}.")
        
        return {
            'statusCode': 200,
            'body': json.dumps(f"Successfully processed merchant product {product_data.get('merchantProductId')}")
        }

    except ClientError as e:
        # Handle potential DynamoDB errors
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        print(f"DynamoDB ClientError: {error_code} - {error_message}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error saving to DynamoDB: {error_message}')
        }
    except Exception as e:
        # Handle other unexpected errors
        print(f"An unexpected error occurred: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'An unexpected error occurred: {str(e)}')
        }

# --- Deployment Notes ---
#
# 1. IAM Role Permissions:
#    The Lambda function's execution role needs permissions for:
#    - AWSLambdaBasicExecutionRole (for CloudWatch Logs)
#    - dynamodb:PutItem on the target DynamoDB table resource.
#
# 2. Lambda Environment Variables:
#    - Key: MERCHANT_PRODUCT_TABLE_NAME
#    - Value: The name of your DynamoDB table (e.g., FraudPyV1MerchantProductNotificationTable)
#
# 3. EventBridge Rule:
#    - Create a rule in EventBridge to trigger this Lambda.
#    - The event pattern should match your merchant product notifications.
#      Example:
#      {
#          "source": ["aws.s3"],
#          "detail-type": ["merchant_product_notifications"]
#      }
