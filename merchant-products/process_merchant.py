import json
import os
import boto3
from botocore.exceptions import ClientError

# Initialize the DynamoDB client
dynamodb = boto3.resource('dynamodb')

# Get the DynamoDB table name from an environment variable for flexibility
TABLE_NAME = os.environ.get('MERCHANT_TABLE_NAME', 'YourMerchantTableName')
table = dynamodb.Table(TABLE_NAME)

def lambda_handler(event, context):
    """
    This function is triggered by an EventBridge event. It extracts merchant
    details from the event and saves them to a DynamoDB table.
    """
    print(f"Received event: {json.dumps(event)}")

    try:
        # 1. Extract the merchant data from the event's 'detail' field
        merchant_data = event.get('detail')

        if not merchant_data or 'id' not in merchant_data:
            print("Error: Event detail is missing or does not contain an 'id'.")
            return {
                'statusCode': 400,
                'body': json.dumps('Invalid event payload: Missing detail or id.')
            }

        merchant_id = merchant_data['id']

        # 2. Construct the item to be saved in DynamoDB
        #    This structure matches the design in the provided markdown document.
        item_to_save = {
            'PK': 'MERCHANT_INFO',  # Static Partition Key as per the design
            'SK': merchant_id, # Sort Key is the unique merchant ID
            'companyName': merchant_data.get('companyName'),
            'code': merchant_data.get('code'),
            'tradeName': merchant_data.get('tradeName'),
            'alias': merchant_data.get('alias'),
            'country': merchant_data.get('country'),
            'tier': merchant_data.get('tier'),
            'typeOfCompany': merchant_data.get('typeOfCompany'),
            'status': merchant_data.get('status'),
            'companyLogo': merchant_data.get('companyLogo'),
            'companyRegistrationNumber': merchant_data.get('companyRegistrationNumber'),
            'vatRegistrationNumber': merchant_data.get('vatRegistrationNumber'),
            'dateOfIncorporation': merchant_data.get('dateOfIncorporation'),
            'dateOfCommencement': merchant_data.get('dateOfCommencement'),
            'taxIdentificationNumber': merchant_data.get('taxIdentificationNumber'),
            'createdAt': merchant_data.get('createdAt'),
            'updatedAt': merchant_data.get('updatedAt'),
            'EntityType': 'Merchant' # Helper attribute for single-table design
        }
        
        # Handle the 'tags' attribute. DynamoDB String Sets cannot be empty.
        tags = merchant_data.get('tags')
        if tags and isinstance(tags, list) and len(tags) > 0:
            item_to_save['tags'] = set(tags)

        # Remove any keys with None values to keep the DynamoDB item clean
        item_to_save = {k: v for k, v in item_to_save.items() if v is not None}

        # 3. Save the item to the DynamoDB table
        print(f"Attempting to save item to DynamoDB: {json.dumps(item_to_save)}")
        table.put_item(Item=item_to_save)
        
        print(f"Successfully saved merchant {merchant_id} to table {TABLE_NAME}.")
        
        return {
            'statusCode': 200,
            'body': json.dumps(f'Successfully processed merchant {merchant_id}')
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
#    The Lambda function's execution role needs the following IAM permissions:
#    - AWSLambdaBasicExecutionRole (for CloudWatch Logs)
#    - dynamodb:PutItem on the target DynamoDB table resource.
#      Example Policy:
#      {
#          "Version": "2012-10-17",
#          "Statement": [
#              {
#                  "Effect": "Allow",
#                  "Action": "dynamodb:PutItem",
#                  "Resource": "arn:aws:dynamodb:REGION:ACCOUNT_ID:table/YourMerchantTableName"
#              }
#          ]
#      }
#
# 2. Lambda Environment Variables:
#    - Key: MERCHANT_TABLE_NAME
#    - Value: The name of your DynamoDB table (e.g., FraudPyV1MerchantProductsTable)
#
# 3. EventBridge Rule:
#    - Create a rule in EventBridge.
#    - For the event pattern, use something like this to match the events:
#      {
#          "source": ["aws.s3"],
#          "detail-type": ["merchant_product_notifications"]
#      }
#    - Set the target of the rule to be this Lambda function.
