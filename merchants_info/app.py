import json
import os
import boto3
from botocore.exceptions import ClientError

# Initialize the DynamoDB client
dynamodb = boto3.resource('dynamodb')

# Get the DynamoDB table name from an environment variable for flexibility
TABLE_NAME = os.environ.get('MERCHANT_TABLE_NAME', 'FraudPyV1MerchantsTable')
table = dynamodb.Table(TABLE_NAME)


def _extract_payload(event: dict) -> dict | None:
    """
    Extract JSON sent by API Gateway (preferred) or EventBridge.

    For REST API calls the payload must be in *event["body"]* as JSON.
    """
    # API Gateway invocation
    body = event.get("body")
    if body:
        try:
            return json.loads(body)
        except json.JSONDecodeError:
            return None

    # EventBridge fallback
    if isinstance(event.get("detail"), dict):
        return event["detail"]

    return None


def lambda_handler(event, context):
    """
    Lambda handler to create / update merchant information.

    When invoked through API Gateway the merchant JSON must be sent
    in the request body (POST /merchants). When invoked by EventBridge
    the merchant JSON must be placed under the *detail* key.
    """
    print(f"Received event: {json.dumps(event)}")

    # ---------- GET /merchants?id={merchantId} ----------
    if event.get("httpMethod") == "GET":
        merchant_id = (event.get("queryStringParameters") or {}).get("id")
        if not merchant_id:
            return {
                "statusCode": 400,
                "body": json.dumps("Missing id query parameter"),
            }
        try:
            resp = table.get_item(Key={"PK": "MERCHANT_INFO", "SK": merchant_id})
            item = resp.get("Item")
            if not item:
                return {
                    "statusCode": 404,
                    "body": json.dumps("Merchant not found"),
                }
            return {
                "statusCode": 200,
                "body": json.dumps(item, default=str),
            }
        except ClientError as e:
            error_message = e.response["Error"]["Message"]
            print(f"DynamoDB ClientError: {error_message}")
            return {
                "statusCode": 500,
                "body": json.dumps(f"Error retrieving from DynamoDB: {error_message}"),
            }

    try:
        merchant_data = _extract_payload(event)

        if not merchant_data or "id" not in merchant_data:
            msg = "Invalid payload: missing merchant *id*"
            print(msg)
            return {
                "statusCode": 400,
                "body": json.dumps(msg),
            }

        merchant_id = merchant_data["id"]

        item_to_save = {
            "PK": "MERCHANT_INFO",
            "SK": merchant_id,
            "companyName": merchant_data.get("companyName"),
            "code": merchant_data.get("code"),
            "tradeName": merchant_data.get("tradeName"),
            "alias": merchant_data.get("alias"),
            "country": merchant_data.get("country"),
            "tier": merchant_data.get("tier"),
            "typeOfCompany": merchant_data.get("typeOfCompany"),
            "status": merchant_data.get("status"),
            "companyLogo": merchant_data.get("companyLogo"),
            "companyRegistrationNumber": merchant_data.get("companyRegistrationNumber"),
            "vatRegistrationNumber": merchant_data.get("vatRegistrationNumber"),
            "dateOfIncorporation": merchant_data.get("dateOfIncorporation"),
            "dateOfCommencement": merchant_data.get("dateOfCommencement"),
            "taxIdentificationNumber": merchant_data.get("taxIdentificationNumber"),
            "createdAt": merchant_data.get("createdAt"),
            "updatedAt": merchant_data.get("updatedAt"),
            "EntityType": "Merchant",
        }

        # Handle optional tags (DynamoDB string-set cannot be empty)
        tags = merchant_data.get("tags")
        if tags:
            item_to_save["tags"] = set(tags)

        # Strip None values
        item_to_save = {k: v for k, v in item_to_save.items() if v is not None}

        print(f"Saving to DynamoDB table {TABLE_NAME}: {json.dumps(item_to_save)}")
        table.put_item(Item=item_to_save)

        return {
            "statusCode": 200,
            "body": json.dumps(f"Successfully processed merchant {merchant_id}"),
        }

    except ClientError as e:
        error_message = e.response["Error"]["Message"]
        print(f"DynamoDB ClientError: {error_message}")
        return {
            "statusCode": 500,
            "body": json.dumps(f"Error saving to DynamoDB: {error_message}"),
        }
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps(f"Unexpected error: {str(e)}"),
        }
