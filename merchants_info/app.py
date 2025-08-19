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
            resp = table.get_item(Key={"PARTITION_KEY": "MERCHANT_INFO", "SORT_KEY": merchant_id})
            item = resp.get("Item")
            if not item:
                return {
                    "statusCode": 404,
                    "body": json.dumps("Merchant not found"),
                }
            return {
                "statusCode": 200,
                # Ensure any DynamoDB sets are returned as JSON arrays
                "body": json.dumps(item, default=list),
            }
        except ClientError as e:
            error_message = e.response["Error"]["Message"]
            print(f"DynamoDB ClientError: {error_message}")
            return {
                "statusCode": 500,
                "body": json.dumps(f"Error retrieving from DynamoDB: {error_message}"),
            }

    try:
        payload = _extract_payload(event)

        if not payload:
            msg = "Invalid payload: request body must be valid JSON"
            print(msg)
            return {
                "statusCode": 400,
                "body": json.dumps(msg),
            }

        # Accept single object or list (max 1000 records)
        merchant_records = payload if isinstance(payload, list) else [payload]

        if len(merchant_records) > 1000:
            return {
                "statusCode": 400,
                "body": json.dumps("Maximum 1000 merchant records allowed per request"),
            }

        # Validate every record contains an *id*
        invalid_idx = [idx for idx, rec in enumerate(merchant_records) if "id" not in rec]
        if invalid_idx:
            msg = f"Invalid payload: missing merchant id at indices {invalid_idx}"
            print(msg)
            return {
                "statusCode": 400,
                "body": json.dumps(msg),
            }

        items_to_save: list[dict] = []
        for rec in merchant_records:
            merchant_id = rec["id"]

            item = {
                "PARTITION_KEY": "MERCHANT_INFO",
                "SORT_KEY": merchant_id,
                "companyName": rec.get("companyName"),
                "code": rec.get("code"),
                "tradeName": rec.get("tradeName"),
                "alias": rec.get("alias"),
                "country": rec.get("country"),
                "tier": rec.get("tier"),
                "typeOfCompany": rec.get("typeOfCompany"),
                "status": rec.get("status"),
                "companyLogo": rec.get("companyLogo"),
                "companyRegistrationNumber": rec.get("companyRegistrationNumber"),
                "vatRegistrationNumber": rec.get("vatRegistrationNumber"),
                "dateOfIncorporation": rec.get("dateOfIncorporation"),
                "dateOfCommencement": rec.get("dateOfCommencement"),
                "taxIdentificationNumber": rec.get("taxIdentificationNumber"),
                "createdAt": rec.get("createdAt"),
                "updatedAt": rec.get("updatedAt"),
                "EntityType": "Merchant",
            }

            tags = rec.get("tags")
            if tags:
                item["tags"] = set(tags)

            # Remove None values
            item = {k: v for k, v in item.items() if v is not None}
            items_to_save.append(item)

        # Batch-write (batch_writer chunks into 25-item API calls)
        with table.batch_writer() as batch:
            for itm in items_to_save:
                print(f"Saving to DynamoDB table {TABLE_NAME}: {json.dumps(itm, default=list)}")
                batch.put_item(Item=itm)

        return {
            "statusCode": 200,
            "body": json.dumps(
                f"Successfully processed {len(items_to_save)} merchant record(s)"
            ),
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
