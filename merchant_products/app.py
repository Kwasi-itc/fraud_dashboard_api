import json
import os
import boto3
from botocore.exceptions import ClientError
from decimal import Decimal

# Initialize the DynamoDB client
dynamodb = boto3.resource("dynamodb")

TABLE_NAME = os.environ.get(
    "MERCHANT_PRODUCT_TABLE_NAME", "FraudPyV1MerchantProductNotificationTable"
)
table = dynamodb.Table(TABLE_NAME)


def json_serial(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, (set, frozenset)):
        # Convert DynamoDB string-set to JSON array
        return list(obj)
    raise TypeError(f"Type {type(obj)} not serializable")


def _extract_payload(event: dict) -> dict | None:
    """
    Extract JSON sent by API Gateway (preferred) or EventBridge.
    """
    body = event.get("body")
    if body:
        try:
            return json.loads(body)
        except json.JSONDecodeError:
            return None

    if isinstance(event.get("detail"), dict):
        return event["detail"]

    return None


def lambda_handler(event, context):
    """
    Lambda handler to create / update merchant-product information.

    When invoked through API Gateway the product JSON must be sent
    in the request body (POST /merchant-products). When invoked by EventBridge
    the product JSON must be placed under the *detail* key.
    """
    print(f"Received event: {json.dumps(event)}")

    # ---------- GET /merchant-products?merchantId={mId}&productId={pId} ----------
    if event.get("httpMethod") == "GET":
        qs = event.get("queryStringParameters") or {}
        merchant_id = qs.get("merchantId")
        product_id = qs.get("productId")
        if not merchant_id or not product_id:
            return {
                "statusCode": 400,
                "body": json.dumps("Missing merchantId or productId query parameters"),
            }
        try:
            resp = table.get_item(
                Key={
                    "PARTITION_KEY": f"MERCHANT_PRODUCT#{merchant_id}",
                    "SORT_KEY": f"PRODUCT#{product_id}",
                }
            )
            item = resp.get("Item")
            if not item:
                return {
                    "statusCode": 404,
                    "body": json.dumps("Merchant product not found"),
                }
            return {
                "statusCode": 200,
                "body": json.dumps(item, default=json_serial),
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
            return {"statusCode": 400, "body": json.dumps(msg)}

        # Accept a single object or an array of objects (max 1000 per request)
        product_records = payload if isinstance(payload, list) else [payload]

        if len(product_records) > 1000:
            return {
                "statusCode": 400,
                "body": json.dumps("Maximum 1000 product records allowed per request"),
            }

        # Validate mandatory keys for every record
        invalid_idx = [
            idx
            for idx, rec in enumerate(product_records)
            if "merchantId" not in rec or "productId" not in rec
        ]
        if invalid_idx:
            msg = (
                "Invalid payload: missing merchantId or productId "
                f"at indices {invalid_idx}"
            )
            print(msg)
            return {"statusCode": 400, "body": json.dumps(msg)}

        items_to_save: list[dict] = []
        for rec in product_records:
            merchant_id = rec["merchantId"]
            product_id = rec["productId"]

            item = {
                "PARTITION_KEY": f"MERCHANT_PRODUCT#{merchant_id}",
                "SORT_KEY": f"PRODUCT#{product_id}",
                "merchantProductId": rec.get("merchantProductId"),
                "merchantId": merchant_id,
                "productId": product_id,
                "merchantProductName": rec.get("name"),
                "description": rec.get("description"),
                "productName": rec.get("productName"),
                "productCode": rec.get("productCode"),
                "merchantProductCode": rec.get("merchantProductCode"),
                "merchantName": rec.get("merchantName"),
                "merchantCode": rec.get("merchantCode"),
                "canSettle": rec.get("canSettle"),
                "status": rec.get("status"),
                "alias": rec.get("alias"),
                "serviceCode": rec.get("serviceCode"),
                "configuration": rec.get("configuration"),
                "createdAt": rec.get("createdAt"),
                "updatedAt": event.get("time") or rec.get("updatedAt"),
            }

            tags = rec.get("tags")
            if tags:
                item["tags"] = set(tags)

            # Strip None values
            item = {k: v for k, v in item.items() if v is not None}
            items_to_save.append(item)

        # Batch-write to DynamoDB (batch_writer chunks into 25-item requests)
        with table.batch_writer() as batch:
            for itm in items_to_save:
                print(
                    f"Saving to DynamoDB table {TABLE_NAME}: "
                    f"{json.dumps(itm, default=json_serial)}"
                )
                batch.put_item(Item=itm)

        return {
            "statusCode": 200,
            "body": json.dumps(
                f"Successfully processed {len(items_to_save)} merchant product record(s)"
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
