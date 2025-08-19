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
        product_data = _extract_payload(event)

        if not product_data or "merchantId" not in product_data or "productId" not in product_data:
            msg = "Invalid payload: missing merchantId or productId"
            print(msg)
            return {"statusCode": 400, "body": json.dumps(msg)}

        merchant_id = product_data["merchantId"]
        product_id = product_data["productId"]

        item_to_save = {
            "PARTITION_KEY": f"MERCHANT_PRODUCT#{merchant_id}",
            "SORT_KEY": f"PRODUCT#{product_id}",
            "merchantProductId": product_data.get("merchantProductId"),
            "merchantId": merchant_id,
            "productId": product_id,
            "merchantProductName": product_data.get("name"),
            "description": product_data.get("description"),
            "productName": product_data.get("productName"),
            "productCode": product_data.get("productCode"),
            "merchantProductCode": product_data.get("merchantProductCode"),
            "merchantName": product_data.get("merchantName"),
            "merchantCode": product_data.get("merchantCode"),
            "canSettle": product_data.get("canSettle"),
            "status": product_data.get("status"),
            "alias": product_data.get("alias"),
            "serviceCode": product_data.get("serviceCode"),
            "configuration": product_data.get("configuration"),
            "createdAt": product_data.get("createdAt"),
            "updatedAt": event.get("time") or product_data.get("updatedAt"),
        }

        tags = product_data.get("tags")
        if tags:
            item_to_save["tags"] = set(tags)

        item_to_save = {k: v for k, v in item_to_save.items() if v is not None}

        print(
            f"Saving to DynamoDB table {TABLE_NAME}: {json.dumps(item_to_save, default=json_serial)}"
        )
        table.put_item(Item=item_to_save)

        return {
            "statusCode": 200,
            "body": json.dumps(
                f"Successfully processed merchant product {product_data.get('merchantProductId')}"
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
