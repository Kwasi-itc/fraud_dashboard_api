import json
import os
import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
from typing import Optional, List

# Initialize the DynamoDB client
dynamodb = boto3.resource('dynamodb')

# Get the DynamoDB table name from an environment variable for flexibility
table = dynamodb.Table(os.environ['FRAUD_PROCESSED_TRANSACTIONS_TABLE'])


def _extract_payload(event: dict) -> Optional[dict]:
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
        qs = event.get("queryStringParameters") or {}
        merchant_id = qs.get("id")

        # ---------- GET /merchants?id={merchantId} ----------
        if merchant_id:
            try:
                resp = table.get_item(
                    Key={"PARTITION_KEY": "MERCHANT_INFO", "SORT_KEY": merchant_id},
                    ConsistentRead=True,
                )
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

        # ---------- GET /merchants?all=true ----------
        if qs.get("all") == "true":
            try:
                resp = table.query(
                    KeyConditionExpression=Key("PARTITION_KEY").eq("MERCHANT_INFO"),
                    Limit=100,
                    ConsistentRead=True,
                )
                items = resp.get("Items", [])
                print(f"Retrieved {len(items)} merchant record(s) (logging up to 100):")
                for itm in items[:100]:
                    print(json.dumps(itm, default=list))

                return {
                    "statusCode": 200,
                    "body": json.dumps(items, default=list),
                }
            except ClientError as e:
                error_message = e.response["Error"]["Message"]
                print(f"DynamoDB ClientError: {error_message}")
                return {
                    "statusCode": 500,
                    "body": json.dumps(f"Error retrieving from DynamoDB: {error_message}"),
                }

        # No recognised query parameters supplied
        return {
            "statusCode": 400,
            "body": json.dumps("Missing 'id' query parameter or 'all=true'"),
        }

    # ---------- DELETE /merchants?deleteAll=true ----------
    # Removes every item whose PARTITION_KEY equals "MERCHANT_INFO"
    if event.get("httpMethod") == "DELETE":
        qs = event.get("queryStringParameters") or {}
        if qs.get("deleteAll") != "true":
            return {
                "statusCode": 400,
                "body": json.dumps('To delete all merchant records pass ?deleteAll=true'),
            }
        try:
            deleted = 0
            last_evaluated_key = None
            while True:
                scan_kwargs = {
                    "KeyConditionExpression": Key("PARTITION_KEY").eq("MERCHANT_INFO"),
                    "ProjectionExpression": "PARTITION_KEY, SORT_KEY",
                }
                if last_evaluated_key:
                    scan_kwargs["ExclusiveStartKey"] = last_evaluated_key

                resp = processed_table.query(**scan_kwargs)
                items = resp.get("Items", [])
                if not items:
                    break

                with processed_table.batch_writer() as batch:
                    for itm in items:
                        batch.delete_item(
                            Key={
                                "PARTITION_KEY": itm["PARTITION_KEY"],
                                "SORT_KEY": itm["SORT_KEY"],
                            }
                        )
                        deleted += 1

                last_evaluated_key = resp.get("LastEvaluatedKey")
                if not last_evaluated_key:
                    break

            return {
                "statusCode": 200,
                "body": json.dumps(f"Deleted {deleted} merchant record(s)"),
            }
        except ClientError as e:
            err_msg = e.response["Error"]["Message"]
            print("DynamoDB ClientError while deleting:", err_msg)
            return {"statusCode": 500, "body": json.dumps(err_msg)}
        except Exception as ex:
            print("Unexpected error while deleting:", str(ex))
            return {"statusCode": 500, "body": json.dumps(str(ex))}

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

        items_to_save: List[dict] = []
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

        # Batch-write (batch_writer chunks into 25-item API calls) – if the same
        # partition/sort key appears twice in the payload we overwrite instead of
        # triggering a “duplicate keys” validation error from DynamoDB.
        with table.batch_writer(overwrite_by_pkeys=["PARTITION_KEY", "SORT_KEY"]) as batch:
            for idx, itm in enumerate(items_to_save):
                # Log only the first 5 records to avoid excessive CloudWatch noise
                if idx < 5:
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
