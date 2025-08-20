import os
import json
import boto3
from boto3.dynamodb.conditions import Key, Attr
from datetime import datetime, timedelta
import re
import math
import base64

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.environ['FRAUD_PROCESSED_TRANSACTIONS_TABLE'])

# ---------------------------------------------------------------------------
# Merchant/Product metadata look-ups
# ---------------------------------------------------------------------------
# Re-use the same processed-transactions table for metadata look-ups
merchant_product_table = table

# In-memory cache for one-invocation reuse
_MERCHANT_PRODUCT_CACHE = {}


def get_merchant_product_data(merchant_id: str, product_id: str) -> dict:
    """
    Resolve *merchant_id* and *product_id* into human-readable names by querying
    FraudPyV1ProcessedTransactionsTable.

    1. merchantName  ← companyName field of the MERCHANT_INFO item  
       Key:  PARTITION_KEY = "MERCHANT_INFO",            SORT_KEY = <merchant_id>

    2. productName & merchantProductName  ← PRODUCT item under the merchant  
       Key:  PARTITION_KEY = "MERCHANT_PRODUCT#<merchant_id>",  
             SORT_KEY      = "PRODUCT#<product_id>"

    Results are cached for the lifetime of the Lambda invocation to minimise
    DynamoDB calls.
    """
    key = (merchant_id, product_id)
    if key in _MERCHANT_PRODUCT_CACHE:
        return _MERCHANT_PRODUCT_CACHE[key]

    result: dict = {}

    # ------------------------------------------------------------------ #
    # 1. Fetch merchant (company) name
    # ------------------------------------------------------------------ #
    try:
        resp = merchant_product_table.get_item(
            Key={
                "PARTITION_KEY": "MERCHANT_INFO",
                "SORT_KEY": merchant_id,
            },
            ProjectionExpression="companyName",
        )
        result["merchantName"] = resp.get("Item", {}).get("companyName", "")
    except Exception as err:
        # Fail gracefully but log for debugging
        print("Error fetching merchant info:", err)

    # ------------------------------------------------------------------ #
    # 2. Fetch product names for this merchant/product pair
    # ------------------------------------------------------------------ #
    try:
        resp = merchant_product_table.query(
            KeyConditionExpression=Key("PARTITION_KEY").eq("MERCHANT_PRODUCT"),
            FilterExpression=Attr("merchantId").eq(merchant_id) & Attr("productId").eq(product_id),
            ProjectionExpression="productName, merchantProductName",
            Limit=1
        )
        items = resp.get("Items", [])
        if items:
            result["productName"] = items[0].get("productName", "")
            result["merchantProductName"] = items[0].get("merchantProductName", "")
    except Exception as err:
        print("Error fetching merchant product info:", err)

    # Cache and return
    _MERCHANT_PRODUCT_CACHE[key] = result
    return result

PAGE_SIZE = 20  # Default page size

def parse_key(key, account_id, application_id, merchant_id, product_id):
    parts = key.split('-')
    channel = parts[1]
    entities = parts[2].split('_')
    time_info = parts[-1].split('-')
    
    year = ""
    period = ""
    if "MONTH" in key:
        period = "MONTH"
        year = key.split("MONTH")[1].split("-")[1]
    elif "WEEK" in key:
        period = "WEEK"
        year = key.split("WEEK")[1].split("-")[1]
    elif "DAY" in key:
        period = "DAY"
        year = key.split("DAY")[1].split("-")[1]
    elif "HOUR" in key:
        period = "HOUR"
        year = key.split("HOUR")[1].split("-")[1]
        
    result = {
        "channel": channel,
        'account_ref': account_id,
        "processor": application_id,
        "merchant_id": merchant_id,
        "product_id": product_id,
        "period": period,
        "year": year,
        "month": "",
        "week": "",
        "day": "",
        "hour": ""
    }
    
    if result["period"] == "MONTH":
        result["month"] = key.split("MONTH")[1].split("-")[2]
    elif result["period"] == "WEEK":
        result["week"] = key.split("WEEK")[1].split("-")[2]
    elif result["period"] == "DAY":
        result["month"] = key.split("DAY")[1].split("-")[2]
        result["day"] = key.split("DAY")[1].split("-")[3]
    elif result["period"] == "HOUR":
        result["month"] = key.split("HOUR")[1].split("-")[2]
        result["day"] = key.split("HOUR")[1].split("-")[3]
        result["hour"] = key.split("HOUR")[1].split("-")[4]
    return result

def transform_aggregates(relevant_aggregates, account_id, application_id, merchant_id, product_id):
    result = {}
    for key, value in relevant_aggregates.items():
        parsed = parse_key(key, account_id, application_id, merchant_id, product_id)
        category = ""
        if "ACCOUNT" in key:
            category = "ACCOUNT"
        if "APPLICATION" in key:
            category = "ACCOUNT_APPLICATION"
        if "MERCHANT" in key:
            category = "ACCOUNT_APPLICATION_MERCHANT"
        if "PRODUCT" in key:
            category = "ACCOUNT_APPLICATION_MERCHANT_PRODUCT"
        
        if category == "ACCOUNT":
            parsed["processor"] = ""
            parsed['merchant_id'] = ""
            parsed['product_id'] = ""
        elif category == "ACCOUNT_APPLICATION":
            parsed['merchant_id'] = ""
            parsed['product_id'] = ""
        elif category == "ACCOUNT_APPLICATION_MERCHANT":
            parsed['product_id'] = ""

        if category not in result:
            result[category] = []
        
        entry = {
            "COUNT": value["COUNT"],
            "VERSION": value["VERSION"],
            "SUM": value["SUM"],
            "account_ref": parsed["account_ref"],
            "processor": parsed["processor"],
            "merchant_id": parsed["merchant_id"],
            "product_id": parsed["product_id"],
            "period": parsed["period"],
            "year": parsed["year"],
            "month": parsed["month"],
            "week": parsed["week"],
            "day": parsed["day"],
            "hour": parsed["hour"],
            "channel": parsed["channel"]
        }
        
        result[category].append(entry)
    return result

def create_pagination_token(last_evaluated_key, current_page, total_records, per_page):
    """Create a pagination token that includes metadata information"""
    if not last_evaluated_key:
        return None
    
    token_data = {
        'dynamodb_key': last_evaluated_key,
        'next_page': current_page + 1,
        'total_records': total_records,
        'per_page': per_page
    }
    
    return base64.b64encode(json.dumps(token_data).encode()).decode()

def parse_pagination_token(token):
    """Parse pagination token to get both DynamoDB key and metadata"""
    if not token:
        return None, None
    
    try:
        token_data = json.loads(base64.b64decode(token).decode())
        
        # Handle both old format (just DynamoDB key) and new format (with metadata)
        if 'dynamodb_key' in token_data:
            # New format with metadata
            return token_data['dynamodb_key'], {
                'page': token_data.get('next_page', 2),
                'total_records': token_data.get('total_records'),
                'per_page': token_data.get('per_page', PAGE_SIZE)
            }
        else:
            # Old format - just DynamoDB key, assume page 2
            return token_data, {
                'page': 2,
                'total_records': None,
                'per_page': PAGE_SIZE
            }
    except:
        return None, None

def get_total_count(partition_key, start_timestamp, end_timestamp, query_params, channel, query_type):
    """Get the total count of records matching the query criteria"""
    print("Getting total count...")
    
    start_sk = f"{start_timestamp}_"
    end_sk = f"{end_timestamp}_z"
    
    total_count = 0
    
    try:
        # If we need to apply additional filtering, we need to scan and count manually
        if query_type in ['normal', 'affected'] or channel:
            # We need to check each item for filtering criteria
            query_kwargs = {
                'KeyConditionExpression': Key('PARTITION_KEY').eq(partition_key) & 
                                        Key('SORT_KEY').between(start_sk, end_sk),
                'Select': 'ALL_ATTRIBUTES'
            }
            
            response = table.query(**query_kwargs)
            items = response.get('Items', [])
            
            # Handle pagination for large datasets
            while 'LastEvaluatedKey' in response:
                query_kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']
                response = table.query(**query_kwargs)
                items.extend(response.get('Items', []))
            
            # Apply filtering and count
            for item in items:
                processed_transaction = json.loads(item["processed_transaction"]) 
                original_transaction = processed_transaction["original_transaction"]
                evaluation = processed_transaction.get('evaluation', {})
                
                should_include = True
                
                # Apply channel filter
                if channel and original_transaction.get('channel') != channel:
                    should_include = False
                
                # Apply query_type filter
                if query_type == 'normal' and evaluation != {}:
                    should_include = False
                elif query_type == 'affected' and evaluation == {}:
                    should_include = False
                
                if should_include:
                    total_count += 1
        else:
            # For simple queries without filtering, use count query
            response = table.query(
                KeyConditionExpression=Key('PARTITION_KEY').eq(partition_key) & 
                                     Key('SORT_KEY').between(start_sk, end_sk),
                Select='COUNT'
            )
            total_count = response.get('Count', 0)
            
            # Handle pagination for count
            while 'LastEvaluatedKey' in response:
                response = table.query(
                    KeyConditionExpression=Key('PARTITION_KEY').eq(partition_key) & 
                                         Key('SORT_KEY').between(start_sk, end_sk),
                    Select='COUNT',
                    ExclusiveStartKey=response['LastEvaluatedKey']
                )
                total_count += response.get('Count', 0)
    
    except Exception as e:
        print(f"Error getting total count: {e}")
        return None
    
    print(f"Total count: {total_count}")
    return total_count

def get_entity_list_total_count(partition_key, start_timestamp, end_timestamp, entity_type, channel):
    """Get total count for entity list queries"""
    print("Getting entity list total count...")
    
    start_sk = f"{start_timestamp}_"
    end_sk = f"{end_timestamp}_z"
    
    try:
        # Query all items to count those matching entity and channel filters
        response = table.query(
            KeyConditionExpression=Key('PARTITION_KEY').eq(partition_key) & 
                                 Key('SORT_KEY').between(start_sk, end_sk),
            Select='ALL_ATTRIBUTES'
        )
        
        items = response.get('Items', [])
        
        # Handle pagination for large datasets
        while 'LastEvaluatedKey' in response:
            response = table.query(
                KeyConditionExpression=Key('PARTITION_KEY').eq(partition_key) & 
                                     Key('SORT_KEY').between(start_sk, end_sk),
                Select='ALL_ATTRIBUTES',
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            items.extend(response.get('Items', []))
        
        # Count items matching filters
        total_count = 0
        for item in items:
            processed_transaction = json.loads(item["processed_transaction"])
            original_transaction = processed_transaction["original_transaction"]
            
            # Apply entity type filter
            entity_match = False
            if entity_type == 'account' and original_transaction.get('account_id'):
                entity_match = True
            elif entity_type == 'application' and original_transaction.get('application_id'):
                entity_match = True
            elif entity_type == 'merchant' and original_transaction.get('merchant_id'):
                entity_match = True
            elif entity_type == 'product' and original_transaction.get('product_id'):
                entity_match = True
            
            # Apply channel filter
            channel_match = not channel or original_transaction.get('channel') == channel
            
            if entity_match and channel_match:
                total_count += 1
        
        print(f"Entity list total count: {total_count}")
        return total_count
        
    except Exception as e:
        print(f"Error getting entity list total count: {e}")
        return None

def lambda_handler(event, context):
    try:
        print("The event is ", event)
        query_params = event['queryStringParameters'] or {}
        print("The query params are ", query_params)

        # Handle both page-based and token-based pagination
        page = int(query_params.get('page', 1))
        page_size = int(query_params.get('page_size', PAGE_SIZE))
        pagination_token = query_params.get('pagination_token')
        
        start_date = query_params.get('start_date')
        end_date = query_params.get('end_date')
        list_type = query_params.get('list_type', '')
        entity_type = query_params.get('entity_type', '')
        channel = query_params.get('channel', '')
        query_type = query_params.get('query_type', '')
        
        if query_type != "single" and (not start_date or not end_date):
            return response(400, {'message': 'start_date and end_date are required'})

        start_timestamp = int(datetime.strptime(start_date, '%Y-%m-%d').timestamp())
        end_timestamp = int((datetime.strptime(end_date, '%Y-%m-%d') + timedelta(days=1)).timestamp() - 1)
        
        partition_key = construct_partition_key(query_params)
        result = {}
        
        if query_type == 'entity_list':
            result = query_transactions_by_entity_and_list(start_timestamp, 
                                                         end_timestamp, 
                                                         list_type, 
                                                         entity_type, 
                                                         query_type, 
                                                         channel,
                                                         page,
                                                         page_size,
                                                         pagination_token)
        elif query_type == 'single':
            items = query_transaction_by_id(partition_key, query_params)
            result = format_single_response(items, page, page_size)
        else:
            result = query_transactions(partition_key,
                                      start_timestamp,
                                      end_timestamp,
                                      query_params, 
                                      channel, 
                                      query_type,
                                      page,
                                      page_size,
                                      pagination_token)
        
        return response(200, result)
    
    except Exception as e:
        print("An error occurred ", e)
        return response(500, {'message': str(e)})

def construct_partition_key(params):
    query_type = params.get('query_type', 'all')
    channel = params.get('channel', '')
    list_type = params.get('list_type', '')
    
    if query_type == 'all' or query_type == 'normal' or query_type == 'affected' or query_type == 'single':
        return 'EVALUATED'
    elif query_type == 'account':
        return f"EVALUATED-{channel}-ACCOUNT-{params.get('account_ref', '')}"
    elif query_type == 'processor':
        return f"EVALUATED-{channel}-APPLICATION-{params.get('processor', '')}"
    elif query_type == 'merchant':
        return f"EVALUATED-{channel}-MERCHANT-{params.get('processor', '')}__{params.get('merchant_id', '')}"
    elif query_type == 'product':
        return f"EVALUATED-{channel}-PRODUCT-{params.get('processor', '')}__{params.get('merchant_id', '')}__{params.get('product_id', '')}"
    elif query_type in ['blacklist', 'watchlist', 'stafflist', 'limit', 'card-diff-country-6h']:
        if query_type == 'stafflist':
            return "EVALUATED-STAFF"
        return f"EVALUATED-{query_type.upper()}"
    elif query_type == 'entity_list':
        return f"EVALUATED-{list_type.upper()}"
    else:
        raise ValueError('Invalid query type')

def format_single_response(items, page, per_page):
    """Format response for single item queries"""
    return {
        'data': items,
        'metadata': {
            'page': 1,
            'previous_page': None,
            'next_page': None,
            'total_records': len(items),
            'pages': 1,
            'per_page': per_page,
            'from': 1 if items else 0,
            'to': len(items),
            'pagination_token': None
        }
    }

def format_paginated_response(items, current_page, per_page, next_pagination_token=None, total_records=None):
    """Format the response with consistent pagination metadata"""
    
    # Calculate pagination metadata
    if total_records is not None:
        total_pages = math.ceil(total_records / per_page) if total_records > 0 else 1
    else:
        total_pages = None
    
    # Calculate from/to based on actual page position
    from_record = ((current_page - 1) * per_page) + 1 if items else 0
    to_record = from_record + len(items) - 1 if items else 0
    
    # Determine next_page
    next_page = None
    if next_pagination_token:  # There are more items
        next_page = current_page + 1
    elif total_records is not None:  # We know total, check if we're at the end
        if current_page < total_pages:
            next_page = current_page + 1
    
    return {
        'data': items,
        'metadata': {
            'page': current_page,
            'previous_page': current_page - 1 if current_page > 1 else None,
            'next_page': next_page,
            'total_records': total_records,
            'pages': total_pages,
            'per_page': per_page,
            'from': from_record,
            'to': to_record,
            'pagination_token': next_pagination_token
        }
    }

def query_transactions(partition_key, start_timestamp, end_timestamp, query_params, channel, query_type, page, per_page, pagination_token=None):
    """Query transactions with proper DynamoDB pagination and consistent metadata"""
    print("Starting query_transactions with proper pagination and consistent metadata")
    
    start_sk = f"{start_timestamp}_"
    end_sk = f"{end_timestamp}_z"
    
    # Parse pagination token to get metadata
    exclusive_start_key, token_metadata = parse_pagination_token(pagination_token)
    current_page = page
    total_records = None
    
    if token_metadata:
        # Use metadata from token for consistency
        current_page = token_metadata['page']
        total_records = token_metadata['total_records']
        per_page = token_metadata['per_page']
        print(f"Using token metadata: page={current_page}, total_records={total_records}")
    elif page == 1:
        # Only get total count on first page without token
        total_records = get_total_count(partition_key, start_timestamp, end_timestamp, query_params, channel, query_type)
    
    # Build query parameters
    query_kwargs = {
        'KeyConditionExpression': Key('PARTITION_KEY').eq(partition_key) & 
                                Key('SORT_KEY').between(start_sk, end_sk),
        'ScanIndexForward': False,
        'Limit': per_page * 2  # Get more items to account for filtering
    }
    
    # Add pagination start key if provided
    if exclusive_start_key:
        query_kwargs['ExclusiveStartKey'] = exclusive_start_key
    
    # Query DynamoDB
    processed_items = []
    last_evaluated_key = None
    
    while len(processed_items) < per_page:
        response = table.query(**query_kwargs)
        items = response.get('Items', [])
        last_evaluated_key = response.get('LastEvaluatedKey')
        
        if not items:
            break
        
        # Process and filter items
        for item in items:
            if len(processed_items) >= per_page:
                break
                
            processed_transaction = json.loads(item["processed_transaction"]) 
            original_transaction = processed_transaction["original_transaction"]
            evaluation = processed_transaction.get('evaluation', {})
            
            account_id = original_transaction['account_id']
            application_id = original_transaction['application_id']
            merchant_id = original_transaction['merchant_id']
            product_id = original_transaction['product_id']
            assigned_person = assigned_status(original_transaction['transaction_id'])
            meta = get_merchant_product_data(merchant_id, product_id)
            
            processed_item = {
                'account_ref': account_id,
                'processor': application_id,
                'merchant_id': merchant_id,
                'product_id': product_id,
                'transaction_id': original_transaction['transaction_id'],
                'date': original_transaction['date'],
                'amount': original_transaction['amount'],
                'currency': original_transaction['currency'],
                'country': original_transaction['country'],
                'channel': original_transaction['channel'],
                'merchant_name': meta.get('merchantName', ''),
                'product_name': meta.get('productName', ''),
                'merchant_product_name': meta.get('merchantProductName', ''),
                'evaluation': transform_keys(evaluation),
                'assigned_to': assigned_person,
                'relevant_aggregates': transform_aggregates(
                    processed_transaction.get('aggregates', {}), 
                    account_id, 
                    application_id, 
                    merchant_id, 
                    product_id
                )
            }
            
            # Apply filtering based on query_type and channel
            should_include = True
            
            # Channel filter
            if channel and channel != processed_item['channel']:
                should_include = False
            
            # Query type filter
            if should_include:
                if query_type == 'normal' and processed_item["evaluation"] != {}:
                    should_include = False
                elif query_type == 'affected' and processed_item["evaluation"] == {}:
                    should_include = False
                    
            if should_include:
                processed_items.append(processed_item)
        
        # If no more items from DynamoDB or we don't have last_evaluated_key, break
        if not last_evaluated_key or not items:
            last_evaluated_key = None
            break
            
        # Update query for next iteration
        query_kwargs['ExclusiveStartKey'] = last_evaluated_key
    
    # Create next pagination token with metadata
    next_token = None
    if last_evaluated_key and len(processed_items) == per_page:
        next_token = create_pagination_token(last_evaluated_key, current_page, total_records, per_page)
    
    return format_paginated_response(processed_items, current_page, per_page, next_token, total_records)

def query_transaction_by_id(partition_key, params):
    """Query a single transaction by ID"""
    response = table.query(
        KeyConditionExpression=Key('PARTITION_KEY').eq(partition_key) & 
        Key('SORT_KEY').eq(params.get("transaction_id"))
    )
    
    items = []
    if 'Items' in response:
        items = response['Items']
    if 'Item' in response:
        items = [response['Item']]

    processed_items = []
    for item in items:
        processed_transaction = json.loads(item["processed_transaction"]) 
        original_transaction = processed_transaction["original_transaction"]
        evaluation = processed_transaction.get('evaluation', {})
        account_id = original_transaction['account_id']
        application_id = original_transaction['application_id']
        merchant_id = original_transaction['merchant_id']
        product_id = original_transaction['product_id']
        meta = get_merchant_product_data(merchant_id, product_id)
        
        processed_item = {
            'account_ref': account_id,
            'processor': application_id,
            'merchant_id': merchant_id,
            'product_id': product_id,
            'transaction_id': original_transaction['transaction_id'],
            'date': original_transaction['date'],
            'amount': original_transaction['amount'],
            'currency': original_transaction['currency'],
            'country': original_transaction['country'],
            'channel': original_transaction['channel'],
            'merchant_name': meta.get('merchantName', ''),
            'product_name': meta.get('productName', ''),
            'merchant_product_name': meta.get('merchantProductName', ''),
            'evaluation': transform_keys(evaluation),
            'relevant_aggregates': transform_aggregates(
                processed_transaction.get('aggregates', {}),
                account_id,
                application_id,
                merchant_id,
                product_id
            )
        }
        processed_items.append(processed_item)
    
    return processed_items

def query_transactions_by_entity_and_list(start_timestamp, end_timestamp, list_type, entity_type, query_type, channel, page, per_page, pagination_token=None):
    """Query transactions by entity and list with consistent metadata"""
    partition_key = f"EVALUATED-{list_type.upper()}"
    start_sk = f"{start_timestamp}_"
    end_sk = f"{end_timestamp}_z"
    
    # Parse pagination token to get metadata
    exclusive_start_key, token_metadata = parse_pagination_token(pagination_token)
    current_page = page
    total_records = None
    
    if token_metadata:
        # Use metadata from token for consistency
        current_page = token_metadata['page']
        total_records = token_metadata['total_records']
        per_page = token_metadata['per_page']
    elif page == 1:
        # Only get total count on first page without token
        total_records = get_entity_list_total_count(partition_key, start_timestamp, end_timestamp, entity_type, channel)
    
    # Build query parameters
    query_kwargs = {
        'KeyConditionExpression': Key('PARTITION_KEY').eq(partition_key) & 
                                Key('SORT_KEY').between(start_sk, end_sk),
        'ScanIndexForward': False,
        'Limit': per_page * 3  # Get more items since we'll filter them
    }
    
    # Add pagination start key if provided
    if exclusive_start_key:
        query_kwargs['ExclusiveStartKey'] = exclusive_start_key
    
    # Query and process items
    processed_items = []
    last_evaluated_key = None
    
    while len(processed_items) < per_page:
        response = table.query(**query_kwargs)
        items = response.get('Items', [])
        last_evaluated_key = response.get('LastEvaluatedKey')
        
        if not items:
            break
            
        for item in items:
            if len(processed_items) >= per_page:
                break
                
            processed_transaction = json.loads(item["processed_transaction"])
            original_transaction = processed_transaction["original_transaction"]
            
            # Apply entity type filter
            entity_match = False
            if entity_type == 'account' and original_transaction.get('account_id'):
                entity_match = True
            elif entity_type == 'application' and original_transaction.get('application_id'):
                entity_match = True
            elif entity_type == 'merchant' and original_transaction.get('merchant_id'):
                entity_match = True
            elif entity_type == 'product' and original_transaction.get('product_id'):
                entity_match = True
            
            # Apply channel filter
            channel_match = not channel or original_transaction.get('channel') == channel
            
            if entity_match and channel_match:
                evaluation = processed_transaction.get('evaluation', {})
                account_id = original_transaction['account_id']
                application_id = original_transaction['application_id']
                merchant_id = original_transaction['merchant_id']
                product_id = original_transaction['product_id']
                assigned_person = assigned_status(original_transaction['transaction_id'])
                meta = get_merchant_product_data(merchant_id, product_id)
                
                processed_item = {
                    'account_ref': account_id,
                    'processor': application_id,
                    'merchant_id': merchant_id,
                    'product_id': product_id,
                    'transaction_id': original_transaction['transaction_id'],
                    'date': original_transaction['date'],
                    'amount': original_transaction['amount'],
                    'currency': original_transaction['currency'],
                    'country': original_transaction['country'],
                    'channel': original_transaction['channel'],
                    'merchant_name': meta.get('merchantName', ''),
                    'product_name': meta.get('productName', ''),
                    'merchant_product_name': meta.get('merchantProductName', ''),
                    'evaluation': transform_keys(evaluation),
                    'assigned_to': assigned_person,
                    'relevant_aggregates': transform_aggregates(
                        processed_transaction.get('aggregates', {}),
                        account_id,
                        application_id,
                        merchant_id,
                        product_id
                    )
                }
                processed_items.append(processed_item)
        
        # If no more items from DynamoDB, break
        if not last_evaluated_key or not items:
            last_evaluated_key = None
            break
            
        # Update query for next iteration
        query_kwargs['ExclusiveStartKey'] = last_evaluated_key
    
    # Create next pagination token with metadata
    next_token = None
    if last_evaluated_key and len(processed_items) == per_page:
        next_token = create_pagination_token(last_evaluated_key, current_page, total_records, per_page)
    
    return format_paginated_response(processed_items, current_page, per_page, next_token, total_records)

def transform_keys(dictionary):
    """Transform keys in dictionary, replacing 'application' with 'processor'"""
    transformed_dict = {}
    for key, value in dictionary.items():
        new_key = key.replace('application', 'processor')
        transformed_dict[new_key] = value
    return transformed_dict

def assigned_status(transaction_id):
    """Get assigned status for transaction"""
    print("The transaction id is ", transaction_id)
    if transaction_id == "":
        return ""
        
    response = table.query(
        KeyConditionExpression=Key('PARTITION_KEY').eq("CASE") & Key('SORT_KEY').eq(transaction_id)
    )
    
    items = response['Items']
    print("The case items are ", items)
    if len(items) == 0:
        return {}
    else:
        try:
            return items[0]["assigned_to"]
        except Exception as e:
            print("Found an error in the retrieval of a case's email ", e)
            return {}

def response(status_code, body):
    """Format API response"""
    response_message = "Operation Successful" if status_code == 200 else "Unsuccessful operation"
    
    body_to_send = {
        "responseCode": status_code,
        "responseMessage": response_message,
        "data": body.get('data', []),
        "metadata": body.get('metadata')
    }
    
    return {
        'statusCode': status_code,
        'body': json.dumps(body_to_send),
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Credentials': True,
        },
    }
