# Lists Service – Technical Specification

The **Lists** service contains four Lambda handlers located in the `lists/`
directory and wired in `template.yaml`:

| Lambda | Path(s) | CRUD verb(s) |
|--------|---------|--------------|
| `create.lambda_handler`  | `/lists`                           | **POST** |
| `read.lambda_handler`    | `/lists*` (all read-helper paths)  | **GET** |
| `update_2.lambda_handler`| `/lists`                           | **PUT** |
| `delete.lambda_handler`  | `/lists`                           | **DELETE** |

It manages three functional lists that influence fraud decisions:

* **BLACKLIST** – block/decline automatically.  
* **WATCHLIST**  – require manual review.  
* **STAFFLIST**  – internal/test entities exempt from rules.

---

## 1. DynamoDB Schema – `FRAUD_LISTS_TABLE`

| Attribute       | Type | Description |
|-----------------|------|-------------|
| `PARTITION_KEY` | S    | Pattern: `<LIST_TYPE>-<CHANNEL>-<ENTITY_TYPE>`<br>Example: `BLACKLIST-MOBILE-ACCOUNT` |
| `SORT_KEY`      | S    | **Entity identifier**<br>`ACCOUNT`&nbsp;→ `account_id`<br>`APPLICATION`&nbsp;→ `application_id`<br>`MERCHANT`&nbsp;→ `application_id__merchant_id`<br>`PRODUCT`&nbsp;→ `application_id__merchant_id__product_id` |
| `created_at`    | S    | ISO-8601 timestamp (UTC). |
| Optional extras | —    | `created_by`, `notes`, etc. |

All queries rely on the PK/SK pattern; no GSIs/LSIs are defined.

---

## 2. Endpoint Matrix

| Method | Path                                   | Detailed description |
|--------|----------------------------------------|----------------------|
| **POST**   | `/lists`                               | Create a new item in **BLACKLIST / WATCHLIST / STAFFLIST**. Performs a DynamoDB <br>`ConditionExpression attribute_not_exists` to reject duplicates, stamps `created_at`, and builds PK/SK following the schema rules. |
| **GET**    | `/lists`                               | Fetch **one** list entry when *all* identifiers are supplied (`list_type`, `channel`, `entity_type`, plus the entity-id fields). Returns **404** if the record is missing. |
| **GET**    | `/lists/by-list-type`                  | Return **every** item that belongs to the provided `list_type` across all channels and entity types. Useful for bulk export of BLACKLIST, etc. |
| **GET**    | `/lists/by-channel`                    | Filter by `channel` (MOBILE / WEB / POS …). Scans all list types and entity types for that channel. |
| **GET**    | `/lists/by-entity-type`                | Filter by high-level `entity_type` (ACCOUNT / APPLICATION / MERCHANT / PRODUCT) irrespective of channel or list_type. |
| **GET**    | `/lists/by-date-range`                 | Time-based audit query. Accepts `start_date` and `end_date` (YYYY-MM-DD). Returns items whose `created_at` falls inside the interval. |
| **GET**    | `/lists/by-list-type-and-entity-type`  | Intersection filter: fetch only entries where **both** `list_type` *and* `entity_type` match the query parameters. |
| **PUT**    | `/lists`                               | Update mutable metadata (currently only `notes`, future fields TBD). Uses `UpdateExpression` & sets `updated_at`. |
| **DELETE** | `/lists`                               | Permanently remove an entry. Idempotent: returns success even when the item did not exist. |

All handlers wrap their payloads with the common schema:

```json
{
  "responseCode": 200,
  "responseMessage": "Operation Successful",
  "data": { ... }
}
```

---

## 3. Requests & Responses

### 3.1  POST `/lists` – Create entry

```http
POST /lists
Content-Type: application/json
```

```json
{
  "list_type": "BLACKLIST",
  "channel": "MOBILE",
  "entity_type": "ACCOUNT",
  "account_id": "ACCT001",
  "notes": "Confirmed fraud account",
  "created_by": "fraud.analyst@example.com"
}
```

Success ⇒ `200`

```json
{
  "responseCode": 200,
  "responseMessage": "Operation Successful",
  "data": {
    "PARTITION_KEY": "BLACKLIST-MOBILE-ACCOUNT",
    "entity_id": "ACCT001",
    "created_at": "2025-07-03T10:00:00.000000"
  }
}
```

Duplicate key ⇒ `409` (conditional-write failure).

---

### 3.2  GET `/lists` – Read specific entry

```http
GET /lists?list_type=WATCHLIST&channel=WEB&entity_type=MERCHANT&application_id=APP1&merchant_id=MERCH9
```

Success ⇒ `200` with the single item; **404** if absent.

---

### 3.3  PUT `/lists` – Update notes

```http
PUT /lists
Content-Type: application/json
```

```json
{
  "list_type": "WATCHLIST",
  "channel": "WEB",
  "entity_type": "MERCHANT",
  "application_id": "APP1",
  "merchant_id": "MERCH9",
  "notes": "Chargeback spike – monitor weekly"
}
```

Returns `200` and the `updated_at` value.

---

### 3.4  DELETE `/lists` – Remove entry

```http
DELETE /lists?list_type=STAFFLIST&channel=POS&entity_type=APPLICATION&application_id=APP2
```

Returns `200` even if the item did not exist (idempotent design).

---

## 4. Error Handling

| HTTP | Condition |
|------|-----------|
| 400  | Missing/invalid parameters or body fields. |
| 404  | Entry not found (GET, PUT, DELETE). |
| 409  | Duplicate key on create. |
| 500  | Unhandled exception (logged in CloudWatch). |

---

## 5. IAM Requirements

Lambda role needs CRUD on the table:

```yaml
dynamodb:PutItem
dynamodb:GetItem
dynamodb:DeleteItem
dynamodb:UpdateItem
dynamodb:Query
```

Restrict permissions to `${FRAUD_LISTS_TABLE}` ARN.

---

## 6. Open Items

* Add `ConditionExpression` to prevent duplicates on create.
* Pagination for large `by-*` queries.
* Structured logging & tracing.
* Unit tests (`tests/unit/lists`) and integration tests.
