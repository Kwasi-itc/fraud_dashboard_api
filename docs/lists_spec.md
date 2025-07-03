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

| Method | Path                                   | What happens inside the Lambda |
|--------|----------------------------------------|--------------------------------|
| **POST**   | `/lists`                               | 1. Validates `list_type` against the **`ALLOWED_LIST_TYPES`** constant.<br>2. Builds `PARTITION_KEY` as `<LIST_TYPE>-<CHANNEL>-<ENTITY_TYPE>` and a composite `SORT_KEY` from the entity identifiers – logic mirrors the code in `lists/create.py`.<br>3. Executes `PutItem` **with a conditional expression** `attribute_not_exists(PARTITION_KEY)` to avoid duplicates.<br>4. Stamps `created_at` before returning PK/SK in the response. |
| **GET**    | `/lists`                               | Branches in `lists/read.py`:<br>&nbsp;&nbsp;• If **no query-string parameters** → calls `query_items_in_all_lists_sorted_by_date()` (*scan+sort*) to deliver a dashboard feed.<br>&nbsp;&nbsp;• If parameters supplied → performs `GetItem` (exact PK/SK) or `Query` when only PK is known, then runs `transform_items()` to replace `SORT_KEY` with `entity_id`. |
| **GET**    | `/lists/by-list-type`                  | Uses a **`Scan`** with `begins_with(PARTITION_KEY, "<LIST_TYPE>-")` to collect everything inside one list. Results transformed the same way as above. |
| **GET**    | `/lists/by-channel`                    | Similar scan but with `contains(PARTITION_KEY, "-<CHANNEL>-")` so it catches all entity types and list types for that channel. |
| **GET**    | `/lists/by-entity-type`                | Scans with `contains(PARTITION_KEY, "-<ENTITY_TYPE>")`, then post-processes each record to explode the `SORT_KEY` into `application_id`, `merchant_id`, `product_id` as shown in `query_by_entity_type()`. |
| **GET**    | `/lists/by-date-range`                 | Scans **all items** and filters on `created_at BETWEEN :start AND :end` (ISO dates). Handy for audits. |
| **GET**    | `/lists/by-list-type-and-entity-type`  | Two-phase call: first reuse *by-list-type* scan, then in Python filter by `entity_type`. Returns transformed items. |
| **PUT**    | `/lists`                               | Builds keys exactly like POST, runs `UpdateItem` with `set updated_at = :now, notes = :notes` (only fields currently allowed). Fails with **404** if the item is absent. |
| **DELETE** | `/lists`                               | Runs `DeleteItem` on the calculated PK/SK. No conditional check → operation is **idempotent**; success even when item never existed. |

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

**Successful response – 200**

```json
{
  "responseCode": 200,
  "responseMessage": "Operation Successful",
  "data": {
    "PARTITION_KEY": "WATCHLIST-WEB-MERCHANT",
    "entity_id": "APP1__MERCH9",
    "application_id": "APP1",
    "merchant_id": "MERCH9",
    "created_at": "2025-07-02T09:31:00.000000"
  }
}
```

If the entry is missing the Lambda returns **404** with the standard wrapper.

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

**Successful response – 200**

```json
{
  "responseCode": 200,
  "responseMessage": "Operation Successful",
  "data": {
    "message": "Item updated successfully",
    "updated_at": "2025-07-04T12:05:00.000000"
  }
}
```

---

### 3.4  DELETE `/lists` – Remove entry

```http
DELETE /lists?list_type=STAFFLIST&channel=POS&entity_type=APPLICATION&application_id=APP2
```

**Successful response – 200**

```json
{
  "responseCode": 200,
  "responseMessage": "Operation Successful",
  "data": {
    "message": "Item deleted successfully"
  }
}
```

The operation is idempotent—calling the same DELETE again still returns **200**.

---

### 3.5  GET `/lists/by-list-type` – List entries by **list_type**

```http
GET /lists/by-list-type?list_type=BLACKLIST
```

Success ⇒ `200`

```json
{
  "responseCode": 200,
  "responseMessage": "Operation Successful",
  "data": [
    {
      "PARTITION_KEY": "BLACKLIST-MOBILE-ACCOUNT",
      "entity_id": "ACCT001",
      "created_at": "2025-07-03T10:00:00.000000"
    },
    { "...": "more items" }
  ]
}
```

---

### 3.6  GET `/lists/by-channel` – List entries by **channel**

```http
GET /lists/by-channel?channel=POS
```

**Successful response – 200**

```json
{
  "responseCode": 200,
  "responseMessage": "Operation Successful",
  "data": [
    {
      "PARTITION_KEY": "BLACKLIST-POS-ACCOUNT",
      "entity_id": "ACCT009",
      "created_at": "2025-07-03T10:00:00.000000"
    },
    { "...": "more items" }
  ]
}
```

---

### 3.7  GET `/lists/by-entity-type` – List entries by **entity_type**

```http
GET /lists/by-entity-type?entity_type=MERCHANT
```

**Successful response – 200**

```json
{
  "responseCode": 200,
  "responseMessage": "Operation Successful",
  "data": [
    {
      "PARTITION_KEY": "WATCHLIST-WEB-MERCHANT",
      "entity_id": "APP1__MERCH9",
      "application_id": "APP1",
      "merchant_id": "MERCH9",
      "created_at": "2025-07-02T09:31:00.000000"
    }
  ]
}
```

---

### 3.8  GET `/lists/by-date-range` – List entries created between two dates

```http
GET /lists/by-date-range?start_date=2025-01-01&end_date=2025-01-31
```

**Successful response – 200**

```json
{
  "responseCode": 200,
  "responseMessage": "Operation Successful",
  "data": [
    {
      "PARTITION_KEY": "WATCHLIST-MOBILE-APPLICATION",
      "entity_id": "APP2",
      "created_at": "2025-01-15T12:00:00.000000"
    }
  ]
}
```

---

### 3.9  GET `/lists/by-list-type-and-entity-type` – Combined filter

```http
GET /lists/by-list-type-and-entity-type?list_type=WATCHLIST&entity_type=ACCOUNT
```

**Successful response – 200**

```json
{
  "responseCode": 200,
  "responseMessage": "Operation Successful",
  "data": [
    {
      "PARTITION_KEY": "WATCHLIST-WEB-ACCOUNT",
      "entity_id": "ACCT777",
      "created_at": "2025-06-10T08:45:00.000000"
    }
  ]
}
```

All the helper paths above return `200` with a **list of items** (possibly empty)
using the same wrapper shape shown in 3.5.  
If no record matches, the `data` array is empty. Invalid/missing parameters
produce `400 Bad Request`.

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
