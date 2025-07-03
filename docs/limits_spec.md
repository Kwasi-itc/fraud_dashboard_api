# Limits Service – Technical Specification

The **Limits** service lives in the `limits/` directory and is deployed as the
`LimitsFunction` Lambda in `template.yaml`.  
It provides CRUD operations for fraud-detection spending/count thresholds at
four different hierarchy levels:

1. **Account** (`/limits/account`)  
2. **Account + Application** (`/limits/account-application`)  
3. **Account + Application + Merchant** (`/limits/account-application-merchant`)  
4. **Account + Application + Merchant + Product** (`/limits/account-application-merchant-product`)  

The same Lambda handles **POST** (create), **GET** (read), **PUT** (update) and
**DELETE** (delete) by routing inside `limits/lambda_handler.py`.

---

## 1. DynamoDB Schema (`FRAUD_LIMITS_TABLE`)

| Attribute         | Type | Notes                                                                                           |
|-------------------|------|-------------------------------------------------------------------------------------------------|
| `PARTITION_KEY`   | S    | Encodes the limit type and channel, e.g. `LIMITS-MOBILE-ACCOUNT`.                               |
| `SORT_KEY`        | S    | Composite identifier for the entity path, built with underscores (`__`).                        |
| `AMOUNT`          | N    | Single-transaction amount limit.                                                                |
| `HOURLY_SUM`      | N    | Hourly cumulative amount limit.                                                                 |
| `DAILY_SUM`       | N    | Daily cumulative amount limit.                                                                  |
| `WEEKLY_SUM`      | N    | Weekly cumulative amount limit.                                                                 |
| `MONTHLY_SUM`     | N    | Monthly cumulative amount limit.                                                                |
| `HOURLY_COUNT`    | N    | Hourly transaction-count limit.                                                                 |
| `DAILY_COUNT`     | N    | Daily transaction-count limit.                                                                  |
| `WEEKLY_COUNT`    | N    | Weekly transaction-count limit.                                                                 |
| `MONTHLY_COUNT`   | N    | Monthly transaction-count limit.                                                                |
| *timestamps*      | —    | `created_at`, `updated_at` handled in a future improvement.                                     |

All operations use the **PK/SK** pattern; no GSIs required.

---

## 2. Endpoints & Examples

> The wrapper returned by `limits/utils.py::response_lambda()` follows the
> uniform structure  
> `{"responseCode": 200, "responseMessage": "Operation Successful", "data": …}`  
> and is omitted below for brevity.

### 2.1 POST `/limits/account` – *Create account limit*

**Key points**

- Applies to one **account** across all applications/merchants/products.
- All numeric fields are **required** by current implementation.

```http
POST /limits/account
Content-Type: application/json
```

```json
{
  "channel": "MOBILE",
  "account_id": "ACCT001",
  "AMOUNT": 2000,
  "HOURLY_SUM": 10000,
  "DAILY_SUM": 30000,
  "WEEKLY_SUM": 100000,
  "MONTHLY_SUM": 200000,
  "HOURLY_COUNT": 50,
  "DAILY_COUNT": 500,
  "WEEKLY_COUNT": 2000,
  "MONTHLY_COUNT": 8000
}
```

---

### 2.2 GET `/limits/account` – *Read account limit*

```http
GET /limits/account?channel=MOBILE&account_id=ACCT001
```

Returns the limit item or **404** if none exists.

---

### 2.3 PUT `/limits/account` – *Update account limit*

Same identifiers as POST; only the attributes present in the JSON body are
updated. Example:

```http
PUT /limits/account
Content-Type: application/json
```

```json
{
  "channel": "MOBILE",
  "account_id": "ACCT001",
  "DAILY_SUM": 40000,
  "DAILY_COUNT": 600
}
```

---

### 2.4 DELETE `/limits/account` – *Delete account limit*

```http
DELETE /limits/account?channel=MOBILE&account_id=ACCT001
```

---

### 2.5 Composite-entity paths

The remaining three paths work identically but require additional identifiers:

| Path                                             | Required identifiers (query/body)                                          |
|--------------------------------------------------|-----------------------------------------------------------------------------|
| `/limits/account-application`                    | `channel`, `account_id`, `application_id`                                   |
| `/limits/account-application-merchant`           | `channel`, `account_id`, `application_id`, `merchant_id`                    |
| `/limits/account-application-merchant-product`   | `channel`, `account_id`, `application_id`, `merchant_id`, `product_id`      |

---

## 3. Error Handling

| HTTP | Reason                                      |
|------|---------------------------------------------|
| 400  | Missing/invalid parameters or JSON schema.  |
| 404  | Limit not found (GET / DELETE).             |
| 500  | Unhandled exception (logged via CloudWatch).|

---

## 4. IAM Permissions

The Lambda role must include at minimum:

```yaml
dynamodb:PutItem
dynamodb:GetItem
dynamodb:DeleteItem
dynamodb:UpdateItem
dynamodb:Query
```

Resource-scoped to the `FRAUD_LIMITS_TABLE` ARN.

---

## 5. Open Items / TODO

* Input validation with Pydantic or `jsonschema`.
* Add `created_at` / `updated_at` timestamps and optimistic locking.
* Pagination & filtering for a future bulk search endpoint.
* Unit tests (`tests/unit/limits/`).
