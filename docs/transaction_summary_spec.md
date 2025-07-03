# Transaction-Summary Service – Technical Specification

The **Transaction-Summary** service is implemented in
`transactions_summary/app.py` and deployed as `TransactionSummaryFunction`
(`template.yaml`).  
It exposes **one** read-only endpoint – `GET /transaction-summary` – that returns
high-level counts and amount totals for a fixed set of categories displayed on
the FraudPy dashboard.

---

## 1. Endpoint

| Method | Path | Description |
|--------|------|-------------|
| **GET** | `/transaction-summary` | Aggregate totals for a date range across 5 buckets:<br>**blacklist · watchlist · stafflist · limits · normal** |

### 1.1 Required query parameters

| Name | Example | Purpose |
|------|---------|---------|
| `start_date` | `2025-07-01` | Inclusive start (YYYY-MM-DD, UTC). |
| `end_date`   | `2025-07-31` | Inclusive end   (YYYY-MM-DD, UTC). |

### 1.2 Request example

```http
GET /transaction-summary?start_date=2025-07-01&end_date=2025-07-31
```

### 1.3 Successful response – 200

```json
{
  "responseCode": 200,
  "responseMessage": "Operation Successful",
  "data": {
    "blacklist": { "count": 12, "sum": 14820.75 },
    "watchlist": { "count": 5,  "sum": 2310.00 },
    "stafflist": { "count": 3,  "sum": 503.50  },
    "limits":    { "count": 7,  "sum": 10200.00},
    "normal":    { "count": 280,"sum": 96250.10}
  }
}
```

### 1.4 Error responses

| HTTP | Scenario |
|------|----------|
| **400** | Missing / invalid query parameters |
| **500** | Unexpected exception while querying / aggregating |

**400 Example**

```json
{
  "responseCode": 400,
  "responseMessage": "Unsuccessful operation",
  "data": { "message": "start_date and end_date are required" }
}
```

**500 Example**

```json
{
  "responseCode": 500,
  "responseMessage": "Unsuccessful operation",
  "data": { "message": "Internal error. See logs" }
}
```

---

## 2. Processing Logic (per `app.py`)

1. **Validate** required params – missing → **400**.  
2. Convert both dates to UNIX epoch and build `<start>_` / `<end>_z` sort-key
   range.  
3. **Loop over list types** `BLACKLIST, WATCHLIST, STAFF, LIMIT`, for each:  
   * Query DynamoDB with `PK = "EVALUATED-<LIST_TYPE>"` + SK range.  
   * Sum `amount` and increment count for each item.  
4. Query `PK="EVALUATED"` for the same range; treat items **without** an
   `evaluation` field as **normal**.  
5. Return the aggregated dictionary via `response()` helper.

---

## 3. DynamoDB Access Pattern

| Partition key queried | Meaning |
|-----------------------|---------|
| `EVALUATED-BLACKLIST` | Transactions matched against **BLACKLIST** rules. |
| `EVALUATED-WATCHLIST` | Transactions matched **WATCHLIST**. |
| `EVALUATED-STAFF`     | Transactions tagged as **STAFFLIST**. |
| `EVALUATED-LIMIT`     | Transactions that triggered **limit** rules. |
| `EVALUATED`           | **All** evaluated transactions – used to compute “normal”. |

Sorting key pattern: `<unix_ts>_<uuid>` enables fast time-range
`between(start_sk, end_sk)` queries.

---

## 4. Error Handling

| HTTP | Reason |
|------|--------|
| 400  | `start_date` / `end_date` missing or malformed. |
| 500  | Unhandled exception during query/aggregation. |

---

## 5. IAM Permissions

Same CRUD permissions on `FRAUD_PROCESSED_TRANSACTIONS_TABLE` already granted in
`template.yaml`:

```yaml
dynamodb:Query
```

---

## 6. Open Items / TODO

* Add additional buckets (e.g. **affected** vs **normal** split per channel).  
* Implement caching (Lambda Extensions / DAX) for large date spans.  
* Structured logging + X-Ray sub-segments for each DDB query.  
* Unit tests covering each aggregation path.
