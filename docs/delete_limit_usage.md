# Delete Limit Endpoint – Usage Guide

This service exposes four HTTP DELETE routes, one per limit hierarchy level.

| Limit type | Route | Required query parameters |
|------------|-------|---------------------------|
| Account | `/limits/account` | `channel`, `account_ref` |
| Account-Application | `/limits/account-processor` | `channel`, `account_ref`, `processor` |
| Account-Application-Merchant | `/limits/account-processor-merchant` | `channel`, `account_ref`, `processor`, `merchant_id` |
| Account-Application-Merchant-Product | `/limits/account-processor-merchant-product` | `channel`, `account_ref`, `processor`, `merchant_id`, `product_id` |

`channel` is always required and is lower-cased internally.  
Other parameters depend on the limit level and are simply ignored when not
needed.

---

## cURL examples

```bash
# 1. Delete an ACCOUNT-level WEB limit
curl -X DELETE \
  "$BASE_URL/limits/account?channel=WEB&account_ref=123456"

# 2. Delete an ACCOUNT-APPLICATION limit
curl -X DELETE \
  "$BASE_URL/limits/account-processor?channel=WEB&account_ref=123456&processor=APP01"

# 3. Delete an ACCOUNT-APPLICATION-MERCHANT limit
curl -X DELETE \
  "$BASE_URL/limits/account-processor-merchant?channel=POS&account_ref=123456&processor=APP01&merchant_id=M123"

# 4. Delete an ACCOUNT-APPLICATION-MERCHANT-PRODUCT limit
curl -X DELETE \
  "$BASE_URL/limits/account-processor-merchant-product?channel=API&account_ref=123456&processor=APP01&merchant_id=M123&product_id=P789"
```

---

## Expected responses

• **200 OK**

```json
{
  "responseCode": 200,
  "responseMessage": "Operation Successful",
  "data": {"message": "Limit deleted successfully"},
  "metadata": null
}
```

• **400 Bad Request** – missing or invalid query parameters.  
• **500 Internal Server Error** – unexpected failure (see logs for details).
