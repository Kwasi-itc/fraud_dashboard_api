### DynamoDB Table Design: Merchant Products



**Table Name:** `FraudPyV1MerchantProductNotificationTable` or `FraudPyV1MerchantProductNotification`

---

### 1. Main Table Structure

This design uses a composite primary key to allow for rich query capabilities. It has been updated to include all fields from the sample event payload for completeness.

| Attribute Name | DynamoDB Type | Value / Pattern | Description |
| :--- | :--- | :--- | :--- |
| **PK** | `String` | `MERCHANT_PRODUCT#<merchant_id>` | **Partition Key**. Uniquely identifies a merchant. |
| **SK** | `String` | `PRODUCT#<product_id>` | **Sort Key**. Uniquely identifies a product for that merchant. |
| `merchantProductId` | `String` | `<merchant_product_id>` | The unique ID for the merchant-product relationship. |
| `merchantId` | `String` | `<merchant_id>` | The ID of the merchant. |
| `productId` | `String` | `<product_id>` | The ID of the product. |
| `merchantProductName` | `String` | Merchant Product Name | The name of the product as defined by the merchant. |
| `description` | `String` | Product Description | The description of the product. |
| `productName` | `String` | Base Product Name | The canonical name of the base product. |
| `productCode` | `String` | `<product_code>` | The canonical code of the base product. |
| `merchantProductCode`| `String` | `<merchant_product_code>` | A specific product code defined by the merchant. Optional. |
| `merchantName` | `String` | Merchant's Name | The full name of the merchant. |
| `merchantCode` | `String` | `<merchant_code>` | The code for the merchant. |
| `canSettle` | `String` | `Y` or `N` | Indicates if the product can be settled. |
| `status` | `String` | `active` or `inactive` | The status of the merchant-product link. |
| `tags` | `String Set` | `["tag1", "tag2"]` | A set of tags associated with the product. |
| `alias` | `String` | `<alias>` | An alias for the merchant product. |
| `serviceCode` | `String` | `<service_code>` | A specific service code. Optional. |
| `configuration` | `Map` | `{...}` | The JSON configuration stored as a DynamoDB Map object. |
| `createdAt` | `String` | ISO 8601 Timestamp | The creation timestamp of the record. |
| `updatedAt` | `String` | ISO 8601 Timestamp | The last modification timestamp of the record. |


#### Example Item:

```json
{
  "PK": "MERCHANT_PRODUCT#58934762-502c-4cf3-a0b1-50ee4a009f24",
  "SK": "PRODUCT#0375dfba-f2f0-11ec-a57d-06c51b458ea3",
  "merchantProductId": "1ebb44d8-9fa3-42ca-b3a8-89f99bec3bd6",
  "merchantId": "58934762-502c-4cf3-a0b1-50ee4a009f24",
  "productId": "0375dfba-f2f0-11ec-a57d-06c51b458ea3",
  "name": "Donewell Standingorderrecurring",
  "description": "Donewell Standingorderrecurring",
  "productName": "Uniwallet",
  "productCode": "UNIWALLT",
  "merchantProductCode": null,
  "merchantName": "Donewell Life Company Limited",
  "merchantCode": "GH-DONEWELL",
  "canSettle": "Y",
  "status": "active",
  "tags": [
    "uniwallet"
  ],
  "alias": "DONEWELL",
  "serviceCode": null,
  "configuration": {
    "components": [
      {
        "key": "charge_elevy",
        "type": "boolean",
        "label": "charge elevy",
        "value": 0,
        "validation": { "required": "true" }
      },
      {
        "key": "name_enquiry_status",
        "type": "boolean",
        "label": "Name Enquiry Status",
        "value": 1,
        "validation": { "required": "false" }
      }
    ],
    "renderType": "gui",
    "callback_url": "[https://ddv15-weekly-debits-callbackuat.transflowitc.com/weekly/mobiledebit-](https://ddv15-weekly-debits-callbackuat.transflowitc.com/weekly/mobiledebit-) callback/uniwallet/callback",
    "charge_elevy": 0,
    "debit_status": 1,
    "product_code": "DONEWELLR"
  },
  "createdAt": "2025-07-08T09:16:01.000Z",
  "updatedAt": "2025-08-01T12:34:56Z",
  "EntityType": "MerchantProduct"
}
```

---

### 2. Query Patterns Supported by the Main Table

This key structure directly supports the following access patterns:

* **Get a specific product for a specific merchant:**
    * Use a `GetItem` call with the full `PK` and `SK`.
* **Get all products for a specific merchant:**
    * Use a `Query` operation where `PK = MERCHANT_PRODUCT#<merchant_id>` and the `SK` `begins_with("PRODUCT#")`.

---
