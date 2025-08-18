### DynamoDB Table Design: Merchants


**Table Name:** `MerchantTable`

---

### 1. Main Table Structure


| Attribute Name | DynamoDB Type | Value / Pattern | Description |
| :--- | :--- | :--- | :--- |
| **PK** | `String` | `MERCHANT` | **Partition Key**. A static value for all merchant items. |
| **SK** | `String` | `<merchant_id>` | **Sort Key**. The unique ID for the merchant. |
| `companyName` | `String` | Merchant's Company Name | The legal name of the company. |
| `code` | `String` | `<merchant_code>` | The unique code assigned to the merchant. |
| `tradeName` | `String` | Merchant's Trade Name | The operating name of the merchant. |
| `alias` | `String` | `<merchant_alias>` | A unique, short alias for the merchant. |
| `tags` | `String Set` | `["tag1", "tag2"]` | A set of tags for categorization. |
| `country` | `String` | `<country_code>` | Two-letter country code (e.g., GH). |
| `tier` | `String` | `<tier_level>` | The assigned merchant tier. |
| `typeOfCompany` | `String` | `<company_type>` | e.g., "Limited Liability". |
| `status` | `String` | `active` or `inactive` | The current status of the merchant. |
| `companyLogo` | `String` | `<url_to_logo>` | URL for the company's logo. Optional. |
| `companyRegistrationNumber` | `String` | `<reg_number>` | The company's registration number. Optional. |
| `vatRegistrationNumber` | `String` | `<vat_number>` | The VAT registration number. Optional. |
| `dateOfIncorporation` | `String` | ISO 8601 Date | e.g., `YYYY-MM-DD`. Optional. |
| `dateOfCommencement` | `String` | ISO 8601 Date | e.g., `YYYY-MM-DD`. Optional. |
| `taxIdentificationNumber`| `String` | `<tin_number>` | The Tax ID number. Optional. |
| `createdAt` | `String` | ISO 8601 Timestamp | The creation timestamp of the record. |
| `updatedAt` | `String` | ISO 8601 Timestamp | The last modification timestamp. Optional. |


#### Example Item:

```json
{
  "PK": "MERCHANT",
  "SK": "7576f4bd-7c31-44f3-b8e8-3b8931c4555f",
  "companyName": "University Of Ghana",
  "code": "GH-UGL",
  "tradeName": "University Of Ghana",
  "alias": "UGL",
  "tags": [
    "schools",
    "transpay"
  ],
  "country": "GH",
  "tier": "tier 1",
  "typeOfCompany": "Limited Liability",
  "status": "active",
  "companyLogo": null,
  "companyRegistrationNumber": null,
  "vatRegistrationNumber": null,
  "dateOfIncorporation": null,
  "dateOfCommencement": null,
  "taxIdentificationNumber": null,
  "createdAt": "2025-07-30T09:55:17.000Z",
  "updatedAt": null,
  "EntityType": "Merchant"
}
```

---

### 2. Query Patterns Supported by the Main Table

* **Get a specific merchant by their ID:**
    * Use a `GetItem` call with `PK = "MERCHANT"` and `SK = "<merchant_id>"`.
* **Get all merchants:**
    * Use a `Query` operation where `PK = "MERCHANT"`. 
