# CDC Product Addon Processor Lambda

A Python AWS Lambda triggered via **Lambda Function URL** that receives addon item data and enriches existing products in the **OpenSearch** `products` index with supplementary fields. When the target product does not yet exist, the addon data is cached in **DynamoDB** for later backfill by the `cdc-product-processor`.

---

## Table of Contents

1. [What This Lambda Does](#what-this-lambda-does)
2. [Architecture](#architecture)
3. [Field Mapping](#field-mapping)
4. [Environment Variables](#environment-variables)
5. [Request Format](#request-format)
6. [DynamoDB Cache Design](#dynamodb-cache-design)
7. [Project Structure](#project-structure)
8. [Local Setup & Testing](#local-setup--testing)
9. [Running Tests & Coverage](#running-tests--coverage)
10. [Testing & Validating from the AWS Console](#testing--validating-from-the-aws-console)
11. [Deployment Notes](#deployment-notes)
12. [Troubleshooting](#troubleshooting)

---

## What This Lambda Does

```
Upstream CDC Source  →  HTTP POST (Lambda Function URL)  →  This Lambda  →  OpenSearch (products index)
                                                                      ↘  DynamoDB (enrichment_cache)
```

| Scenario | What happens |
|---|---|
| Product **exists** in OpenSearch | Addon fields are merged into the product document via partial update; addon data is also cached in DynamoDB for future re-indexes |
| Product **does not exist** | Addon data is cached in DynamoDB (`PK=PRODUCT_ADDON`, `SK=<partNumber>`); when `cdc-product-processor` later indexes this product, it auto-backfills the addon fields |
| Delete operation (`d`) | Addon-managed fields are set to `null` on the product document; DynamoDB cache entry is deleted |

**Key design decisions:**
- Products are looked up by `partNumber.keyword` (not `uniqueID`) since the addon source provides `ItemID` which maps to `partNumber`.
- The addon processor does **not** create new products — it only enriches existing ones or caches data for backfill.
- When enriching, addon fields do **not** overwrite values already present on the product document during backfill.
- `ShortDescription` from the source is intentionally ignored.
- The DynamoDB cache is always updated alongside OpenSearch to ensure addon data survives product re-indexing.

---

## Architecture

```
┌───────────────────────────────────────────────────────────────────────┐
│                          AWS Lambda                                   │
│                                                                       │
│  lambda_handler()                                                     │
│       │                                                               │
│       ├─ parse_body()  → extract JSON from event                      │
│       │                                                               │
│       ├─ For each item in payload.items:                               │
│       │   ├─ map_addon_fields()  → ItemID→partNumber, etc.            │
│       │   │                                                           │
│       │   ├─ find_product_doc_id()  → search by partNumber.keyword    │
│       │   │                                                           │
│       │   └─ Product found?                                           │
│       │       ├─ YES → partial update in OpenSearch                   │
│       │       │        + cache to DynamoDB                            │
│       │       └─ NO  → cache to DynamoDB for backfill                 │
│       │                                                               │
│       └─ Return results                                               │
│                                                                       │
│                 ┌──────────────────┐    ┌──────────────────┐          │
│                 │   OpenSearch     │    │   DynamoDB        │          │
│                 │  products index  │    │  enrichment_cache │          │
│                 └──────────────────┘    └──────────────────┘          │
└───────────────────────────────────────────────────────────────────────┘
```

---

## Field Mapping

| Source Field (Addon) | Target Field (Products Index) | Notes |
|---|---|---|
| `ItemID` | `partNumber` | Used as **lookup key** — not stored as a separate field |
| `UnitOfMeasure` | `uom` | Unit of measure (e.g. "EA", "BX") |
| `CostCurrency` | `currency` | Cost currency code (e.g. "USD") |
| `DefaultProductClass` | `defaultProductClass` | Product class code |
| `Description` | `description` | Full product description |
| `Status` | `status` | Product status code (e.g. "3000") |
| `PrimarySupplier` | `primarySupplier` | Supplier identifier (e.g. "15087:11") |
| `ShortDescription` | *(ignored)* | Not mapped or stored |

**Metadata field added on enrichment:**

| Field | Description |
|---|---|
| `addonEnrichedAt` | Epoch millis timestamp recording when addon fields were merged |

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `OPENSEARCH_HOST` | `opensearch` | Hostname of the OpenSearch endpoint (no `https://`) |
| `OPENSEARCH_PORT` | `443` | Port for the OpenSearch endpoint |
| `OPENSEARCH_SCHEME` | `https` | `https` or `http` (basic auth only) |
| `OPENSEARCH_AUTH` | `iam` | `iam` for AWS SigV4 signing, `basic` for username/password |
| `OPENSEARCH_USERNAME` | `admin` | Used only when `OPENSEARCH_AUTH=basic` |
| `OPENSEARCH_PASSWORD` | `admin` | Used only when `OPENSEARCH_AUTH=basic` |
| `OPENSEARCH_MODE` | `provisioned` | `provisioned` (managed/self-hosted) |
| `AWS_REGION` | `us-east-1` | AWS region for SigV4 signing |
| `INDEX_PRODUCT` | `products` | Name of the OpenSearch products index |
| `DYNAMODB_TABLE` | `enrichment_cache` | DynamoDB table name for addon cache |
| `DYNAMODB_ENDPOINT` | *(none)* | Optional endpoint override for local DynamoDB |
| `LOG_LEVEL` | `INFO` | Logging level |

---

## Request Format

### Lambda Function URL (primary trigger)

The Lambda is triggered via HTTP POST. The request body must be a JSON object:

```json
{
  "operation": "u",
  "payload": {
    "items": [
      {
        "ItemID": "TPPLATINUM",
        "UnitOfMeasure": "EA",
        "CostCurrency": "USD",
        "DefaultProductClass": "A",
        "Description": "Trivantage Plus Platinum Member",
        "ShortDescription": "Trivantage Plus Platinum Member",
        "Status": "3000",
        "PrimarySupplier": "15087:11"
      }
    ]
  }
}
```

### Direct Invocation (testing / backfill)

```json
{
  "body": {
    "operation": "u",
    "payload": {
      "items": [
        {
          "ItemID": "TPPLATINUM",
          "UnitOfMeasure": "EA",
          "CostCurrency": "USD",
          "DefaultProductClass": "A",
          "Description": "Trivantage Plus Platinum Member",
          "Status": "3000",
          "PrimarySupplier": "15087:11"
        }
      ]
    }
  }
}
```

### Batch Processing

Multiple items can be sent in a single request:

```json
{
  "operation": "u",
  "payload": {
    "items": [
      { "ItemID": "ITEM-001", "UnitOfMeasure": "EA", "Status": "3000" },
      { "ItemID": "ITEM-002", "UnitOfMeasure": "BX", "Status": "1100" }
    ]
  }
}
```

A **flat list** format is also supported:

```json
{
  "operation": "u",
  "payload": [
    { "ItemID": "ITEM-001", "UnitOfMeasure": "EA" },
    { "ItemID": "ITEM-002", "UnitOfMeasure": "BX" }
  ]
}
```

**Request body field reference:**

| Field | Required | Description |
|---|---|---|
| `operation` | ❌ | `"c"` create, `"u"` update (default), `"d"` delete |
| `payload` | ✅ | Object with `items` array, or a flat list of items |
| `payload.items[].ItemID` | ✅ | Maps to `partNumber` — used as the product lookup key |

---

## DynamoDB Cache Design

Uses the existing `enrichment_cache` table (same single-table design as other CDC lambdas):

| Attribute | Value | Description |
|---|---|---|
| `PK` | `PRODUCT_ADDON` | Partition key — entity type |
| `SK` | `<partNumber>` | Sort key — the product's part number |
| `uom` | e.g. `"EA"` | Cached addon field |
| `currency` | e.g. `"USD"` | Cached addon field |
| `defaultProductClass` | e.g. `"A"` | Cached addon field |
| `description` | e.g. `"Trivantage..."` | Cached addon field |
| `status` | e.g. `"3000"` | Cached addon field |
| `primarySupplier` | e.g. `"15087:11"` | Cached addon field |
| `updatedAt` | epoch millis | Timestamp of last cache write |

### Backfill Flow

When `cdc-product-processor` receives a product with a `partNumber`:
1. It queries DynamoDB for `PK=PRODUCT_ADDON, SK=<partNumber>`
2. If found, it merges the cached addon fields into the product payload (only fields not already present)
3. The enriched product is indexed into OpenSearch

---

## Project Structure

```
.
├── lambda_function.py       # Main Lambda handler
├── test_lambda_function.py  # Pytest test suite
├── requirements.txt         # Runtime dependencies
└── README.md                # This file
```

---

## Local Setup & Testing

### Prerequisites
- Python 3.11+
- pip

### 1. Install dependencies
```bash
pip install -r requirements.txt
pip install pytest pytest-cov    # for testing
```

### 2. Set environment variables for local testing
```bash
export OPENSEARCH_HOST=localhost
export OPENSEARCH_PORT=9200
export OPENSEARCH_SCHEME=http
export OPENSEARCH_AUTH=basic
export OPENSEARCH_USERNAME=admin
export OPENSEARCH_PASSWORD=admin
export OPENSEARCH_MODE=provisioned
export AWS_REGION=us-east-1
export DYNAMODB_ENDPOINT=http://localhost:8000
```

---

## Running Tests & Coverage

```bash
# Run all tests with coverage report
pytest test_lambda_function.py -v --cov=lambda_function --cov-report=term-missing

# Generate an HTML coverage report
pytest test_lambda_function.py --cov=lambda_function --cov-report=html
open htmlcov/index.html

# Run only a specific test class
pytest test_lambda_function.py::TestHandleUpsert -v
```

---

## Testing & Validating from the AWS Console

### Step 1 — Test an UPSERT event (product exists in index)

Open the Lambda function in the AWS Console → **Test** tab → create a new test event:

```json
{
  "body": "{\"operation\":\"u\",\"payload\":{\"items\":[{\"ItemID\":\"TPPLATINUM\",\"UnitOfMeasure\":\"EA\",\"CostCurrency\":\"USD\",\"DefaultProductClass\":\"A\",\"Description\":\"Trivantage Plus Platinum Member\",\"Status\":\"3000\",\"PrimarySupplier\":\"15087:11\"}]}}"
}
```

**Expected response (product found and enriched):**
```json
{
  "statusCode": 200,
  "body": {
    "results": [
      { "partNumber": "TPPLATINUM", "status": "enriched", "docId": "..." }
    ]
  }
}
```

**Expected response (product not found — cached for backfill):**
```json
{
  "statusCode": 200,
  "body": {
    "results": [
      { "partNumber": "TPPLATINUM", "status": "cached_for_backfill" }
    ]
  }
}
```

---

### Step 2 — Test a DELETE event

```json
{
  "body": "{\"operation\":\"d\",\"payload\":{\"items\":[{\"ItemID\":\"TPPLATINUM\",\"UnitOfMeasure\":\"EA\",\"Status\":\"3000\"}]}}"
}
```

**Expected response:**
```json
{
  "statusCode": 200,
  "body": {
    "results": [
      { "partNumber": "TPPLATINUM", "status": "addon_deleted" }
    ]
  }
}
```

---

### Step 3 — Verify in OpenSearch

```bash
# Search by partNumber
GET products/_search
{
  "query": {
    "term": { "partNumber.keyword": "TPPLATINUM" }
  },
  "_source": ["partNumber", "uom", "currency", "defaultProductClass", "description", "status", "primarySupplier", "addonEnrichedAt"]
}
```

---

### Step 4 — Verify in DynamoDB

```bash
aws dynamodb get-item \
  --table-name enrichment_cache \
  --key '{"PK": {"S": "PRODUCT_ADDON"}, "SK": {"S": "TPPLATINUM"}}'
```

---

## Deployment Notes

### IAM Permissions Required

```json
{
  "Effect": "Allow",
  "Action": [
    "es:ESHttpGet",
    "es:ESHttpPut",
    "es:ESHttpPost"
  ],
  "Resource": "arn:aws:es:<region>:<account>:domain/<domain-name>/*"
}
```

```json
{
  "Effect": "Allow",
  "Action": [
    "dynamodb:PutItem",
    "dynamodb:GetItem",
    "dynamodb:DeleteItem"
  ],
  "Resource": "arn:aws:dynamodb:<region>:<account>:table/enrichment_cache"
}
```

### Lambda Configuration
- **Runtime:** Python 3.11
- **Memory:** 256 MB minimum
- **Timeout:** 30 seconds minimum (OpenSearch client timeout is 15s)
- **VPC:** Must be in the same VPC as OpenSearch and have access to DynamoDB (via VPC endpoint or NAT)
- **Trigger:** Lambda Function URL

### OpenSearch Index Mapping

Ensure the following fields exist in the `products` index mapping:

```json
PUT /products/_mapping
{
  "properties": {
    "currency": { "type": "keyword" },
    "description": { "type": "text" },
    "addonEnrichedAt": { "type": "date", "format": "epoch_millis" }
  }
}
```

---

## Troubleshooting

| Symptom | Likely Cause | Fix |
|---|---|---|
| `RuntimeError: AWS credentials not found` | Lambda execution role has no IAM credentials | Attach the correct IAM role |
| `403 Forbidden` from OpenSearch | IAM role lacks `es:ESHttp*` permissions | Update the resource-based policy |
| `400 payload.items must be a non-empty list` | Request body missing `items` array | Ensure payload contains `{ "items": [...] }` |
| `400 Request body is missing or could not be parsed.` | Event has no `body` key | Wrap the payload in a `body` field |
| Product not enriched but cached | Product doesn't exist yet in index | Product will be enriched when `cdc-product-processor` indexes it |
| DynamoDB cache write failed | IAM role lacks DynamoDB permissions | Add `dynamodb:PutItem` to the Lambda role |
| Lambda times out | Client timeout (15s) exceeds Lambda timeout | Set Lambda timeout to at least 30 seconds |
