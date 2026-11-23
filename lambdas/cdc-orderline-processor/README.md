# Order Lines Lambda Handler

A Python AWS Lambda that consumes **Change Data Capture (CDC)** events for the `YFS_ORDER_LINE` table (Sterling OMS) and keeps a dedicated **OpenSearch index** (`order-lines`) in sync in real time.

---

## Table of Contents

1. [What This Lambda Does](#what-this-lambda-does)
2. [Architecture](#architecture)
3. [OpenSearch Index](#opensearch-index)
4. [Environment Variables](#environment-variables)
5. [OpenSearch Modes](#opensearch-modes)
6. [Event Sources](#event-sources)
7. [Project Structure](#project-structure)
8. [Local Setup & Testing](#local-setup--testing)
9. [Running Tests & Coverage](#running-tests--coverage)
10. [Testing & Validating from the AWS Console](#testing--validating-from-the-aws-console)
11. [Deployment Notes](#deployment-notes)
12. [Troubleshooting](#troubleshooting)

---

## What This Lambda Does

```
Sterling OMS DB  →  Debezium CDC  →  Amazon MSK (Kafka)  →  This Lambda  →  OpenSearch (order-lines index)
```

| CDC Operation | What happens in OpenSearch |
|---|---|
| `c` — INSERT | Document created via `doc_as_upsert` |
| `u` — UPDATE | Document merged/patched (existing fields not in payload are preserved) |
| `r` — Snapshot read | Treated identically to INSERT |
| `d` — DELETE | Document removed from the index (404 is silently ignored) |

**Key design decisions:**
- Each order line is stored as a **flat, top-level document** — not nested inside an order header. This gives full-text search over every field on every line.
- The order header index is **not touched** by this Lambda.
- A `searchableText` field is built by concatenating item identifiers, descriptions, and codes — making free-text search across all key fields possible with a single query.
- Column names from the DB payload are **normalised to uppercase** before processing so case differences in CDC messages never cause silent misses.
- Order lines are **enriched with hold types** from the DynamoDB cache (`ORDER_LINE_HOLD_TYPE` entity).
- Order lines are **enriched with status** from the DynamoDB cache (`ORDER_STATUS` entity) based on `orderHeaderKey` and matched by `orderLineKey`.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        AWS Lambda                               │
│                                                                 │
│  lambda_handler()  ──────────────────────────────────┐         │
│       │                                              │         │
│       ├─ "records" key present? ──► process_msk_event()        │
│       │       │                                      │         │
│       │       └─ for each Kafka message:             │         │
│       │              decode base64 → parse JSON      │         │
│       │              extract op / table / payload    │         │
│       │              └──► process_single_event()     │         │
│       │                                              │         │
│       └─ direct JSON payload? ──────────────────────►│         │
│                                         process_single_event() │
│                                              │                  │
│                                    process_order_line_event()  │
│                                         │         │            │
│                                    upsert      delete          │
│                                         │         │            │
│                                    OpenSearch (order-lines)    │
└─────────────────────────────────────────────────────────────────┘
```

---

## OpenSearch Index

**Index name:** `order-lines`

**Document ID:** `ORDER_LINE_KEY` (unique per order line)

**Sample document:**
```json
{
  "orderLineKey":       "OL-00123456",
  "orderHeaderKey":     "OH-00098765",
  "primeLineNo":        1,
  "subLineNo":          0,
  "lineType":           "PRODUCT",
  "itemId":             "SKU-XYZ",
  "itemDescription":    "Widget Pro 2000",
  "unitPrice":          29.99,
  "orderedQty":         3.0,
  "shipnodeKey":        "WH-EAST",
  "deliveryMethod":     "SHP",
  "searchableText":     "SKU-XYZ Widget Pro 2000 111-222-3333",
  "holdTypes":          ["FRAUD_REVIEW", "CREDIT_HOLD"],
  "statusName":         "Shipped",
  "statusCode":         "3700",
  "cdcOperation":       "u",
  "cdcTimestamp":       1716300000000,
  "indexedAt":          1716300001234
}
```

**For provisioned OpenSearch**, the index is **automatically created** on the first cold start with:
- 3 primary shards, 1 replica
- Explicit field mappings (keyword, text, double, date types)
- 5-second refresh interval

**For serverless OpenSearch (AOSS)**, index lifecycle is managed by AWS — no explicit creation needed.

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `OPENSEARCH_HOST` | `opensearch` | Hostname of the OpenSearch endpoint (no `https://`) |
| `OPENSEARCH_PORT` | `443` | Port (443 for both serverless and provisioned HTTPS) |
| `OPENSEARCH_SCHEME` | `https` | `https` or `http` (basic auth only) |
| `OPENSEARCH_AUTH` | `iam` | `iam` for AWS SigV4 signing, `basic` for username/password |
| `OPENSEARCH_USERNAME` | `admin` | Used only when `OPENSEARCH_AUTH=basic` |
| `OPENSEARCH_PASSWORD` | `admin` | Used only when `OPENSEARCH_AUTH=basic` |
| `OPENSEARCH_MODE` | `serverless` | `serverless` (AOSS) or `provisioned` (managed/self-hosted) |
| `AWS_REGION` | `us-east-1` | AWS region for SigV4 signing |
| `INDEX_ORDER_LINES` | `order-lines` | Name of the OpenSearch index where order line documents are written. Override per environment |
| `ENABLE_HARD_DELETE` | `false` | Set to `true` to physically delete; `false` uses soft delete |
| `DYNAMODB_TABLE` | `enrichment_cache` | DynamoDB table for hold type enrichment cache |
| `DYNAMODB_ENDPOINT` | none | Optional local DynamoDB endpoint (for testing) |

---

## Soft Delete Behaviour

By default (`ENABLE_HARD_DELETE=false`) a `d` CDC event performs a **soft delete** — the order line document is **updated** rather than removed.

### OpenSearch — `soft_delete_os_doc`

```python
client.update(
    index=INDEX_ORDER_LINES,
    id=order_line_key,
    body={"doc": {"_deleted": True, "_deletedAt": int(datetime.now(UTC).timestamp() * 1000)}}
)
```

The document remains in the `order-lines` index. To exclude soft-deleted lines from search results, add a filter:

```json
{ "term": { "_deleted": false } }
```

> Previously this lambda had **no DynamoDB dependency** — now it reads hold types (`ORDER_LINE_HOLD_TYPE`) and order status (`ORDER_STATUS`) from the cache entity during enrichment. Soft delete only affects the OpenSearch document.

### Hard Delete

Set `ENABLE_HARD_DELETE=true` to physically remove the document:

```python
client.delete(index=INDEX_ORDER_LINES, id=order_line_key, ignore=[404])
```

### Direct Invocation — Delete Payload

```json
{
  "table": "YFS_ORDER_LINE",
  "operation": "d",
  "payload": {
    "ORDER_LINE_KEY": "<orderLineKey>",
    "ORDER_HEADER_KEY": "<orderHeaderKey>"
  }
}
```

## OpenSearch Modes

### Serverless (Amazon OpenSearch Serverless — AOSS)
```
OPENSEARCH_AUTH = iam
OPENSEARCH_MODE = serverless
```
- SigV4 signing uses service `aoss`
- Index and collection lifecycle managed by AWS
- Lambda execution role needs `aoss:APIAccessAll` on the collection

### Provisioned (Amazon OpenSearch Service or self-managed)
```
OPENSEARCH_AUTH = iam
OPENSEARCH_MODE = provisioned
```
- SigV4 signing uses service `es`
- **Index is auto-created** on cold start if it doesn't exist (with full mappings)
- Lambda execution role needs `es:ESHttpGet`, `es:ESHttpPut`, `es:ESHttpPost`, `es:ESHttpDelete` on the domain ARN

### Basic Auth (local / non-AWS OpenSearch)
```
OPENSEARCH_AUTH = basic
OPENSEARCH_USERNAME = admin
OPENSEARCH_PASSWORD = yourpassword
```
- Useful for local development with Docker-based OpenSearch

---

## Event Sources

### 1. Amazon MSK Trigger (primary)
The Lambda is triggered by an MSK event source mapping. Each Kafka message value is a **base64-encoded Debezium JSON** event:

```json
{
  "op": "u",
  "before": { "ORDER_LINE_KEY": "OL123", ... },
  "after":  { "ORDER_LINE_KEY": "OL123", "UNIT_PRICE": "15.00", ... },
  "ts_ms":  1716300000000
}
```

Topic naming convention: `<db>.<schema>.yfs_order_line`
(e.g. `sterling.public.yfs_order_line`)

### 2. Direct Invocation (testing / backfill)
```json
{
  "table": "YFS_ORDER_LINE",
  "operation": "c",
  "sourceTimestamp": 1716300000000,
  "payload": {
    "ORDER_LINE_KEY":   "OL123",
    "ORDER_HEADER_KEY": "OH456",
    "ITEM_ID":          "SKU-XYZ",
    "UNIT_PRICE":       "29.99",
    "ORDERED_QTY":      "2.0"
  }
}
```

---

## Project Structure

```
.
├── lambda_function.py       # Main Lambda handler (all logic lives here)
├── test_lambda_function.py # Pytest test suite (80%+ coverage)
├── requirements.txt        # Runtime + test dependencies
└── README.md               # This file
```

---

## Local Setup & Testing

### Prerequisites
- Python 3.11+
- pip

### 1. Install dependencies
```bash
pip install -r requirements.txt
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
```

### 3. (Optional) Start local OpenSearch with Docker
```bash
docker run -d \
  --name opensearch-local \
  -p 9200:9200 -p 9600:9600 \
  -e "discovery.type=single-node" \
  -e "OPENSEARCH_INITIAL_ADMIN_PASSWORD=Admin@1234" \
  opensearchproject/opensearch:2.13.0
```

---

## Running Tests & Coverage

```bash
# Run all tests with coverage report
pytest test_lambda_function.py -v --cov=lambda_function --cov-report=term-missing

# Generate an HTML coverage report (opens in browser)
pytest test_lambda_function.py --cov=lambda_function --cov-report=html
open htmlcov/index.html
```

**Expected output (≥80% coverage):**

```
---------- coverage: platform ... ----------
Name                Stmts   Miss  Cover
---------------------------------------
lambda_function.py     180     28    84%
---------------------------------------
TOTAL                 180     28    84%
```

---

## Testing & Validating from the AWS Console

### Step 1 — Test a CREATE event (direct invocation)

Open the Lambda function in the AWS Console → **Test** tab → create a new test event:

```json
{
  "table": "YFS_ORDER_LINE",
  "operation": "c",
  "sourceTimestamp": 1716300000000,
  "payload": {
    "ORDER_LINE_KEY":   "TEST-OL-001",
    "ORDER_HEADER_KEY": "TEST-OH-001",
    "ITEM_ID":          "SKU-TEST-123",
    "ITEM_DESCRIPTION": "Console Test Item",
    "PRIME_LINE_NO":    "1",
    "SUB_LINE_NO":      "0",
    "UNIT_PRICE":       "49.99",
    "ORDERED_QTY":      "1.0",
    "UOM":              "EA"
  }
}
```

**Expected response:**
```json
{
  "statusCode": 200,
  "body": "Upserted order line: TEST-OL-001"
}
```

---

### Step 2 — Test an UPDATE event

```json
{
  "table": "YFS_ORDER_LINE",
  "operation": "u",
  "sourceTimestamp": 1716300001000,
  "payload": {
    "ORDER_LINE_KEY":   "TEST-OL-001",
    "ORDER_HEADER_KEY": "TEST-OH-001",
    "ITEM_ID":          "SKU-TEST-123",
    "UNIT_PRICE":       "39.99",
    "ORDERED_QTY":      "3.0"
  }
}
```

---

### Step 3 — Test a DELETE event

```json
{
  "table": "YFS_ORDER_LINE",
  "operation": "d",
  "sourceTimestamp": 1716300002000,
  "payload": {
    "ORDER_LINE_KEY":   "TEST-OL-001",
    "ORDER_HEADER_KEY": "TEST-OH-001"
  }
}
```

**Expected response:**
```json
{
  "statusCode": 200,
  "body": "Deleted order line: TEST-OL-001"
}
```

---

### Step 4 — Test an MSK (Kafka) event

Simulate what MSK sends by base64-encoding a Debezium event. Use this test payload:

```json
{
  "eventSource": "aws:kafka",
  "records": {
    "sterling.public.yfs_order_line-0": [
      {
        "topic": "sterling.public.yfs_order_line",
        "offset": 100,
        "value": "eyJvcCI6ICJjIiwgImFmdGVyIjogeyJPUkRFUl9MSU5FX0tFWSI6ICJNU0stT0wtMDAxIiwgIk9SREVSX0hFQURFUl9LRVkiOiAiTVNLLU9ILTAwMSIsICJJVEVNX0lEIjogIlNLVS1NU0siLCAiSVRFTV9ERVNDUklQVElPTiI6ICJNU0sgVGVzdCBJdGVtIiwgIlVOSVRfUFJJQ0UiOiAiMTkuOTkiLCAiT1JERVJFRF9RVFkiOiAiMi4wIn0sICJ0c19tcyI6IDE3MTYzMDAwMDAwMDB9"
      }
    ]
  }
}
```

> **Note:** The `value` field above decodes to:
> ```json
> {"op": "c", "after": {"ORDER_LINE_KEY": "MSK-OL-001", "ORDER_HEADER_KEY": "MSK-OH-001", "ITEM_ID": "SKU-MSK", "ITEM_DESCRIPTION": "MSK Test Item", "UNIT_PRICE": "19.99", "ORDERED_QTY": "2.0"}, "ts_ms": 1716300000000}
> ```

---

### Step 5 — Verify the document in OpenSearch

After a successful CREATE or UPDATE, verify the document was indexed by querying OpenSearch directly (via Dev Tools in OpenSearch Dashboards, or curl):

```bash
# Check the document exists
GET order-lines/_doc/TEST-OL-001

# Search by item ID
GET order-lines/_search
{
  "query": {
    "term": { "itemId": "SKU-TEST-123" }
  }
}

# Full-text search using searchableText
GET order-lines/_search
{
  "query": {
    "match": { "searchableText": "Console Test Item" }
  }
}
```

---

### Step 6 — Check CloudWatch Logs

After any test invocation, go to **CloudWatch → Log groups → `/aws/lambda/<function-name>`** to see:

- DNS resolution results (private vs public IP)
- OpenSearch client startup diagnostics
- Per-record processing logs (`Record decoded`, `Document ready`, `OpenSearch upsert OK`)
- Any errors with full stack traces

---

## Deployment Notes

### IAM Permissions Required

```json
{
  "Effect": "Allow",
  "Action": [
    "es:ESHttpGet",
    "es:ESHttpPut",
    "es:ESHttpPost",
    "es:ESHttpDelete"
  ],
  "Resource": "arn:aws:es:<region>:<account>:domain/<domain-name>/*"
}
```

For **serverless (AOSS)**:
```json
{
  "Effect": "Allow",
  "Action": "aoss:APIAccessAll",
  "Resource": "arn:aws:aoss:<region>:<account>:collection/<collection-id>"
}
```

### MSK Trigger Configuration
- **Starting position:** `LATEST` (or `TRIM_HORIZON` for full backfill)
- **Batch size:** 100–500 records recommended
- **Bisect on error:** Enable for production to isolate poison-pill messages
- **Destination on failure:** Configure a Dead Letter Queue (SQS or SNS)

### Lambda Configuration
- **Runtime:** Python 3.11
- **Memory:** 256 MB minimum (512 MB recommended for large batches)
- **Timeout:** 5 minutes (for large MSK batches)
- **VPC:** Must be in the same VPC as the OpenSearch domain (provisioned) or have internet access (serverless via VPC endpoint)

---

## Troubleshooting

| Symptom | Likely Cause | Fix |
|---|---|---|
| `RuntimeError: AWS credentials not found` | Lambda execution role has no IAM credentials | Attach the correct IAM role to the Lambda |
| `DNS resolution skipped or failed` | Lambda is not in the correct VPC/subnet | Place Lambda in the same VPC as OpenSearch |
| `Public IP` warning in logs | OpenSearch endpoint resolves to a public IP | Use a VPC endpoint or private-link for the OpenSearch domain |
| `403 Forbidden` from OpenSearch | IAM role lacks `es:ESHttp*` permissions | Update the resource-based policy on the OpenSearch domain |
| `Errors occurred during batch processing` | One or more Kafka messages failed | Check CloudWatch logs for the specific `offset` that failed |
| `400 Missing ORDER_LINE_KEY` | CDC payload has no primary key | Verify the Debezium connector is configured to include all PK columns |
| `holdTypes` missing on order line | Hold types not yet cached in DynamoDB | Hold types will be enriched when order line is re-ingested or when hold type lambda runs |
| `statusName` missing on order line | Status not yet cached in DynamoDB | Status will be enriched when order line is re-ingested or when status lambda runs |
| Index not created on cold start | Running in serverless mode | Serverless manages index lifecycle — switch `OPENSEARCH_MODE=provisioned` if using managed OpenSearch |
