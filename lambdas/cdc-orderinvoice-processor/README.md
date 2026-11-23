# Order Invoice CDC Lambda → OpenSearch

AWS Lambda that consumes **Change Data Capture (CDC)** events from **MSK / Kafka (Debezium)** for the `YFS_ORDER_INVOICE` table and keeps invoice data synchronized in the **OpenSearch orders index**.

The Lambda stores invoice data in a **DynamoDB enrichment cache** first, then performs a partial update on the OpenSearch orders index. This ensures late-arriving invoices (before the order exists) are never lost.

---

## Table of Contents

* [What This Lambda Does](#what-this-lambda-does)
* [Architecture](#architecture)
* [DynamoDB Cache Schema](#dynamodb-cache-schema)
* [OpenSearch Index](#opensearch-index)
* [Environment Variables](#environment-variables)
* [Event Sources](#event-sources)
* [Project Structure](#project-structure)
* [Local Setup & Testing](#local-setup--testing)
* [Running Tests & Coverage](#running-tests--coverage)
* [Testing & Validating from the AWS Console](#testing--validating-from-the-aws-console)
* [Deployment Notes](#deployment-notes)
* [Troubleshooting](#troubleshooting)

---

## What This Lambda Does

This Lambda processes **order invoice CDC events** from Kafka and synchronizes invoice numbers into the **OpenSearch orders index**.

Key capabilities:

• Processes **Debezium CDC events from MSK**
• Supports **manual invocation for testing**
• Handles **Create / Update / Delete operations**
• Stores invoice data in **DynamoDB cache** for late-arriving order enrichment
• Performs **partial updates** on the OpenSearch orders index

Source table columns used:

| Column | Purpose |
|--------|---------|
| `ORDER_HEADER_KEY` | Links invoice to order (DynamoDB SK and OpenSearch doc ID) |
| `INVOICE_NO` | Invoice number stored on the order document |

---

## Architecture

```
Sterling OMS Database
        │
        ▼
   Debezium CDC
        │
        ▼
   Kafka (MSK)  ─── topic: yfs_order_invoice
        │
        ▼
   AWS Lambda (this)
        │
        ├──► DynamoDB Cache (PK=ORDER_INVOICE, SK=ORDER_HEADER_KEY)
        │
        └──► OpenSearch (orders index, partial update invoiceNo)
```

### Late-arriving invoice handling:

1. Invoice CDC event arrives **before** the order → saved to DynamoDB ✅, OpenSearch 404 skipped ⚠️
2. Order arrives later via `cdc-orderheader-processor` → reads invoice from DynamoDB cache → enriches order document ✅

---

## DynamoDB Cache Schema

| PK | SK | Fields |
|----|-----|--------|
| `ORDER_INVOICE` | `{ORDER_HEADER_KEY}` | `invoiceNo`, `updatedAt` |

The `cdc-orderheader-processor` reads this cache via `enrich_order_invoice()` during order ingestion.

---

## OpenSearch Index

**Index name:** `orders` (configured via `INDEX_ORDER` env var)

**Partial update field:** `invoiceNo`

On create/update:
```python
client.update(index="orders", id=order_header_key, body={"doc": {"invoiceNo": "INV-001"}})
```

On delete:
```python
client.update(index="orders", id=order_header_key, body={"doc": {"invoiceNo": null}})
```

---

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DYNAMODB_TABLE` | `enrichment_cache` | DynamoDB table name |
| `DYNAMODB_ENDPOINT` | none | Optional local endpoint (for testing) |
| `OPENSEARCH_HOST` | `opensearch` | OpenSearch endpoint |
| `OPENSEARCH_PORT` | `443` | OpenSearch port |
| `OPENSEARCH_SCHEME` | `https` | `http` or `https` |
| `OPENSEARCH_AUTH` | `iam` | `iam` or `basic` |
| `OPENSEARCH_USERNAME` | `admin` | Used when `OPENSEARCH_AUTH=basic` |
| `OPENSEARCH_PASSWORD` | `admin` | Used when `OPENSEARCH_AUTH=basic` |
| `OPENSEARCH_MODE` | `serverless` | `serverless` or `provisioned` |
| `AWS_REGION` | `us-east-1` | AWS region |
| `INDEX_ORDER` | `orders` | OpenSearch index name |
| `ENABLE_HARD_DELETE` | `false` | `true` = hard delete cache item; `false` = soft delete |

---

## Soft Delete Behaviour

By default (`ENABLE_HARD_DELETE=false`) a `d` CDC event performs a **soft delete** on the DynamoDB cache item:

```python
cache_table.update_item(
    Key={"PK": "ORDER_INVOICE", "SK": order_header_key},
    UpdateExpression="SET #d = :true, #da = :now",
    ConditionExpression="attribute_exists(PK)",  # prevents phantom deletes
    ...
)
```

The `ConditionExpression` ensures that if the invoice was never cached (phantom delete), no record is created.

---

## Event Sources

### 1️⃣ MSK Kafka Events

Topic naming: `<db>.<schema>.yfs_order_invoice`

```json
{
  "records": {
    "topic-0": [
      {
        "value": "BASE64_ENCODED_JSON",
        "topic": "sterling.public.yfs_order_invoice",
        "offset": 1
      }
    ]
  }
}
```

### 2️⃣ Manual Test Event

```json
{
  "table": "YFS_ORDER_INVOICE",
  "operation": "c",
  "payload": {
    "ORDER_HEADER_KEY": "OH123",
    "INVOICE_NO": "INV-2026-0001"
  }
}
```

Operations supported:

| Code | Meaning |
|------|---------|
| c | Create |
| u | Update |
| d | Delete |

---

## Project Structure

```
.
├── lambda_function.py       # Main Lambda handler
├── test_lambda_function.py  # Pytest test suite (80%+ coverage)
├── requirements.txt         # Python dependencies
└── README.md                # This file
```

---

## Local Setup & Testing

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Set environment variables

```bash
export OPENSEARCH_HOST=localhost
export OPENSEARCH_AUTH=basic
export DYNAMODB_TABLE=enrichment_cache
export DYNAMODB_ENDPOINT=http://localhost:8000
```

### 3. Run Lambda locally

```python
import lambda_function

event = {
    "table": "YFS_ORDER_INVOICE",
    "operation": "c",
    "payload": {
        "ORDER_HEADER_KEY": "OH123",
        "INVOICE_NO": "INV-2026-0001"
    }
}

print(lambda_function.lambda_handler(event, None))
```

---

## Running Tests & Coverage

```bash
pytest test_lambda_function.py -v --cov=lambda_function --cov-report=term-missing
```

Tests cover:

* Utility functions
* DynamoDB cache save/delete
* Phantom delete prevention (ConditionExpression)
* OpenSearch partial updates
* MSK event processing
* Debezium message parsing (SMT + standard)
* Error handling

---

## Testing & Validating from the AWS Console

### Step 1 — Create Event

```json
{
  "table": "YFS_ORDER_INVOICE",
  "operation": "c",
  "payload": {
    "ORDER_HEADER_KEY": "TEST-OH-001",
    "INVOICE_NO": "INV-TEST-001"
  }
}
```

Expected response:
```json
{
  "statusCode": 200,
  "body": "Indexed invoice INV-TEST-001 for order TEST-OH-001"
}
```

### Step 2 — Delete Event

```json
{
  "table": "YFS_ORDER_INVOICE",
  "operation": "d",
  "payload": {
    "ORDER_HEADER_KEY": "TEST-OH-001",
    "INVOICE_NO": "INV-TEST-001"
  }
}
```

### Step 3 — Verify in OpenSearch

```bash
GET orders/_doc/TEST-OH-001
```

---

## Deployment Notes

```
Runtime: Python 3.11
Memory: 256 MB
Timeout: 30s
```

Required IAM permissions:

```
es:ESHttp*           (or aoss:APIAccessAll for serverless)
dynamodb:GetItem
dynamodb:PutItem
dynamodb:UpdateItem
dynamodb:DeleteItem
logs:CreateLogStream
logs:PutLogEvents
```

---

## Troubleshooting

| Issue | Possible Cause | Resolution |
|-------|---------------|------------|
| Invoice not appearing on order | Order not yet indexed | Invoice is cached in DynamoDB; will be enriched when order arrives |
| `ConditionalCheckFailedException` in logs | Delete for non-existent invoice | Expected behaviour — phantom delete prevented |
| OpenSearch update skipped (404 in logs) | Order not yet indexed | Expected — invoice is cached in DynamoDB |
| Lambda cannot connect to OpenSearch | Incorrect endpoint or VPC | Verify `OPENSEARCH_HOST`, security groups |
| Missing `ORDER_HEADER_KEY` error | Malformed CDC event | Verify Debezium connector configuration |
