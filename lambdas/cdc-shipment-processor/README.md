# Shipment CDC Lambda → OpenSearch

AWS Lambda that consumes **Change Data Capture (CDC)** events from **MSK / Kafka (Debezium)** for the `YFS_SHIPMENT` table and keeps the **shipment statuses** synchronized in the **OpenSearch orders index**.

The Lambda stores shipment statuses in a **DynamoDB enrichment cache** (`PK=YFS_SHIPMENT, SK=<ORDER_HEADER_KEY>`) using a map of `SHIPMENT_KEY` to `STATUS`. It then computes the unique statuses and performs a partial doc update on OpenSearch. This ensures late-arriving shipments (before the order exists) are never lost, and multiple shipments for the same order are fully supported.

---

## Table of Contents

* [What This Lambda Does](#what-this-lambda-does)
* [Architecture](#architecture)
* [DynamoDB Cache Schema](#dynamodb-cache-schema)
* [OpenSearch Updates](#opensearch-updates)
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

This Lambda processes **YFS_SHIPMENT CDC events** from Kafka and synchronizes the shipment status into the **orders** OpenSearch index.

Key capabilities:

• Processes **Debezium CDC events from MSK**
• Supports **manual invocation for testing**
• Handles **Create / Update / Delete operations**
• Stores shipment statuses in a **DynamoDB cache map** (`shipmentStatuses`) to handle multiple shipments per order
• Extracts the unique list of statuses from the map on every change
• Performs **partial doc updates** on OpenSearch orders index
• Updates the **`shipmentStatus`** field on the order document with an array of unique statuses

Source table columns used:

| Column | Purpose |
|--------|---------|
| `ORDER_HEADER_KEY` | Links shipment to order (used as DynamoDB SK and OpenSearch doc id) |
| `SHIPMENT_KEY` | Uniquely identifies the shipment (used as map key in DynamoDB) |
| `STATUS` | The shipment status value |

---

## Architecture

```
Sterling OMS Database
        │
        ▼
   Debezium CDC
        │
        ▼
   Kafka (MSK)  ─── topic: yfs_shipment
        │
        ▼
    AWS Lambda (this)
        │
        ├──► DynamoDB Cache
        │      └── PK=YFS_SHIPMENT, SK=ORDER_HEADER_KEY  → shipmentStatuses (Map of SHIPMENT_KEY -> STATUS)
        │
        └──► OpenSearch
               └── orders index  → shipmentStatus (array of unique statuses via partial update)
```

### Late-arriving shipment handling:

1. Shipment CDC event arrives **before** the order → saved to DynamoDB ✅, OpenSearch 404 skipped ⚠️
2. Order arrives later via `cdc-orderheader-processor` → reads shipment status from DynamoDB cache → enriches order document ✅

---

## DynamoDB Cache Schema

| PK | SK | Fields | Consumers |
|----|-----|--------|-----------|
| `YFS_SHIPMENT` | `{ORDER_HEADER_KEY}` | `shipmentStatuses` (Map), `updatedAt` | `cdc-orderheader-processor` |

---

## OpenSearch Updates

### Create / Update

Uses a simple partial doc update to set the `shipmentStatus` field with an array:

```json
{
  "doc": {
    "shipmentStatus": ["1100.0500", "1400.0000"]
  }
}
```

### Delete

When a shipment is deleted, it is removed from the `shipmentStatuses` map. If the array of remaining statuses becomes empty, it is set to `null` in OpenSearch:

```json
{
  "doc": {
    "shipmentStatus": null
  }
}
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
| `INDEX_ORDERS` | `orders` | OpenSearch orders index name |
| `ENABLE_HARD_DELETE` | `false` | `true` to hard-delete DynamoDB items |

---

## Phantom Delete Prevention

Delete operations on the DynamoDB cache use `ConditionExpression="attribute_exists(PK)"` to prevent creating phantom records when the item doesn't exist:

```python
cache_table.update_item(
    Key={"PK": entity, "SK": key},
    UpdateExpression="SET #d = :true, #da = :now",
    ConditionExpression="attribute_exists(PK)",
    ...
)
```

If the item doesn't exist, `ConditionalCheckFailedException` is caught and logged as an info-level skip.

---

## Event Sources

### 1️⃣ MSK Kafka Events

Topic naming: `<db>.<schema>.yfs_shipment`

### 2️⃣ Manual Test Event

```json
{
  "table": "YFS_SHIPMENT",
  "operation": "c",
  "payload": {
    "ORDER_HEADER_KEY": "OH123",
    "SHIPMENT_KEY": "SHP123",
    "STATUS": "1100.0500"
  }
}
```

Operations supported:

| Code | Meaning | DynamoDB | OpenSearch |
|------|---------|----------|------------|
| c | Create | Add/update `SHIPMENT_KEY` in `shipmentStatuses` map | Partial doc update with `shipmentStatus` array |
| u | Update | Add/update `SHIPMENT_KEY` in `shipmentStatuses` map | Partial doc update with `shipmentStatus` array |
| d | Delete | Remove `SHIPMENT_KEY` from `shipmentStatuses` map | Partial doc update with remaining `shipmentStatus` array |

---

## Project Structure

```
.
├── lambda_function.py       # Main Lambda handler
├── test_lambda_function.py  # Pytest test suite
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

---

## Running Tests & Coverage

```bash
pytest test_lambda_function.py -v --cov=lambda_function --cov-report=term-missing
```

Tests cover:

* DynamoDB cache map upserts and removes
* Phantom delete prevention (ConditionExpression)
* OpenSearch partial doc update for shipmentStatus array
* Extracting unique values from shipmentStatuses map
* MSK batch event processing
* Debezium message parsing (SMT + standard)
* Edge cases (missing ORDER_HEADER_KEY, null STATUS)

---

## Testing & Validating from the AWS Console

### Step 1 — Create a shipment status

```json
{
  "table": "YFS_SHIPMENT",
  "operation": "c",
  "payload": {
    "ORDER_HEADER_KEY": "TEST-OH-001",
    "SHIPMENT_KEY": "TEST-SHP-001",
    "STATUS": "1100.0500"
  }
}
```

Expected response:
```json
{
  "statusCode": 200,
  "body": "Indexed shipment status for order TEST-OH-001"
}
```

### Step 2 — Create a second shipment for the same order

```json
{
  "table": "YFS_SHIPMENT",
  "operation": "c",
  "payload": {
    "ORDER_HEADER_KEY": "TEST-OH-001",
    "SHIPMENT_KEY": "TEST-SHP-002",
    "STATUS": "3700"
  }
}
```

### Step 3 — Delete a shipment

```json
{
  "table": "YFS_SHIPMENT",
  "operation": "d",
  "payload": {
    "ORDER_HEADER_KEY": "TEST-OH-001",
    "SHIPMENT_KEY": "TEST-SHP-002"
  }
}
```

### Step 4 — Verify in OpenSearch

```bash
GET orders/_doc/TEST-OH-001

# Expected shipmentStatus: ["1100.0500", "3700"] (after step 2)
# Expected shipmentStatus: ["1100.0500"] (after step 3)
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
dynamodb:PutItem
dynamodb:UpdateItem
dynamodb:DeleteItem
dynamodb:GetItem
logs:CreateLogStream
logs:PutLogEvents
```

---

## Troubleshooting

| Issue | Possible Cause | Resolution |
|-------|---------------|------------|
| Shipment status not on order | Order not yet indexed | Shipment status cached in DynamoDB; enriched when order arrives |
| `ConditionalCheckFailedException` in logs | Delete for non-existent item | Expected — phantom delete prevented |
| OpenSearch update skipped (404) | Order not yet indexed | Expected — cached in DynamoDB |
| `shipmentStatus` is null after update | Delete event processed after update | Check CDC event ordering |
