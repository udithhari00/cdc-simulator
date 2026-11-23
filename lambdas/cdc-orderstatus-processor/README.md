# Order Status Update Lambda

AWS Lambda triggered via **Lambda Function URL**. Receives order status updates (with per-line enrichment data) from the **BFF**, persists them to a **DynamoDB cache**, and immediately reflects the change on the corresponding document in the **OpenSearch orders index** (`statusName`, `statusCode`) and updates per-line statuses directly in the **OpenSearch order-lines index** (`statusName`, `statusCode`).

---

# Table of Contents

* [What This Lambda Does](#what-this-lambda-does)
* [Architecture](#architecture)
* [Request Format](#request-format)
* [DynamoDB Cache](#dynamodb-cache)
* [OpenSearch Index Update](#opensearch-index-update)
* [Environment Variables](#environment-variables)
* [Operations](#operations)
* [Late-Arriving Order Events](#late-arriving-order-events)
* [Project Structure](#project-structure)
* [Local Setup & Testing](#local-setup--testing)
* [Running Tests & Coverage](#running-tests--coverage)
* [Testing & Validating from the AWS Console](#testing--validating-from-the-aws-console)
* [Deployment Notes](#deployment-notes)
* [Troubleshooting](#troubleshooting)

---

# What This Lambda Does

This Lambda receives order status updates from the BFF and performs three operations on every invocation:

1. **Writes the status to DynamoDB** — acts as a durable cache so status is available for future order ingestion events
2. **Partially updates the OpenSearch orders index** — patches `statusName` (and optionally `statusCode`) on the existing order document without touching any other fields
3. **Updates per-line statuses in the OpenSearch order-lines index** — patches `statusName` and `statusCode` on each individual order-line document using data from `enrich.orderlinekey`

For delete operations, it removes the status from DynamoDB, nulls out the status fields in the orders index, and clears per-line statuses in the order-lines index.

---

# Architecture

```
BFF
 │
 │  HTTP POST (Function URL)
 ▼
AWS Lambda (order-status-update)
 │
 ├──► DynamoDB Cache
 │     PK = ORDER_STATUS
 │     SK = orderHeaderKey
 │
 ├──► OpenSearch (orders index)
 │     Partial update on document id = orderHeaderKey
 │     Sets statusName (and optionally statusCode)
 │
 └──► OpenSearch (order-lines index)
       Partial update on each order-line document
       Sets statusName, statusCode per line from enrich.orderlinekey
```

---

# Request Format

All requests are HTTP POST to the Lambda Function URL with a JSON body.

### Create / Update

```json
{
  "operation": "c",
  "payload": {
    "orderHeaderKey": "202406111239006158392735",
    "statusName": "Shipped",
    "statusCode": "3700",
    "enrich": {
      "orderlinekey": [
        {"OrderLineKey": "202605201418156749242990", "Status": "Created"},
        {"OrderLineKey": "202605201418156749242991", "Status": "Shipped"},
        {"OrderLineKey": "202605201418156749242992", "Status": "Created"}
      ]
    }
  }
}
```

### Delete

```json
{
  "operation": "d",
  "payload": {
    "orderHeaderKey": "202406111239006158392735"
  }
}
```

Notes:
- `operation` defaults to `u` if omitted
- `statusName` is required for `c` and `u`
- `statusCode`, `enrich`, and `enrich.orderlinekey` are **optional**
- If provided, each entry in `enrich.orderlinekey` must have `OrderLineKey` and `Status`
- Only `orderHeaderKey` is required for `d`
- The body can be a plain JSON string, a base64-encoded JSON string, or a raw dict

---

# DynamoDB Cache

Status is stored in the shared enrichment cache table:

| Attribute | Value |
|---|---|
| PK | `ORDER_STATUS` |
| SK | `<orderHeaderKey>` |
| statusName | e.g. `Shipped` |
| statusCode | (optional) e.g. `3700` |
| enrich | `{"orderlinekey": [{"OrderLineKey": "...", "Status": "..."}]}` |
| updatedAt | epoch milliseconds |

One record per order. Since `orderHeaderKey` is unique, overwriting on update is intentional.

---

# OpenSearch Index Update

The Lambda performs **partial updates** — only the relevant status fields are patched. All other fields on the documents are untouched.

### Orders Index — Create / Update patch:
```json
{
  "doc": {
    "statusName": "Shipped",
    "statusCode": "3700"
  }
}
```

### Order-Lines Index — Per-line update (for each entry in `enrich.orderlinekey`):
```json
{
  "doc": {
    "statusName": "Shipped",
    "statusCode": "3700"
  }
}
```

### Orders Index — Delete patch:
```json
{
  "doc": {
    "statusCode": null,
    "statusName": null
  }
}
```

### Order-Lines Index — Delete patch (for each line):
```json
{
  "doc": {
    "statusCode": null,
    "statusName": null
  }
}
```

If the order document does not yet exist in the index (status arrives before the order CDC event), the OpenSearch error is caught and logged as a warning. The request still returns 200 because the status is already safely written to DynamoDB. Similarly, if an order-line document does not yet exist, the per-line update is logged as a warning and skipped.

---

# Environment Variables

| Variable | Description | Default |
|---|---|---|
| DYNAMODB_TABLE | DynamoDB enrichment cache table | enrichment_cache |
| DYNAMODB_ENDPOINT | Local DynamoDB endpoint (for testing) | none |
| OPENSEARCH_HOST | OpenSearch endpoint | opensearch |
| OPENSEARCH_PORT | OpenSearch port | 443 |
| OPENSEARCH_SCHEME | http or https | https |
| OPENSEARCH_AUTH | Authentication mode: `iam` or `basic` | iam |
| OPENSEARCH_USERNAME | Basic auth username | admin |
| OPENSEARCH_PASSWORD | Basic auth password | admin |
| OPENSEARCH_MODE | `serverless` or `provisioned` | serverless |
| AWS_REGION | AWS region | us-east-1 |
| INDEX_ORDER | OpenSearch orders index name | orders |
| INDEX_ORDER_LINES | OpenSearch order-lines index name | order-lines |
| ENABLE_HARD_DELETE | `true` = physical DynamoDB delete; `false` = soft delete | false |

---

# Soft Delete Behaviour

By default (`ENABLE_HARD_DELETE=false`) a `d` operation performs a **soft delete** on the DynamoDB `ORDER_STATUS` cache row rather than physically removing it.

### DynamoDB — `soft_delete_cache_item`

```python
cache_table.update_item(
    Key={'PK': 'ORDER_STATUS', 'SK': order_header_key},
    UpdateExpression='SET #d = :true, #da = :now',
    ExpressionAttributeNames={'#d': '_deleted', '#da': '_deletedAt'},
    ExpressionAttributeValues={':true': True, ':now': datetime.now(UTC).isoformat()},
)
```

### OpenSearch (unchanged regardless of soft/hard delete)

The OpenSearch partial update always **nulls out** `statusCode` and `statusName` on the order document, and clears per-line `statusCode`/`statusName` on order-line documents:

```json
{ "doc": { "statusCode": null, "statusName": null } }
```

This is consistent whether soft or hard delete is active — the status is cleared from the visible documents in both cases.

### Hard Delete

Set `ENABLE_HARD_DELETE=true` to physically remove the DynamoDB `ORDER_STATUS` row:

```python
cache_table.delete_item(Key={'PK': 'ORDER_STATUS', 'SK': order_header_key})
```

---

# Operations

| operation | DynamoDB | OpenSearch (orders) | OpenSearch (order-lines) |
|---|---|---|---|
| `c` | `put_item` (upsert) | patch `statusName` (and optionally `statusCode`) | patch per-line `statusName`, `statusCode` |
| `u` | `put_item` (upsert) | patch `statusName` (and optionally `statusCode`) | patch per-line `statusName`, `statusCode` |
| `d` | soft delete (mark `_deleted=true`) or `delete_item` | set `statusCode`, `statusName` to null | set per-line `statusCode`, `statusName` to null |

`c` and `u` follow the same code path — both are upserts.

---

# Late-Arriving Order Events

This Lambda and the `cdc-orderheader-processor` are designed to handle either event arriving first:

**Status arrives before the order:**
1. Status is saved to DynamoDB by this Lambda
2. OpenSearch orders update is skipped (order doc doesn't exist yet) — logged as a warning
3. OpenSearch order-lines updates may also be skipped if the line docs don't exist yet
4. When the order CDC event later arrives, `cdc-orderheader-processor` reads `ORDER_STATUS` from DynamoDB and stamps `statusName` (and optionally `statusCode`) onto the order document at index time
5. When order-line CDC events arrive, `cdc-orderline-processor` reads `ORDER_STATUS` from DynamoDB and stamps per-line `statusName`/`statusCode` onto each order-line document

**Order arrives before the status:**
1. Order is indexed with `statusName: null` and `statusCode: null`
2. Order lines are indexed without `statusName`/`statusCode`
3. When the status update arrives, this Lambda patches the existing order document and each order-line document directly in OpenSearch

---

# Project Structure

```
order-status-update/
│
├── lambda_function.py      Main Lambda implementation
├── test_lambda_function.py Pytest test suite
├── requirements.txt        Python dependencies
└── README.md               This file
```

---

# Local Setup & Testing

### 1. Install dependencies

```
pip install -r requirements.txt
```

### 2. Set environment variables

```
export OPENSEARCH_HOST=localhost
export OPENSEARCH_AUTH=basic
export DYNAMODB_TABLE=enrichment_cache
export DYNAMODB_ENDPOINT=http://localhost:8000
export INDEX_ORDER=orders
```

### 3. Invoke locally

```python
import lambda_function

event = {
    "body": json.dumps({
        "operation": "u",
        "payload": {
            "orderHeaderKey": "OHK-001",
            "statusName": "Shipped",
            "enrich": {
                "orderlinekey": [
                    {"OrderLineKey": "LINE-001", "Status": "Shipped"}
                ]
            }
        }
    })
}

print(lambda_function.lambda_handler(event, None))
```

---

# Running Tests & Coverage

Run tests:

```
pytest test_lambda_function.py -v
```

Run with coverage:

```
pytest test_lambda_function.py \
  --cov=lambda_function \
  --cov-report=term-missing
```

Tests cover:

* Body parsing (JSON string, base64, raw dict)
* Input validation for all operations
* DynamoDB save and delete
* OpenSearch update and clear
* OpenSearch unavailable / document missing (graceful handling)
* Create, update, and delete operation paths

---

# Testing & Validating from the AWS Console

### Step 1

Open **AWS Lambda Console** and select the `order-status-update` function.

### Step 2

Create a **Test Event** with the following payload:

```json
{
  "body": "{\"operation\": \"u\", \"payload\": {\"orderHeaderKey\": \"202406111239006158392735\", \"statusName\": \"Shipped\", \"enrich\": {\"orderlinekey\": [{\"OrderLineKey\": \"LINE-001\", \"Status\": \"Shipped\"}]}}}"
}
```

### Step 3

Click **Test**.

Expected response:

```json
{
  "statusCode": 200,
  "body": "{\"orderHeaderKey\": \"202406111239006158392735\", \"statusName\": \"Shipped\", \"enrich\": {\"orderlinekey\": [{\"OrderLineKey\": \"LINE-001\", \"Status\": \"Shipped\"}]}}"
}
```

### Step 4

Verify in OpenSearch:

```
GET orders/_doc/202406111239006158392735
```

The document should have:

```json
{
  "statusName": "Shipped"
}
```

And each order-line document should have:

```json
{
  "statusName": "Shipped",
  "statusCode": "..."
}
```

---

# Deployment Notes

Recommended configuration:

```
Runtime:  Python 3.11
Memory:   256 MB
Timeout:  15s
Trigger:  Lambda Function URL (no auth or IAM auth depending on environment)
```

Required IAM permissions:

```
dynamodb:PutItem
dynamodb:UpdateItem
dynamodb:DeleteItem
es:ESHttp*          (provisioned OpenSearch)
aoss:APIAccessAll   (serverless OpenSearch)
logs:CreateLogStream
logs:PutLogEvents
```

---

# Troubleshooting

| Issue | Possible Cause | Resolution |
|---|---|---|
| 400 — Missing orderHeaderKey | `orderHeaderKey` not in payload | Verify the BFF is sending the correct payload structure |
| 400 — Missing statusName | `statusName` absent on `c`/`u` | Include `statusName` for non-delete operations |
| 400 — Invalid enrich | `enrich` is not a dict | Ensure `enrich` is a valid JSON object if provided |
| 400 — Invalid orderlinekey | `orderlinekey` is not a list | Ensure `orderlinekey` is a list if provided |
| 400 — Invalid orderlinekey entry | Entry missing `OrderLineKey` or `Status` | Each object in `enrich.orderlinekey` must have both fields |
| OpenSearch update skipped (warning) | Order doc not yet in index | Expected for late-arriving status — will be applied on next order ingestion |
| DynamoDB error | Permissions or endpoint misconfigured | Verify `DYNAMODB_TABLE`, IAM role has `dynamodb:PutItem` / `dynamodb:DeleteItem` |
| Authentication error | IAM role missing permissions | Add `es:ESHttp*` or `aoss:APIAccessAll` |
| Status not reflected after ingestion | DynamoDB write failed silently | Check CloudWatch logs for DynamoDB errors |
