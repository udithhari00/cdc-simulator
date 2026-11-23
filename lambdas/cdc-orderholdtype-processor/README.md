# Order Hold Type CDC Lambda → OpenSearch

AWS Lambda that consumes **Change Data Capture (CDC)** events from **MSK / Kafka (Debezium)** for the `YFS_ORDER_HOLD_TYPE` table and keeps hold type data synchronized in the **OpenSearch orders and order-lines indexes**.

The Lambda stores hold types and statuses in a **DynamoDB enrichment cache** (hold types as String Sets, statuses as Number Sets), then performs Painless script updates on OpenSearch. This ensures late-arriving hold types (before the order or order line exists) are never lost.

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

This Lambda processes **order hold type CDC events** from Kafka and synchronizes hold types into both the **orders** and **order-lines** OpenSearch indexes.

Key capabilities:

• Processes **Debezium CDC events from MSK**
• Supports **manual invocation for testing**
• Handles **Create / Update / Delete operations**
• Stores hold types in **DynamoDB cache** as String Sets for atomic add/remove
• Stores hold type statuses in **DynamoDB cache** as Number Sets
• Performs **Painless script updates** on OpenSearch for array manipulation
• Maintains **`holdTypeStatus`** — the highest STATUS value per key (max-wins)
• Updates **both** orders and order-lines indexes

Source table columns used:

| Column | Purpose |
|--------|---------|
| `ORDER_HEADER_KEY` | Links hold type to order |
| `ORDER_LINE_KEY` | Links hold type to order line (may be empty) |
| `HOLD_TYPE` | The hold type value (e.g., `FRAUD_REVIEW`, `CREDIT_HOLD`) |
| `STATUS` | Hold type status (`1100`, `1200`, or `1300`); null values are skipped |

Multiple hold types can exist per order/order-line — they are stored as arrays.
The `holdTypeStatus` field stores only the **highest** STATUS value (1300 > 1200 > 1100).

---

## Architecture

```
Sterling OMS Database
        │
        ▼
   Debezium CDC
        │
        ▼
   Kafka (MSK)  ─── topic: yfs_order_hold_type
        │
        ▼
    AWS Lambda (this)
        │
        ├──► DynamoDB Cache
        │      ├── PK=ORDER_HOLD_TYPE,       SK=ORDER_HEADER_KEY  → holdTypes {Set}, holdTypeStatuses {NS}
        │      └── PK=ORDER_LINE_HOLD_TYPE,   SK=ORDER_LINE_KEY    → holdTypes {Set}, holdTypeStatuses {NS}
        │
        └──► OpenSearch
               ├── orders index       → holdTypes[] (Painless), holdTypeStatus (max-wins)
               └── order-lines index  → holdTypes[] (Painless), holdTypeStatus (max-wins)
```

### Late-arriving hold type handling:

1. Hold type CDC event arrives **before** the order → saved to DynamoDB ✅, OpenSearch 404 skipped ⚠️
2. Order arrives later via `cdc-orderheader-processor` → reads hold types from DynamoDB cache → enriches order document ✅
3. Order line arrives later via `cdc-orderline-processor` → reads hold types from DynamoDB cache → enriches order line document ✅

---

## DynamoDB Cache Schema

| PK | SK | Fields | Consumers |
|----|-----|--------|-----------|
| `ORDER_HOLD_TYPE` | `{ORDER_HEADER_KEY}` | `holdTypes` (String Set), `holdTypeStatuses` (Number Set), `updatedAt` | `cdc-orderheader-processor` |
| `ORDER_LINE_HOLD_TYPE` | `{ORDER_LINE_KEY}` | `holdTypes` (String Set), `holdTypeStatuses` (Number Set), `updatedAt` | `cdc-orderline-processor` |

### Why String Sets?

DynamoDB String Sets provide:
- **Atomic ADD/DELETE** — no read-modify-write needed
- **Native deduplication** — adding the same hold type twice is a no-op
- **Automatic cleanup** — removing the last element removes the attribute entirely

---

## OpenSearch Updates

### Append (create/update)

Uses a Painless script to append a hold type to the array (deduplicated):

```painless
if (ctx._source.holdTypes == null) {
    ctx._source.holdTypes = [];
}
if (!ctx._source.holdTypes.contains(params.holdType)) {
    ctx._source.holdTypes.add(params.holdType);
}
```

### Remove (delete)

Uses a Painless script to remove a specific hold type from the array:

```painless
if (ctx._source.holdTypes != null) {
    ctx._source.holdTypes.removeIf(h -> h == params.holdType);
}
```

### holdTypeStatus — upsert (max-wins)

Only writes the new status if it is **larger** than the existing value:

```painless
if (ctx._source.holdTypeStatus == null || params.status > ctx._source.holdTypeStatus) {
    ctx._source.holdTypeStatus = params.status;
}
```

### holdTypeStatus — recalculate on delete

After removing a status from the DynamoDB cache, recalculates from the remaining set:

```painless
if (params.allStatuses != null && params.allStatuses.size() > 0) {
    int maxStatus = 0;
    for (int s : params.allStatuses) {
        if (s > maxStatus) maxStatus = s;
    }
    ctx._source.holdTypeStatus = maxStatus;
} else {
    ctx._source.remove('holdTypeStatus');
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
| `INDEX_ORDER` | `orders` | OpenSearch orders index name |
| `INDEX_ORDER_LINE` | `order-lines` | OpenSearch order-lines index name |

---

## Phantom Delete Prevention

Delete operations on the DynamoDB cache use `ConditionExpression="attribute_exists(PK)"` to prevent creating phantom records when the item doesn't exist:

```python
cache_table.update_item(
    Key={"PK": entity, "SK": key},
    UpdateExpression="DELETE holdTypes :ht SET updatedAt = :now",
    ConditionExpression="attribute_exists(PK)",
    ...
)
```

If the item doesn't exist, `ConditionalCheckFailedException` is caught and logged as an info-level skip.

---

## Event Sources

### 1️⃣ MSK Kafka Events

Topic naming: `<db>.<schema>.yfs_order_hold_type`

### 2️⃣ Manual Test Event

```json
{
  "table": "YFS_ORDER_HOLD_TYPE",
  "operation": "c",
  "payload": {
    "ORDER_HEADER_KEY": "OH123",
    "ORDER_LINE_KEY": "OL456",
    "HOLD_TYPE": "FRAUD_REVIEW",
    "STATUS": "1300"
  }
}
```

Operations supported:

| Code | Meaning | DynamoDB | OpenSearch |
|------|---------|----------|-----------|
| c | Create | ADD to holdTypes set + holdTypeStatuses set | Append to holdTypes[], upsert holdTypeStatus (max-wins) |
| u | Update | ADD to holdTypes set + holdTypeStatuses set | Append to holdTypes[], upsert holdTypeStatus (max-wins) |
| d | Delete | DELETE from holdTypes set + holdTypeStatuses set | Remove from holdTypes[], recalculate holdTypeStatus |

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

---

## Running Tests & Coverage

```bash
pytest test_lambda_function.py -v --cov=lambda_function --cov-report=term-missing
```

Tests cover:

* DynamoDB String Set ADD/DELETE operations
* Phantom delete prevention (ConditionExpression)
* Painless script append/remove
* Order-level vs order-line-level hold types
* MSK batch event processing
* Debezium message parsing (SMT + standard)
* Edge cases (missing ORDER_LINE_KEY, missing HOLD_TYPE on delete)

---

## Testing & Validating from the AWS Console

### Step 1 — Add a hold type

```json
{
  "table": "YFS_ORDER_HOLD_TYPE",
  "operation": "c",
  "payload": {
    "ORDER_HEADER_KEY": "TEST-OH-001",
    "ORDER_LINE_KEY": "TEST-OL-001",
    "HOLD_TYPE": "FRAUD_REVIEW",
    "STATUS": "1300"
  }
}
```

Expected response:
```json
{
  "statusCode": 200,
  "body": "Indexed hold type FRAUD_REVIEW (status=1300) for order TEST-OH-001"
}
```

### Step 2 — Add a second hold type

```json
{
  "table": "YFS_ORDER_HOLD_TYPE",
  "operation": "c",
  "payload": {
    "ORDER_HEADER_KEY": "TEST-OH-001",
    "ORDER_LINE_KEY": "TEST-OL-001",
    "HOLD_TYPE": "CREDIT_HOLD",
    "STATUS": "1200"
  }
}
```

### Step 3 — Remove a hold type

```json
{
  "table": "YFS_ORDER_HOLD_TYPE",
  "operation": "d",
  "payload": {
    "ORDER_HEADER_KEY": "TEST-OH-001",
    "ORDER_LINE_KEY": "TEST-OL-001",
    "HOLD_TYPE": "FRAUD_REVIEW",
    "STATUS": "1300"
  }
}
```

### Step 4 — Verify in OpenSearch

```bash
GET orders/_doc/TEST-OH-001

# Expected holdTypes: ["CREDIT_HOLD"]  (FRAUD_REVIEW removed)
# Expected holdTypeStatus: 1200  (1300 deleted → falls back to 1200)
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
dynamodb:UpdateItem
dynamodb:GetItem     (for status recalculation on delete)
logs:CreateLogStream
logs:PutLogEvents
```

---

## Troubleshooting

| Issue | Possible Cause | Resolution |
|-------|---------------|------------|
| Hold types not on order | Order not yet indexed | Hold types cached in DynamoDB; enriched when order arrives |
| Hold types not on order line | Order line not yet indexed | Hold types cached in DynamoDB; enriched when line arrives |
| `ConditionalCheckFailedException` in logs | Delete for non-existent hold type | Expected — phantom delete prevented |
| OpenSearch update skipped (404) | Order/order-line not yet indexed | Expected — cached in DynamoDB |
| Duplicate hold types in array | Painless script bug | Should not happen — `contains` check prevents duplicates |
| Empty `holdTypes` array | All hold types deleted | Expected — DynamoDB removes attribute, OpenSearch keeps `[]` |
| `holdTypeStatus` not updating | STATUS is null in CDC event | Null statuses are skipped — only non-null values are processed |
| `holdTypeStatus` shows wrong value after delete | DynamoDB cache stale | Lambda reads remaining statuses from DynamoDB after delete and recalculates max |
