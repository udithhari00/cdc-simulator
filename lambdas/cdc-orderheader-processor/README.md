# Order CDC Lambda → OpenSearch

AWS Lambda that consumes **Change Data Capture (CDC)** events from **MSK / Kafka (Debezium)** for the `YFS_ORDER_HEADER` table and keeps an **OpenSearch order index** synchronized in near real time.
  
The Lambda enriches order documents using **DynamoDB cached entities** and writes them into an **OpenSearch search index**.

---

# Table of Contents

* [What This Lambda Does](#what-this-lambda-does)
* [Architecture](#architecture)
* [OpenSearch Index](#opensearch-index)
* [Environment Variables](#environment-variables)
* [OpenSearch Modes](#opensearch-modes)
* [Event Sources](#event-sources)
* [Project Structure](#project-structure)
* [Local Setup & Testing](#local-setup--testing)
* [Running Tests & Coverage](#running-tests--coverage)
* [Testing & Validating from the AWS Console](#testing--validating-from-the-aws-console)
* [Deployment Notes](#deployment-notes)
* [Troubleshooting](#troubleshooting)

---

# What This Lambda Does

This Lambda processes **order header CDC events** from Kafka and synchronizes them into **OpenSearch**.

Key capabilities:

• Processes **Debezium CDC events from MSK**
• Supports **manual invocation for testing**
• Handles **Create / Update / Delete operations**
• Builds a **search optimized order document**
• Enriches orders using **DynamoDB cached entities**

Supported enrichment entities:

* Person Info (`PERSON_INFO`)
* Organization (`ORGANIZATION`)
* Customer (`CUSTOMER`)
* Order Status (`ORDER_STATUS`)
* Order Invoice (`ORDER_INVOICE`) — enriches `invoiceNo`
* Order Hold Types (`ORDER_HOLD_TYPE`) — enriches `holdTypes` (array)

All enrichment data is retrieved from the **DynamoDB cache table**.

Order status enrichment handles late-arriving CDC events: if the order-status-update lambda has already written a status for this `orderHeaderKey` before the order CDC event arrives, the status is stamped onto the document at index time.

The Lambda builds a **search optimized document** before indexing it into OpenSearch.

---

# Architecture

```
Sterling OMS Database
        │
        ▼
   Debezium CDC
        │
        ▼
   Kafka (MSK)
        │
        ▼
   AWS Lambda
        │
        │ Enrichment
        ▼
   DynamoDB Cache
        │
        ▼
   OpenSearch
        │
        ▼
   Orders Search Index
```

Processing flow:

1. Debezium captures table changes
2. CDC events published to Kafka
3. MSK triggers Lambda
4. Lambda decodes the CDC message
5. Order document is constructed
6. Enrichment fetched from DynamoDB
7. Document indexed into OpenSearch

---

# OpenSearch Index

Default index name:

```
orders
```

Configured using environment variable:

```
ORDER_INDEX
```

Internally stored as:

```
INDEX_ORDER
```

Create and update operations both use:

```
client.index()
```

Delete operations use:

```
client.delete()
```

---

## Example Document

```json
{
  "orderHeaderKey": "OH123",
  "orderNo": "10001",
  "enterpriseKey": "ENT",
  "documentType": "0001",

  "orderDate": "2023-11-14T10:12:34Z",

  "customer": {
    "customerId": "CUST1",
    "customerFirstName": "John",
    "customerLastName": "Doe",
    "customerFullName": "John Doe",
    "customerEmailId": "john@test.com"
  },

  "billTo": {
    "fullName": "John Doe",
    "emailId": "john@test.com"
  },

  "sellerOrganizationName": "TestOrg",

  "statusCode": "3700",
  "statusName": "Shipped",

  "invoiceNo": "INV-2026-0001",
  "holdTypes": ["FRAUD_REVIEW", "CREDIT_HOLD"],

  "cdcOperation": "c",
  "cdcTimestamp": 1700000000000,
  "indexedAt": 1700000000000,

  "searchableText": "10001 John Doe ENT Shipped ..."
}
```

---

# Environment Variables

| Variable            | Description                           | Default          |
| ------------------- | ------------------------------------- | ---------------- |
| DYNAMODB_TABLE      | DynamoDB enrichment cache table       | enrichment_cache |
| DYNAMODB_ENDPOINT   | Local DynamoDB endpoint (for testing) | none             |
| OPENSEARCH_HOST     | OpenSearch endpoint                   | opensearch       |
| OPENSEARCH_PORT     | OpenSearch port                       | 443              |
| OPENSEARCH_SCHEME   | http or https                         | https            |
| OPENSEARCH_AUTH     | Authentication mode                   | iam              |
| OPENSEARCH_USERNAME | Basic auth username                   | admin            |
| OPENSEARCH_PASSWORD | Basic auth password                   | admin            |
| OPENSEARCH_MODE     | serverless or provisioned             | serverless       |
| AWS_REGION          | AWS region                            | us-east-1        |
| ORDER_INDEX         | OpenSearch index name                 | orders           |
| ENABLE_HARD_DELETE  | `true` = physical delete; `false` = soft delete | false |

---

# Soft Delete Behaviour

By default (`ENABLE_HARD_DELETE=false`) a `d` CDC event performs a **soft delete** — the order document is **updated** rather than removed.

### OpenSearch — `soft_delete_os_doc`

```python
client.update(
    index=INDEX_ORDER,
    id=order_header_key,
    body={"doc": {"_deleted": True, "_deletedAt": int(datetime.now(UTC).timestamp() * 1000)}}
)
```

The document remains in the `orders` index with `_deleted: true`. To exclude soft-deleted orders from search results, add a filter:

```json
{ "term": { "_deleted": false } }
```

### Hard Delete

Set `ENABLE_HARD_DELETE=true` to physically remove the document:

```python
client.delete(index=INDEX_ORDER, id=order_header_key, ignore=[404])
```

### Direct Invocation — Delete Payload

```json
{
  "table": "YFS_ORDER_HEADER",
  "operation": "d",
  "payload": {
    "ORDER_HEADER_KEY": "<orderHeaderKey>"
  }
}
```

---

# OpenSearch Modes

The Lambda supports two OpenSearch deployments.

### Serverless

Service used:

```
aoss
```

Authentication:

```
IAM SigV4
```

---

### Provisioned

Service used:

```
es
```

Authentication:

```
IAM SigV4
```

---

# Event Sources

Lambda supports **two invocation types**.

---

# 1️⃣ MSK Kafka Events

Events are emitted by **Debezium CDC**.

Example event:

```json
{
  "records": {
    "topic-0": [
      {
        "value": "BASE64_ENCODED_JSON",
        "topic": "sterling.public.yfs_order_header",
        "offset": 1
      }
    ]
  }
}
```

Processing flow:

1. Base64 decode message
2. Parse Debezium event
3. Extract operation (`op`)
4. Extract `after` or `before`
5. Normalize payload
6. Process order event

---

# 2️⃣ Manual Test Event

Used to test Lambda directly.

Example:

```json
{
  "table": "YFS_ORDER_HEADER",
  "operation": "c",
  "payload": {
    "ORDER_HEADER_KEY": "OH123",
    "ORDER_NO": "10001",
    "CUSTOMER_FIRST_NAME": "John",
    "CUSTOMER_LAST_NAME": "Doe",
    "BILL_TO_ID": "CUST1"
  }
}
```

Operations supported:

| Code | Meaning |
| ---- | ------- |
| c    | Create  |
| u    | Update  |
| d    | Delete  |

---

# Project Structure

```
project/
│
├── lambda_function.py
├── test_lambda_function.py
├── requirements.txt
└── README.md
```

| File                    | Description                |
| ----------------------- | -------------------------- |
| lambda_function.py      | Main Lambda implementation |
| test_lambda_function.py | Pytest test suite          |
| requirements.txt        | Python dependencies        |

Dependencies include AWS SDK, OpenSearch client, and testing tools. 

---

# Local Setup & Testing

### 1️⃣ Clone repository

```
git clone <repo>
cd project
```

---

### 2️⃣ Install dependencies

```
pip install -r requirements.txt
```

---

### 3️⃣ Set environment variables

Example:

```
export OPENSEARCH_HOST=localhost
export ORDER_INDEX=orders
export DYNAMODB_TABLE=enrichment_cache
```

---

### 4️⃣ Run Lambda locally

Example test script:

```python
import lambda_function

event = {
    "table": "YFS_ORDER_HEADER",
    "operation": "c",
    "payload": {
        "ORDER_HEADER_KEY": "OH1",
        "ORDER_NO": "10001",
        "BILL_TO_ID": "CUST1"
    }
}

print(lambda_function.lambda_handler(event, None))
```

---

# Running Tests & Coverage

A comprehensive **pytest suite** is included. 

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

Target coverage:

```
> 80%
```

Tests cover:

* Utility functions
* Timestamp handling
* Document builder
* DynamoDB enrichment
* MSK event processing
* OpenSearch operations
* DNS resolution checks

---

# Testing & Validating from the AWS Console

### Step 1

Open **AWS Lambda Console**

---

### Step 2

Create **Test Event**

---

### Step 3

Paste sample event

```json
{
  "table": "YFS_ORDER_HEADER",
  "operation": "c",
  "payload": {
    "ORDER_HEADER_KEY": "TEST1",
    "ORDER_NO": "50001",
    "CUSTOMER_FIRST_NAME": "Test",
    "CUSTOMER_LAST_NAME": "User",
    "BILL_TO_ID": "CUST1"
  }
}
```

---

### Step 4

Click **Test**

Expected response:

```
{
  "statusCode": 200,
  "body": "Indexed order TEST1"
}
```

---

### Step 5

Verify in OpenSearch

```
GET orders/_search
{
  "query": {
    "match": {
      "orderNo": "50001"
    }
  }
}
```

---

# Deployment Notes

Recommended configuration:

```
Runtime: Python 3.11
Memory: 512 MB+
Timeout: 30s
```

Required IAM permissions:

```
es:ESHttp*
aoss:APIAccessAll
dynamodb:GetItem
logs:CreateLogStream
logs:PutLogEvents
```

Also configure:

* MSK trigger
* VPC access (if OpenSearch is private)

---

# Troubleshooting

| Issue                               | Possible Cause                          | Resolution                                         |
| ----------------------------------- | --------------------------------------- | -------------------------------------------------- |
| Lambda cannot connect to OpenSearch | Incorrect endpoint or VPC configuration | Verify `OPENSEARCH_HOST`, `PORT`, security groups  |
| DNS resolution warning              | OpenSearch endpoint not reachable       | Check VPC DNS settings                             |
| Orders not indexed                  | Missing `ORDER_HEADER_KEY`              | Verify payload fields                              |
| Enrichment missing                  | DynamoDB record not found               | Verify `PK/SK` values in cache table               |
| `orderStatus` null after ingestion  | Status not yet written to DynamoDB      | Invoke order-status-update lambda first; status will be applied on next ingestion |
| `invoiceNo` null after ingestion     | Invoice not yet written to DynamoDB     | Invoice will be enriched when order is re-ingested or when invoice lambda runs |
| `holdTypes` missing after ingestion  | Hold types not yet written to DynamoDB  | Hold types will be enriched when order is re-ingested or when hold type lambda runs |
| Authentication error                | IAM role missing permissions            | Add `es:ESHttp*` or `aoss:APIAccessAll`            |
| MSK events not processed            | Incorrect Kafka event structure         | Verify base64 Debezium payload                     |
| Timestamp fields incorrect          | Debezium timestamp format mismatch      | Verify microsecond/millisecond conversion          |
| Delete operation fails              | Document ID mismatch                    | Ensure `ORDER_HEADER_KEY` matches OpenSearch `_id` |

---






