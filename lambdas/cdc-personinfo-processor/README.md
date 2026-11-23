# cdc-lambda-personinfo

AWS Lambda that processes **YFS_PERSON_INFO** CDC events from Oracle via MSK (Kafka / Debezium).

---

## What it does

Person info records are address and contact data referenced by orders and organizations. This lambda caches every change to DynamoDB and fans the updated data out to every OpenSearch document that references it.

---

## Five-Phase Write

### Phase 1 — DynamoDB Cache

Stores `PK=PERSON_INFO, SK=<personInfoKey>` with all address and contact fields.

| Field | Source column |
|---|---|
| `firstName` / `middleName` / `lastName` | `FIRST_NAME` / `MIDDLE_NAME` / `LAST_NAME` |
| `fullName` | joined from firstName + middleName + lastName |
| `company` | `COMPANY` |
| `addressLine1` / `addressLine2` / `addressLine3` | `ADDRESS_LINE1` / `ADDRESS_LINE2` / `ADDRESS_LINE3` |
| `city` / `state` / `zipCode` / `country` | address fields |
| `emailId` | `EMAILID` |
| `dayPhone` / `eveningPhone` / `mobilePhone` | `DAY_PHONE` / `EVENING_PHONE` / `MOBILE_PHONE` |

On a **delete** (`op=d`) the DynamoDB entry is removed and all fan-out phases run to null out the referenced fields.

---

### Phase 2 — Fan-out to Orders Index

Queries the orders index on `billToKey`, `shipToKey`, and `soldToKey` (one `update_by_query` per role). Replaces the entire nested `billTo`/`shipTo`/`soldTo` address object and rebuilds `searchableText`.

**Address object fields written:**

`personInfoKey`, `firstName`, `middleName`, `lastName`, `fullName`, `emailId`, `dayPhone`, `eveningPhone`, `mobilePhone`, `company`, `addressLine1`, `addressLine2`, `addressLine3`, `city`, `state`, `zipCode`, `country`

---

### Phase 3 — Fan-out to Organizations: Corporate Address

Queries the organizations index on `corporateAddressKey`. Hash-gated (`addressHash`) to skip already-current docs. Sets the nested `address` object on the org document and rebuilds `searchableText`.

**Fields written:** `addressLine1`–`addressLine6`, `city`, `state`, `zipCode`, `country`, `addressHash`

---

### Phase 4 — Fan-out to Organizations: Contact Person

Queries the organizations index on `contactAddressKey`. Hash-gated (`contactHash`) to skip already-current docs. Sets the nested `contact` object on the org document and rebuilds `searchableText`.

**Fields written:** `firstName`, `lastName`, `fullName`, `email`, `phone`, `dayPhone`, `eveningPhone`, `mobilePhone`, `company`, `addressLine1`–`addressLine6`, `city`, `state`, `zipCode`, `country`, `contactHash`

Phone priority: `MOBILE_PHONE` → `DAY_PHONE` → `EVENING_PHONE`

---

### Phase 5 — Fan-out to Organizations: Address Roles

Queries the organizations index on `billTo.personInfoKey`, `shipTo.personInfoKey`, and `soldTo.personInfoKey` (one `update_by_query` per role). Replaces the entire nested address object — mirroring what `cdc-lambda-customeraddnladdress` writes — and rebuilds `searchableText`.

**Address object fields written:**

`personInfoKey`, `firstName`, `lastName`, `fullName`, `emailId`, `dayPhone`, `eveningPhone`, `mobilePhone`, `company`, `addressLine1`, `city`, `state`, `zipCode`, `country`

All fan-out phases are **non-fatal** — logged as warnings, processing continues.

---

## DynamoDB Single-Table Design

Table name: `enrichment_cache` (configurable via `DYNAMODB_TABLE` env var)

| PK | SK | Written by | Read by |
|---|---|---|---|
| `PERSON_INFO` | `<personInfoKey>` | this lambda | `cdc-lambda-orderheader`, `cdc-lambda-organization`, `cdc-lambda-customeraddnladdress` |

---

## Event Sources

### MSK (production)

Topic consumed:

```
<prefix>.STERLING.yfs_person_info
```

### Direct invocation (testing)

```json
{
  "table": "YFS_PERSON_INFO",
  "operation": "u",
  "payload": {
    "PERSON_INFO_KEY": "PIK001",
    "FIRST_NAME": "John",
    "LAST_NAME": "Doe",
    "EMAILID": "john@example.com",
    "CITY": "Springfield",
    "STATE": "IL"
  },
  "sourceTimestamp": 1700000000000
}
```

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `DYNAMODB_TABLE` | `enrichment_cache` | DynamoDB table name |
| `DYNAMODB_ENDPOINT` | *(unset)* | Local DynamoDB endpoint (dev only) |
| `OPENSEARCH_HOST` | `opensearch` | OpenSearch host |
| `OPENSEARCH_PORT` | `443` | OpenSearch port |
| `OPENSEARCH_SCHEME` | `https` | `http` or `https` |
| `OPENSEARCH_AUTH` | `iam` | `iam` (AWS SigV4) or `basic` |
| `OPENSEARCH_MODE` | `serverless` | `provisioned` → service=`es`; `serverless` → service=`aoss` |
| `OPENSEARCH_USERNAME` | `admin` | Basic auth username (non-IAM only) |
| `OPENSEARCH_PASSWORD` | `admin` | Basic auth password (non-IAM only) |
| `AWS_REGION` | `us-east-1` | AWS region |
| `INDEX_ORDERS` | `orders` | OpenSearch orders index name |
| `INDEX_ORGANIZATION` | `organizations` | OpenSearch organizations index name |
| `ENABLE_HARD_DELETE` | `false` | Set to `true` to physically delete; `false` uses soft delete |

> `INDEX_ORDERS` and `INDEX_ORGANIZATION` must match the values set in `cdc-lambda-orderheader` and `cdc-lambda-organization` respectively.

---

## Soft Delete Behaviour

By default (`ENABLE_HARD_DELETE=false`) the lambda performs a **soft delete** when `op=d` is received.

### Phase 1 — DynamoDB `soft_delete_cache_item`

The `PK=PERSON_INFO, SK=<personInfoKey>` row is **marked** rather than removed:

```python
cache_table.update_item(
    Key={'PK': ENTITY_PERSON_INFO, 'SK': person_info_key},
    UpdateExpression='SET #d = :true, #da = :now',
    ExpressionAttributeNames={'#d': '_deleted', '#da': '_deletedAt'},
    ExpressionAttributeValues={':true': True, ':now': datetime.now(UTC).isoformat()},
)
```

Cache reads in downstream lambdas check `_deleted` and return `None` if set, so soft-deleted person info records are invisible to enrichment lookups.

### Phases 2–5 — Fan-out (unchanged on delete)

All four OpenSearch fan-out phases still run after a soft delete, nulling out the referenced nested objects (`billTo`, `shipTo`, `soldTo`, `address`, `contact`, `billTo`/`shipTo`/`soldTo` on orgs) — identical behaviour to a hard delete for the downstream indexes.

### Hard Delete

Set `ENABLE_HARD_DELETE=true` to physically remove the DynamoDB row:

```python
cache_table.delete_item(Key={'PK': ENTITY_PERSON_INFO, 'SK': person_info_key})
```

### Direct Invocation — Delete Payload

```json
{
  "table": "YFS_PERSON_INFO",
  "operation": "d",
  "payload": {
    "PERSON_INFO_KEY": "<personInfoKey>"
  }
}
```

---

## Response Codes

| Code | Meaning |
|---|---|
| `200` | Processed successfully |
| `207` | Batch partially processed (at least one error) |
| `400` | Missing required key (`PERSON_INFO_KEY`) or unknown table |
| `500` | Unhandled exception |

---

## Running Tests

```bash
python3.13 -m venv .venv
.venv/bin/pip install -r requirements.txt pytest

.venv/bin/python -m pytest test_lambda_function.py -v
```

60 tests covering: utility helpers, topic extraction, Phase 1 DynamoDB (cache write, full name assembly, PK/SK, delete clears entry), Phase 2 orders fan-out (all three roles queried, address object content, delete sets null, role param cycles, conflicts proceed, non-fatal failure), Phase 3 org corporate address (query key, address fields, delete nulls, non-fatal), Phase 4 org contact (query key, name/email/phone, phone fallback order, delete nulls, non-fatal), Phase 5 org address roles (all three roles, nested personInfoKey queries, address object content, delete sets null, conflicts proceed, non-fatal), routing, MSK batch (200/207, delete uses before image, uppercase keys, malformed base64), `lambda_handler`, and OpenSearch client auth branches.
