# cdc-lambda-customercontact

AWS Lambda that processes **YFS_CUSTOMER_CONTACT** CDC events from Oracle via MSK (Kafka / Debezium).

---

## What it does

Customer contact data is **not indexed in its own index**. This lambda caches contact data in DynamoDB (one record per contact), writes a `CUSTOMER_CONTACT_BY_ORG_KEY` reverse-lookup entry, and fans out the **full array of contacts** to the organization index.

---

## Three-Phase Write

### Phase 1 — DynamoDB Cache

Stores `PK=CUSTOMER_CONTACT, SK=<customer_key>#<customer_contact_key>` with contact fields:

| Field | Source column |
|---|---|
| `contactName` | `FIRST_NAME` + `MIDDLE_NAME` + `LAST_NAME` (joined) |
| `contactEmail` | `EMAILID` |
| `contactPhone` | `MOBILE_PHONE` → `DAY_PHONE` → `EVENING_PHONE` (priority order) |
| `contactJobTitle` | `JOB_TITLE` |
| `contactDepartment` | `DEPARTMENT` |
| `contactType` | `EXTN_CONTACT_TYPE` |
| `contactCompany` | `COMPANY` |
| `contactDob` | `DOB` |
| `statementEmail` | `EXTN_STATEMENT_EMAIL` |

Also reads `PK=CUSTOMER, SK=<customer_key>` to obtain the `buyerOrganizationCode` (written by `cdc-customer-processor`).

> Note: `EVENING_FAX_N0` column (DDL typo — zero not letter O) is intentionally excluded.

On a **delete** (`op=d`) the DynamoDB entry is removed and Phase 3 runs to rebuild the `customerContact` array on the org document (sans the deleted contact). Phase 2 deletes the `CUSTOMER_CONTACT_BY_ORG_KEY` entry if `buyerOrganizationCode` is known.

---

### Phase 2 — CUSTOMER_CONTACT_BY_ORG_KEY (DynamoDB write/delete)

If `buyerOrganizationCode` is found in the CUSTOMER record, writes `PK=CUSTOMER_CONTACT_BY_ORG_KEY, SK=<buyerOrganizationCode>` with the same contact fields. On delete, the entry is removed.

> **Note:** The `cdc-organization-processor` no longer reads `CUSTOMER_CONTACT_BY_ORG_KEY` (it now queries `CUSTOMER_CONTACT` records directly via `customerKey`). This entry is retained for potential future use.

Skipped if:
- No CUSTOMER record exists for the `customerKey`
- The CUSTOMER record has no `buyerOrganizationCode`

---

### Phase 3 — Fan-out to Organization Index

Uses `build_contacts_array(customer_key)` to query all `CUSTOMER_CONTACT` records from DynamoDB for the given `customerKey`, then executes one `update_by_query` against the organizations index:

- **Query:** `customer.customerKey == <customer_key>`
- **Script:** Sets `customerContact` as a **full array** of contact objects (each with `contactName`, `contactEmail`, `contactPhone`, `contactJobTitle`, `contactDepartment`, `contactCompany`, `contactDob`, `contactType`, `statementEmail`) on the matching org document, then rebuilds `searchableText`

On delete, the array is rebuilt from the remaining contacts in DynamoDB (the deleted contact was already removed in Phase 1).

Fan-out failures are **non-fatal** — logged as warnings, processing continues.

---

## DynamoDB Single-Table Design

Table name: `enrichment_cache` (configurable via `DYNAMODB_TABLE` env var)

| PK | SK | Written by | Read by |
|---|---|---|---|
| `CUSTOMER_CONTACT` | `<customerKey>#<customerContactKey>` | this lambda | other lambdas |
| `CUSTOMER_CONTACT_BY_ORG_KEY` | `<buyerOrganizationCode>` | this lambda | *(retained for future use)* |
| `CUSTOMER` | `<customerKey>` | `cdc-customer-processor` | this lambda (Phase 1 read) |

---

## Event Sources

### MSK (production)

Topic consumed:

```
<prefix>.STERLING.yfs_customer_contact
```

### Direct invocation (testing)

```json
{
  "table": "YFS_CUSTOMER_CONTACT",
  "operation": "u",
  "payload": { "CUSTOMER_KEY": "CKEY001", "CUSTOMER_CONTACT_KEY": "CCKEY001", "FIRST_NAME": "Jane", "EMAILID": "jane@example.com", "COMPANY": "Acme Corp", ... },
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
| `INDEX_ORGANIZATION` | `organizations` | OpenSearch organizations index name |
| `ENABLE_HARD_DELETE` | `false` | Set to `true` to physically delete; `false` uses soft delete |

> `INDEX_ORGANIZATION` must match the value set in `cdc-lambda-organization`.

---

## Soft Delete Behaviour

By default (`ENABLE_HARD_DELETE=false`) the lambda performs a **soft delete** when `op=d` is received.

### Phase 1 — DynamoDB `soft_delete_cache_item`

The `CUSTOMER_CONTACT` row is **marked** rather than removed:

```python
cache_table.update_item(
    Key={'PK': 'CUSTOMER_CONTACT', 'SK': f'{customer_key}#{customer_contact_key}'},
    UpdateExpression='SET #d = :true, #da = :now',
    ExpressionAttributeNames={'#d': '_deleted', '#da': '_deletedAt'},
    ExpressionAttributeValues={':true': True, ':now': datetime.now(UTC).isoformat()},
)
```

### Phase 2 — `CUSTOMER_CONTACT_BY_ORG_KEY` (soft delete if present)

If `buyerOrganizationCode` is known, the org-key row is also soft-deleted.

### Phase 3 — Fan-out (unchanged on delete)

`build_contacts_array` re-queries all `CUSTOMER_CONTACT` rows (which now excludes the marked record since soft-deleted items are filtered out), then updates the `customerContact` array on the org document.

### Hard Delete

Set `ENABLE_HARD_DELETE=true` to physically remove the DynamoDB rows:

```python
cache_table.delete_item(Key={'PK': 'CUSTOMER_CONTACT', 'SK': f'{customer_key}#{customer_contact_key}'})
cache_table.delete_item(Key={'PK': 'CUSTOMER_CONTACT_BY_ORG_KEY', 'SK': buyer_org_code})
```

### Direct Invocation — Delete Payload

```json
{
  "table": "YFS_CUSTOMER_CONTACT",
  "operation": "d",
  "payload": {
    "CUSTOMER_KEY": "<customerKey>",
    "CUSTOMER_CONTACT_KEY": "<customerContactKey>"
  }
}
```

---

## Response Codes

| Code | Meaning |
|---|---|
| `200` | All records processed successfully |
| `207` | Batch partially processed (at least one error) |
| `400` | Missing required key (`CUSTOMER_KEY`) or unknown table |
| `500` | Unhandled exception |

---

## Running Tests

```bash
python3.13 -m venv .venv
.venv/bin/pip install -r requirements.txt pytest

.venv/bin/python -m pytest test_lambda_function.py -v
```

Tests cover: utility helpers, topic extraction, Phase 1 DynamoDB (composite SK, full name assembly, contactCompany/contactDob, CUSTOMER read for buyerOrganizationCode, delete with composite SK), Phase 2 CUSTOMER_CONTACT_BY_ORG_KEY (write, delete, skip when no org), Phase 3 fan-out (`customerContact` array via `build_contacts_array`, query uses `customer.customerKey`, rebuild on delete, non-fatal failure), routing, MSK batch (200/207, delete uses before image, uppercase keys, malformed base64), `lambda_handler`, real CDC event integration, and OpenSearch client auth branches.
