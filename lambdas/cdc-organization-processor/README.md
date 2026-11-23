# cdc-lambda-organization

AWS Lambda that processes **YFS_ORGANIZATION** CDC events from Oracle via MSK (Kafka / Debezium) and keeps the OpenSearch organization index in sync.

---

## What it does

For every organization change event the lambda executes a **three-phase write**:

```
Phase 1 → DynamoDB  (cache org data + hash)
Phase 2 → OpenSearch orders index  (fan-out name change to all referencing orders)
Phase 3 → OpenSearch organizations index  (full org document, enriched)
```

---

## Three-Phase Write Detail

### Phase 1 — DynamoDB Cache

Stores the organization in a single-table DynamoDB cache so other lambdas (order header, customer) can look it up without querying OpenSearch.

| Attribute | Value |
|---|---|
| `PK` | `ORGANIZATION` |
| `SK` | `<ORGANIZATION_KEY>` |
| `organizationKey` | Oracle ORGANIZATION_KEY |
| `organizationCode` | Oracle ORGANIZATION_CODE |
| `organizationName` | Display name |
| `orgNameHash` | SHA-256 (first 16 hex chars) of `organizationName` |
| `corporateAddressKey` | FK → PERSON_INFO for address |
| `contactAddressKey` | FK → PERSON_INFO for contact person |

**Hash optimization:** Before writing, the lambda fetches the existing `orgNameHash` from DynamoDB. If the hash has not changed (name is the same), Phase 2 is skipped entirely.

On a **delete** (`op=d`) the cache entry is removed and Phase 2 is forced.

---

### Phase 2 — Fan-out to Orders (conditional)

Only runs when `name_changed = True` (hash differs from stored value).

Executes three `update_by_query` calls against the orders index — one for each role an organization can play on an order:

| Role | Query field | Fields updated |
|---|---|---|
| Seller | `sellerOrganizationCode` | `sellerOrganizationName`, `sellerOrgNameHash` |
| Buyer | `buyerOrganizationCode` | `buyerOrganizationName`, `buyerOrgNameHash` |
| Customer org | `customer.organizationCode` | `customer.organizationName`, `customer.orgNameHash` |

Each query also uses a `must_not` clause to filter out orders that already carry the current hash, so already-updated documents are skipped at query time.

After updating the name/hash fields each query runs a **Painless script** (`PAINLESS_REBUILD_ORDER_SEARCHABLE_TEXT`) that rebuilds the order's `searchableText` field to stay consistent with the orderheader lambda.

Fan-out failures are **non-fatal** — logged as warnings, processing continues.

---

### Phase 3 — Upsert to OpenSearch (partial update)

Builds an organization document and upserts it into the organizations index using `client.update(doc_as_upsert=True)`. This **partial update** preserves enrichment fields set by fan-out events from other Lambdas (e.g. `customerContact`, `address`, `contact`) that would otherwise be lost with a full `client.index()` replace.

**Enrichment chain (all from DynamoDB cache):**

1. `CORPORATE_ADDRESS_KEY` → `PERSON_INFO` cache → populates nested `address` object (fullName, email, phone, company, addressLines, city, state, zip, country)
2. `CONTACT_ADDRESS_KEY` → `PERSON_INFO` cache → populates nested `contact` object (fullName, email, phone, company, addressLines, city, state, zip, country)
3. `ORGANIZATION_KEY` → `CUSTOMER_BY_ORG_KEY` cache → populates nested `customer` object (customerId, customerKey, customerType, customerStatus, extn* fields)
4. `customerKey` (from step 3, stripped of Oracle CHAR whitespace) → `CUSTOMER_ADDNL_ADDRESS` query (begins_with `customerKey#`) → filters out soft-deleted items (`_deleted=true`) → resolves `PERSON_INFO` → populates `billTo`/`shipTo`/`soldTo` nested objects
5. `customerKey` (from step 3, stripped of Oracle CHAR whitespace) → `CUSTOMER_CONTACT` query (begins_with `customerKey#`) → filters out soft-deleted items (`_deleted=true`) → populates `customerContact` array (contactName, contactEmail, contactPhone, contactJobTitle, contactDepartment, contactCompany, contactDob, contactType, statementEmail)

Steps 4–5 reuse the sanitized `customerKey` obtained from `CUSTOMER_BY_ORG_KEY` in step 3. The `CUSTOMER_CONTACT_BY_ORG_KEY` reverse-lookup is **no longer used** for enrichment. Addresses in `CUSTOMER_ADDNL_ADDRESS` are cached by role (`#BillTo`, `#ShipTo`, `#SoldTo`) based on boolean default flags.

Document is upserted using `organizationCode` as the OpenSearch `_id`.

**searchableText** is built from: `organizationCode`, `organizationName`, `customerId`, `customerRewardsNo`, `extnAplusCustomerId`, `contact.fullName`, `contact.email`, `contact.phone`, `customerContact[].contactName/contactEmail/contactPhone`, `billTo.fullName`, `billTo.emailId`, `billTo.company`, `shipTo.fullName`, `shipTo.emailId`, `shipTo.company`, `soldTo.fullName`, `soldTo.emailId`, `soldTo.company`, `extnSalesPerson`, `extnInsideSalesRep`, `extnMarketSegment`.

---

## DynamoDB Single-Table Design

Table name: `enrichment_cache` (configurable via `DYNAMODB_TABLE` env var)

| PK | SK | Access | Description |
|---|---|---|---|
| `ORGANIZATION` | `<organizationKey>` | Write | Organization data cached by this lambda |
| `PERSON_INFO` | `<personInfoKey>` | Read | Address/contact data written by cdc-lambda-personinfo |
| `CUSTOMER_BY_ORG_KEY` | `<buyerOrgCode>` | Read | Customer data (incl. `customerKey`) written by cdc-lambda-customer |
| `CUSTOMER_CONTACT` | `<customerKey>#<contactKey>` | Query (begins_with) | Contact records written by cdc-lambda-customercontact |
| `CUSTOMER_ADDNL_ADDRESS` | `<customerKey>#<addressType>` | Query (begins_with) | Address records written by cdc-lambda-customeraddnladdress |

> **Note:** `CUSTOMER_CONTACT_BY_ORG_KEY` is **no longer read** by this lambda. The `customerKey` for address/contact enrichment comes from `CUSTOMER_BY_ORG_KEY`.
>
> Orders reference organizations by `ORGANIZATION_KEY` (not `ORGANIZATION_CODE`), so the DynamoDB SK and all fan-out queries use `organizationKey`.

---

## Event Sources

### MSK (production)

Triggered by the MSK event source mapping. Debezium publishes CDC events to topic:

```
<prefix>.STERLING.yfs_organization
```

MSK event structure:
```json
{
  "eventSource": "aws:kafka",
  "records": {
    "topic-partition": [
      {
        "topic": "glenraven-dev.STERLING.yfs_organization",
        "offset": 42,
        "value": "<base64-encoded Debezium JSON>"
      }
    ]
  }
}
```

Debezium JSON (after decode):
```json
{
  "op": "u",
  "before": { "ORGANIZATION_KEY": "..." },
  "after":  { "ORGANIZATION_KEY": "...", "ORGANIZATION_NAME": "Acme Corp", ... },
  "ts_ms": 1700000000000
}
```

Payload column names are **uppercased** during MSK processing to normalize Oracle CHAR/VARCHAR column casing.

### Direct invocation (testing)

```json
{
  "table": "YFS_ORGANIZATION",
  "operation": "u",
  "payload": { "ORGANIZATION_KEY": "ORG1", "ORGANIZATION_NAME": "Acme" },
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
| `ORDER_INDEX` | `orders` | OpenSearch orders index name |
| `ORGANIZATION_INDEX` | `organizations` | OpenSearch organizations index name |
| `ENABLE_HARD_DELETE` | `false` | Set to `true` to physically delete; `false` uses soft delete |

---

## Soft Delete Behaviour

By default (`ENABLE_HARD_DELETE=false`) a `d` CDC event performs a **soft delete** rather than a physical removal.

### Phase 1 — DynamoDB `soft_delete_cache_item`

The `PK=ORGANIZATION, SK=<organizationKey>` row is **marked**, not deleted:

```python
cache_table.update_item(
    Key={'PK': ENTITY_ORGANIZATION, 'SK': organization_key},
    UpdateExpression='SET #d = :true, #da = :now',
    ExpressionAttributeNames={'#d': '_deleted', '#da': '_deletedAt'},
    ExpressionAttributeValues={':true': True, ':now': datetime.now(UTC).isoformat()},
)
```

Cache reads in downstream lambdas check `_deleted` and return `None` if set, so soft-deleted orgs are invisible to enrichment lookups.

### Phase 2 — Fan-out (unchanged)

The org name fan-out to orders still runs on delete (`name_changed = True` forced), nulling out `sellerOrganizationName`, `buyerOrganizationName`, and `customer.organizationName` on all referencing orders.

### Phase 3 — OpenSearch `soft_delete_os_doc`

Instead of `client.delete()`, the org document is **updated** with two new fields:

```python
client.update(
    index=INDEX_ORGANIZATION,
    id=organization_code,
    body={"doc": {"_deleted": True, "_deletedAt": int(datetime.now(UTC).timestamp() * 1000)}}
)
```

The document remains in the index and can be filtered out with:
```json
{ "term": { "_deleted": true } }
```

### Hard Delete

Set `ENABLE_HARD_DELETE=true` to revert to physical removal:

```python
# DynamoDB
cache_table.delete_item(Key={'PK': ENTITY_ORGANIZATION, 'SK': organization_key})

# OpenSearch
client.delete(index=INDEX_ORGANIZATION, id=organization_code, ignore=[404])
```

### Direct Invocation — Delete Payload

```json
{
  "table": "YFS_ORGANIZATION",
  "operation": "d",
  "payload": {
    "ORGANIZATION_KEY": "<organizationKey>",
    "ORGANIZATION_CODE": "<organizationCode>"
  }
}
```

---

## OpenSearch Authentication

| `OPENSEARCH_AUTH` | `OPENSEARCH_MODE` | IAM service |
|---|---|---|
| `iam` | `provisioned` | `es` (OpenSearch Service) |
| `iam` | `serverless` | `aoss` (OpenSearch Serverless) |
| `basic` | any | username/password |

The client is a **lazy singleton** — created on the first invocation and reused across warm Lambda executions.

---

## Hash-Based Optimization

```
compute_org_name_hash(name) = SHA-256(name).hexdigest()[:16]
```

Flow on every non-delete event:

```
1. Compute new_name_hash from incoming organizationName
2. Fetch existing_hash from DynamoDB (orgNameHash field)
3. If new_name_hash == existing_hash → skip Phase 2 (no fan-out)
4. Write new_name_hash to DynamoDB item
5. Fan-out queries filter with must_not { term: { *OrgNameHash: new_name_hash } }
   → orders already carrying the current hash are skipped server-side
6. Painless script writes new hash into updated order documents
   → next fan-out is even cheaper
```

---

## Response Codes

| Code | Meaning |
|---|---|
| `200` | All records processed successfully |
| `207` | Batch partially processed (at least one error) |
| `400` | Missing required key (`ORGANIZATION_KEY`) or unknown table |
| `500` | Unhandled exception |

---

## Running Tests

```bash
# Create venv (requires Python 3.11+)
python3.13 -m venv .venv
.venv/bin/pip install -r requirements.txt pytest

# Run
.venv/bin/python -m pytest test_lambda_function.py -v
```

Tests cover: utility helpers, topic extraction, DynamoDB phase (hash storage, skip-on-unchanged, trigger-on-changed), fan-out queries (field paths, hash filters, Painless scripts, non-fatal failure), Phase 3 upsert (address, contact, customer, customerContact array, billTo/shipTo/soldTo via `customerKey` from `CUSTOMER_BY_ORG_KEY`, cache miss), searchableText, MSK batch processing (200/207, multi-message, delete, uppercase keys, malformed base64), `lambda_handler` entry point, and `enrich_org_with_contacts`.
