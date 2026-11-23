# cdc-lambda-customeraddnladdress

AWS Lambda that processes **YFS_CUSTOMER_ADDNL_ADDRESS** CDC events from Oracle via MSK (Kafka / Debezium).

---

## What it does

Customer additional address data is **not indexed in its own index**. This lambda caches address records in DynamoDB and fans out `billTo`, `shipTo`, and `soldTo` address objects to the organization index in OpenSearch.

Only **organization-level default** addresses are processed:
- **Organization-level check:** `CUSTOMER_CONTACT_KEY` must be empty/null (contact-specific addresses are safely skipped with `200`).
- **Default address check:** Roles are derived directly from the boolean default flags. Non-default addresses are skipped with `200`.

---

## Two-Phase Write

### Phase 1 — DynamoDB Cache

Stores `PK=CUSTOMER_ADDNL_ADDRESS, SK=<customer_key>#<role>` with a `personInfoKey` reference for each qualifying role.

#### Role Derivation Rules:
In Sterling OMS, `ADDRESS_TYPE` can be blank, non-standard, or single-valued (e.g., `"ShipTo"` even when both `IS_DEFAULT_SHIP_TO='Y'` and `IS_DEFAULT_SOLD_TO='Y'`). To eliminate data ambiguity and missing addresses, roles are derived strictly from boolean flags:

- `IS_DEFAULT_BILL_TO == 'Y'` (and `IS_BILL_TO in ('Y', None)`) → **`BillTo`**
- `IS_DEFAULT_SHIP_TO == 'Y'` (and `IS_SHIP_TO in ('Y', None)`) → **`ShipTo`**
- `IS_DEFAULT_SOLD_TO == 'Y'` (and `IS_SOLD_TO in ('Y', None)`) → **`SoldTo`**

A single Oracle row can satisfy **multiple roles** simultaneously (e.g., both `BillTo` and `SoldTo`). When this occurs, Phase 1 creates DynamoDB cache rows for every qualifying role.

#### Snapshot (`r`) vs. Live CDC (`c`) Processing:
- **Snapshot Reads (`operation == 'r'`):** If a record has no qualifying default roles, it is immediately fast-skipped without performing any DynamoDB reads (saving hundreds of thousands of read units during full re-snapshots).
- **Live CDC (`operation != 'r'`):** The lambda checks the reverse lookup (`CUSTOMER_ADDNL_ADDRESS_KEY`) to compare existing roles with new qualifying roles. Any previously assigned roles that were revoked are automatically cleaned up in DynamoDB and nulled out in OpenSearch.

#### Deletes (`operation == 'd'`):
On a delete event, all active and historical roles associated with the address key are removed or soft-deleted from DynamoDB, and Phase 2 sets the respective address fields to `null` in OpenSearch.

> **Note on Oracle delete behavior:** Debezium with `online_catalog` strategy on a CDB/PDB Oracle setup does not capture the before-image for deletes, resulting in a `payload: null` message. The lambda handles this by decoding the Kafka message **key** to extract `CUSTOMER_ADDNL_ADDRESS_KEY` and performing a reverse DynamoDB lookup to resolve `customerKey` and roles.

---

### Phase 2 — Fan-out to Organization Index (Atomic Update)

Looks up `personInfoKey` → `PERSON_INFO` DynamoDB cache → builds an address object → runs an atomic `update_by_query` on `customer.customerKey` in OpenSearch.

#### Multi-Role Atomic Updates:
When an address record qualifies for multiple roles (e.g., `BillTo` and `SoldTo`), the lambda executes a **single atomic OpenSearch update** using the Painless script `PAINLESS_UPDATE_ORG_ADDRESS_FIELDS`. This avoids multiple sequential `update_by_query` calls and eliminates OpenSearch version conflict drops (`version_conflict_engine_exception`).

**Address object fields (from PERSON_INFO cache):**

| Field | Source |
|---|---|
| `personInfoKey` | from payload |
| `firstName` / `lastName` / `fullName` | `firstName` + `middleName` + `lastName` |
| `emailId` | `emailId` |
| `dayPhone` / `eveningPhone` / `mobilePhone` | `dayPhone` / `eveningPhone` / `mobilePhone` |
| `company` | `company` |
| `addressLine1` | `addressLine1` |
| `city` / `state` / `zipCode` / `country` | address fields |

Phase 2 is **skipped** (returns 200) if:
- `PERSON_INFO_KEY` is missing from the payload
- `PERSON_INFO_KEY` is not found in the DynamoDB cache

Fan-out failures are **non-fatal** — logged as warnings, processing continues.

---

## DynamoDB Single-Table Design

Table name: `enrichment_cache` (configurable via `DYNAMODB_TABLE` env var)

| PK | SK | Attributes | Written by | Read by |
|---|---|---|---|---|
| `CUSTOMER_ADDNL_ADDRESS` | `<customerKey>#<role>` | `personInfoKey`, `customerKey`, `addressType`, `roles` | this lambda | other lambdas (`cdc-organization`, `cdc-customer`) |
| `CUSTOMER_ADDNL_ADDRESS_KEY` | `<addnlAddressKey>` | `customerKey`, `roles`, `addressType` | this lambda | this lambda (reverse lookup for role revocation & key-based delete) |
| `PERSON_INFO` | `<personInfoKey>` | person/address fields | `cdc-lambda-personinfo` | this lambda (Phase 2) |

---

## Event Sources

### MSK (production)

Topic consumed:

```
<prefix>.STERLING.yfs_customer_addnl_address
```

### Direct invocation (testing)

```json
{
  "table": "YFS_CUSTOMER_ADDNL_ADDRESS",
  "operation": "c",
  "payload": {
    "CUSTOMER_KEY": "CKEY001",
    "CUSTOMER_ADDNL_ADDRESS_KEY": "ADDR001",
    "PERSON_INFO_KEY": "PIK001",
    "IS_DEFAULT_BILL_TO": "Y",
    "IS_DEFAULT_SOLD_TO": "Y",
    "CUSTOMER_CONTACT_KEY": null
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
| `INDEX_ORGANIZATION` | `organizations` | OpenSearch organizations index name |
| `ENABLE_HARD_DELETE` | `false` | Set to `true` to physically delete; `false` uses soft delete |

---

## Soft Delete Behaviour

By default (`ENABLE_HARD_DELETE=false`) the lambda performs a **soft delete** when `op=d` is received:

### Phase 1 — DynamoDB `soft_delete_cache_item`

Both the forward and reverse cache rows are marked rather than removed:

```python
cache_table.update_item(
    Key={'PK': 'CUSTOMER_ADDNL_ADDRESS', 'SK': f'{customer_key}#{role}'},
    UpdateExpression='SET #d = :true, #da = :now',
    ExpressionAttributeNames={'#d': '_deleted', '#da': '_deletedAt'},
    ExpressionAttributeValues={':true': True, ':now': datetime.now(UTC).isoformat()},
)
```

Cache lookups (`get_cached_customer_addnl_address`) explicitly filter out items marked with `_deleted=True`.

### Phase 2 — Fan-out on Delete

The `update_by_query` executes to null out `billTo` / `shipTo` / `soldTo` on the org document.

### Hard Delete

Set `ENABLE_HARD_DELETE=true` to physically delete items:

```python
cache_table.delete_item(Key={'PK': 'CUSTOMER_ADDNL_ADDRESS', 'SK': f'{customer_key}#{role}'})
cache_table.delete_item(Key={'PK': 'CUSTOMER_ADDNL_ADDRESS_KEY', 'SK': addnl_address_key})
```

---

## Response Codes

| Code | Meaning |
|---|---|
| `200` | Processed successfully (includes skipped non-default or contact-level addresses) |
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

All 72 test cases cover: safe `CUSTOMER_CONTACT_KEY` handling, multi-role derivation and atomic fanout, snapshot fast-skip, live role revocations, tombstone delete key resolution, soft-delete filtering, and OpenSearch client error branches.
