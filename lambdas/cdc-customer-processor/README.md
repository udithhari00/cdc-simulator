# cdc-lambda-customer

AWS Lambda that processes **YFS_CUSTOMER** CDC events from Oracle via MSK (Kafka / Debezium).

---

## What it does

Customer data is **not indexed in its own index**. This lambda caches customer data in DynamoDB and enriches the organization index via fan-out.

### Four-Phase Pipeline

| Phase | Description |
|---|---|
| **1 — Cache** | Write `PK=CUSTOMER, SK=<customerKey>` + `PK=CUSTOMER_BY_ORG_KEY, SK=<buyerOrgCode>` |
| **2 — Fan-out** | Update org doc's nested `customer` object (hash-gated) |
| **3 — Address** | Read `CUSTOMER_ADDNL_ADDRESS` + `PERSON_INFO` → update `billTo`/`shipTo`/`soldTo` on org doc |
| **4 — Contacts** | Query `CUSTOMER_CONTACT` (begins_with) → update `customerContact` array on org doc |

---

## Phase 1 — DynamoDB Cache

Writes two entries per customer event:

- **`PK=CUSTOMER, SK=<customerKey>`** — base customer record
- **`PK=CUSTOMER_BY_ORG_KEY, SK=<buyerOrgCode>`** — customer fields keyed by org code, includes `customerHash`

**Hash optimization:** Before writing, fetches existing `customerHash`. If unchanged, Phase 2 fan-out is skipped.

On **delete**: both entries removed, Phases 2–4 run to clear all enriched fields.

---

## Phase 2 — Fan-out to Organization Index (conditional)

Only runs when `customerHash` has changed.

- **Query:** `organizationKey == BUYER_ORGANIZATION_CODE` AND `customerHash != new_hash`
- **Script:** Sets 16 customer fields + `customerHash` on org doc, rebuilds `searchableText`

Fan-out failures are **non-fatal**.

---

## Phase 3 — Address Enrichment

1. Sanitize `customerKey` by stripping trailing whitespace (`str(customer_key).strip()`)
2. Query `PK=CUSTOMER_ADDNL_ADDRESS, SK begins_with <customerKey>#` from cache
3. Filter out any soft-deleted records (`_deleted=true`)
4. For each returned item, extract address role from SK suffix (`BillTo`/`ShipTo`/`SoldTo`)
5. Look up `personInfoKey` → `PK=PERSON_INFO, SK=<personInfoKey>`
6. Build full address object (fullName, email, phone, company, city, state, zip, country)
7. `update_by_query` on `customer.customerKey` → sets `billTo`/`shipTo`/`soldTo` on org doc

On **delete**: all three address fields are set to `null`.

---

## Phase 4 — Contact Array Enrichment

1. Sanitize `customerKey` by stripping trailing whitespace (`str(customer_key).strip()`)
2. Query `PK=CUSTOMER_CONTACT, SK begins_with <customerKey>#`
3. Filter out any soft-deleted records (`_deleted=true`)
4. Build array of contact objects (contactName, contactEmail, contactPhone, etc.)
5. `update_by_query` on `customer.customerKey` → sets `customerContact` as array on org doc

On **delete**: `customerContact` is removed from org doc.

---

## DynamoDB Access Patterns

Table: `enrichment_cache`

| PK | SK | Access |
|---|---|---|
| `CUSTOMER` | `<customerKey>` | Write (Phase 1) |
| `CUSTOMER_BY_ORG_KEY` | `<buyerOrgCode>` | Write (Phase 1) |
| `CUSTOMER_ADDNL_ADDRESS` | `<customerKey>#<addressType>` | Read (Phase 3) |
| `PERSON_INFO` | `<personInfoKey>` | Read (Phase 3) |
| `CUSTOMER_CONTACT` | `<customerKey>#<contactKey>` | Query begins_with (Phase 4) |

---

## Hash-Based Optimization

```
compute_customer_hash(payload) = SHA-256(json.dumps(14 fields, sort_keys=True))[:16]
```

1. Compute `new_hash` → fetch `existing_hash` from `CUSTOMER_BY_ORG_KEY`
2. If equal → skip Phase 2
3. `update_by_query` uses `must_not { customerHash: new_hash }` for server-side skip

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `DYNAMODB_TABLE` | `enrichment_cache` | DynamoDB table name |
| `DYNAMODB_ENDPOINT` | *(unset)* | Local DynamoDB endpoint (dev only) |
| `OPENSEARCH_HOST` | `opensearch` | OpenSearch host |
| `OPENSEARCH_PORT` | `443` | OpenSearch port |
| `OPENSEARCH_AUTH` | `iam` | `iam` or `basic` |
| `OPENSEARCH_MODE` | `serverless` | `provisioned` or `serverless` |
| `AWS_REGION` | `us-east-1` | AWS region |
| `INDEX_ORGANIZATION` | `organizations` | Organization index name |
| `ENABLE_HARD_DELETE` | `false` | Set to `true` to physically delete; `false` uses soft delete |

---

## Soft Delete Behaviour

By default (`ENABLE_HARD_DELETE=false`) the lambda performs a **soft delete** when a `d` CDC event is received, instead of physically removing records.

### DynamoDB — `soft_delete_cache_item(pk, sk)`

Both cache entries are marked rather than erased:

```python
cache_table.update_item(
    Key={'PK': pk, 'SK': sk},
    UpdateExpression='SET #d = :true, #da = :now',
    ExpressionAttributeNames={'#d': '_deleted', '#da': '_deletedAt'},
    ExpressionAttributeValues={':true': True, ':now': datetime.now(UTC).isoformat()},
)
```

Affected rows:

| PK | SK |
|---|---|
| `CUSTOMER` | `<customerKey>` |
| `CUSTOMER_BY_ORG_KEY` | `<buyerOrgCode>` |

### Cache Lookup Awareness

All cache reads check the `_deleted` flag and treat marked items as absent:

```python
if item and item.get('_deleted'):
    return None   # treated as not found
```

The `_deleted` and `_deletedAt` fields are stripped from returned items so they never appear in enriched documents.

### Hard Delete

Set `ENABLE_HARD_DELETE=true` to physically remove both DynamoDB rows on delete:

```python
cache_table.delete_item(Key={'PK': ENTITY_CUSTOMER, 'SK': customer_key})
cache_table.delete_item(Key={'PK': ENTITY_CUSTOMER_BY_ORG_KEY, 'SK': buyer_org_code})
```

### Direct Invocation — Delete Payload

```json
{
  "table": "YFS_CUSTOMER",
  "operation": "d",
  "payload": {
    "CUSTOMER_KEY": "<customerKey>",
    "BUYER_ORGANIZATION_CODE": "<buyerOrgCode>"
  }
}
```

> `BUYER_ORGANIZATION_CODE` is optional but required to soft/hard delete the `CUSTOMER_BY_ORG_KEY` cache entry.

---

## Response Codes

| Code | Meaning |
|---|---|
| `200` | All records processed |
| `207` | Partial failure |
| `400` | Missing `CUSTOMER_ID` or unknown table |
| `500` | Unhandled exception |

---

## Running Tests

```bash
python -m pytest test_lambda_function.py -v
```
