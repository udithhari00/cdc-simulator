# Product Lambda Handler

A Python AWS Lambda triggered via **Lambda Function URL** that receives product data and indexes it into a dedicated **OpenSearch index** (`products`).

---

## Table of Contents

1. [What This Lambda Does](#what-this-lambda-does)
2. [Architecture](#architecture)
3. [OpenSearch Index](#opensearch-index)
4. [Environment Variables](#environment-variables)
5. [OpenSearch Modes](#opensearch-modes)
6. [Request Format](#request-format)
7. [Project Structure](#project-structure)
8. [Local Setup & Testing](#local-setup--testing)
9. [Running Tests & Coverage](#running-tests--coverage)
10. [Testing & Validating from the AWS Console](#testing--validating-from-the-aws-console)
11. [Deployment Notes](#deployment-notes)
12. [Troubleshooting](#troubleshooting)

---

## What This Lambda Does

```
Upstream Service  →  HTTP POST (Lambda Function URL)  →  This Lambda  →  OpenSearch (products index)
```

| Operation | What happens in OpenSearch |
|---|---|
| `u` — UPDATE | Document created or merged via `doc_as_upsert` |
| `i` — INSERT | Treated identically to UPDATE — document is upserted |
| `d` — DELETE | Document removed from the index (404 is silently ignored) |

**Key design decisions:**
- Each product is stored as a **flat, top-level document** identified by `uniqueID`.
- The Lambda always indexes into the `products` index.
- Payloads are **enriched with metadata fields** (`cdcOperation`, `cdcTimestamp`, `indexedAt`) before indexing so you can trace when and how each document was last modified.
- `cdcTimestamp` is taken from `requestContext.timeEpoch` in the Function URL event, falling back to the Lambda processing time if absent.
- The body parser handles **plain JSON, base64-encoded JSON, and direct dict payloads**, making the Lambda compatible with Lambda Function URLs and direct test invocations.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        AWS Lambda                               │
│                                                                 │
│  lambda_handler()  ──────────────────────────────────┐         │
│       │                                              │         │
│       ├─ parse_body()  → extract JSON from event     │         │
│       │                                              │         │
│       ├─ validate payload + uniqueID                 │         │
│       │                                              │         │
│       └─ index into OpenSearch ──────────────────────►         │
│                                              │                  │
│                                    ┌─────────┴──────────┐      │
│                                    ▼                    ▼      │
│                              upsert (u/i)          delete (d)  │
│                                    │                    │      │
│                              OpenSearch           OpenSearch   │
│                            (products index)    (products index) │
└─────────────────────────────────────────────────────────────────┘
```

---

## OpenSearch Index

**Index name:** `products` (overridable via `INDEX_PRODUCTS` env var)

**Document ID:** `uniqueID` (must be present in every payload)

**Sample document:**
```json
{
  "uniqueID":       "prod-abc-123",
  "name":           "Widget Pro",
  "price":          29.99,
  "category":       "Electronics",
  "sku":            "WP-2000",
  "cdcOperation":   "u",
  "cdcTimestamp":   1583348638390,
  "indexedAt":      1700000001234
}
```

**Enrichment fields added by this Lambda:**

| Field | Description |
|---|---|
| `cdcOperation` | The operation that triggered this event (`u`, `i`, `d`) |
| `cdcTimestamp` | `requestContext.timeEpoch` from the Function URL event; falls back to Lambda processing time if absent |
| `indexedAt` | Unix milliseconds recording exactly when this Lambda indexed the document |

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `OPENSEARCH_HOST` | `opensearch` | Hostname of the OpenSearch endpoint (no `https://`) |
| `OPENSEARCH_PORT` | `443` | Port (443 for both serverless and provisioned HTTPS) |
| `OPENSEARCH_SCHEME` | `https` | `https` or `http` (basic auth only) |
| `OPENSEARCH_AUTH` | `iam` | `iam` for AWS SigV4 signing, `basic` for username/password |
| `OPENSEARCH_USERNAME` | `admin` | Used only when `OPENSEARCH_AUTH=basic` |
| `OPENSEARCH_PASSWORD` | `admin` | Used only when `OPENSEARCH_AUTH=basic` |
| `OPENSEARCH_MODE` | `provisioned` | `provisioned` (managed/self-hosted) — reserved for future serverless support |
| `AWS_REGION` | `us-east-1` | AWS region for SigV4 signing |
| `INDEX_PRODUCTS` | `products` | Name of the OpenSearch index. Override per environment |
| `ENABLE_HARD_DELETE` | `false` | Set to `true` to physically delete; `false` uses soft delete |

---

## Soft Delete Behaviour

By default (`ENABLE_HARD_DELETE=false`) a `d` operation performs a **soft delete** — the product document is **updated** rather than removed from the index.

### OpenSearch — `soft_delete_os_doc`

```python
client.update(
    index=INDEX_PRODUCTS,
    id=unique_id,
    body={"doc": {"_deleted": True, "_deletedAt": int(datetime.now(UTC).timestamp() * 1000)}}
)
```

The document remains in the `products` index. To exclude soft-deleted products from search results, add a filter:

```json
{ "term": { "_deleted": false } }
```

> This lambda has **no DynamoDB** — soft delete only affects the OpenSearch document.

### Hard Delete

Set `ENABLE_HARD_DELETE=true` to physically remove the document:

```python
client.delete(index=INDEX_PRODUCTS, id=unique_id, ignore=[404])
```

### Delete Request Format

```json
{
  "body": "{\"operation\": \"d\", \"payload\": {\"uniqueID\": \"prod-abc-123\"}}"
}
```

---

## OpenSearch Modes

### Provisioned (Amazon OpenSearch Service or self-managed)
```
OPENSEARCH_AUTH = iam
OPENSEARCH_MODE = provisioned
```
- SigV4 signing uses service `es`
- Lambda execution role needs `es:ESHttpGet`, `es:ESHttpPut`, `es:ESHttpPost`, `es:ESHttpDelete` on the domain ARN

### Basic Auth (local / non-AWS OpenSearch)
```
OPENSEARCH_AUTH = basic
OPENSEARCH_USERNAME = admin
OPENSEARCH_PASSWORD = yourpassword
```
- Useful for local development with Docker-based OpenSearch
- `verify_certs` is disabled in this mode — **do not use in production**

---

## Request Format

### Lambda Function URL (primary trigger)
The Lambda is triggered via HTTP POST to its Function URL. The request body must be a JSON object:

```json
{
  "operation": "u",
  "payload": {
    "uniqueID":   "prod-abc-123",
    "name":       "Widget Pro",
    "price":      29.99,
    "category":   "Electronics"
  }
}
```

The `body` field in the Lambda event may be:
- A plain **JSON string** (standard Function URL behaviour)
- A **base64-encoded** JSON string (payload format v2 with `isBase64Encoded: true`)
- A **dict** (direct Lambda invocation during testing)

The `cdcTimestamp` is sourced from `requestContext.timeEpoch` in the Function URL event envelope — not from the request body.

### Direct Invocation (testing / backfill)
```json
{
  "body": {
    "operation": "u",
    "payload": {
      "uniqueID": "prod-abc-123",
      "name":     "Widget Pro",
      "price":    29.99
    }
  }
}
```

**Request body field reference:**

| Field | Required | Description |
|---|---|---|
| `operation` | ❌ | `"c"` create, `"u"` update, `"d"` delete. Defaults to `"c"` |
| `payload` | ✅ | The product document. Must be a non-empty dict |
| `payload.uniqueID` | ✅ | Used as the OpenSearch document `_id` |

---

## Project Structure

```
.
├── lambda_function.py       # Main Lambda handler (all logic lives here)
├── test_lambda_function.py  # Pytest test suite
├── requirements.txt         # Runtime dependencies
└── README.md                # This file
```

---

## Local Setup & Testing

### Prerequisites
- Python 3.11+
- pip

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Set environment variables for local testing
```bash
export OPENSEARCH_HOST=localhost
export OPENSEARCH_PORT=9200
export OPENSEARCH_SCHEME=http
export OPENSEARCH_AUTH=basic
export OPENSEARCH_USERNAME=admin
export OPENSEARCH_PASSWORD=admin
export OPENSEARCH_MODE=provisioned
export AWS_REGION=us-east-1
```

### 3. (Optional) Start local OpenSearch with Docker
```bash
docker run -d \
  --name opensearch-local \
  -p 9200:9200 -p 9600:9600 \
  -e "discovery.type=single-node" \
  -e "OPENSEARCH_INITIAL_ADMIN_PASSWORD=Admin@1234" \
  opensearchproject/opensearch:2.13.0
```

---

## Running Tests & Coverage

```bash
# Run all tests with coverage report
pytest test_lambda_function.py -v --cov=lambda_function --cov-report=term-missing

# Generate an HTML coverage report (opens in browser)
pytest test_lambda_function.py --cov=lambda_function --cov-report=html
open htmlcov/index.html

# Run only a specific test class
pytest test_lambda_function.py::TestLambdaHandlerDelete -v
```

---

## Testing & Validating from the AWS Console

### Step 1 — Test a CREATE / UPDATE event (direct invocation)

Open the Lambda function in the AWS Console → **Test** tab → create a new test event:

```json
{
  "body": "{\"operation\":\"u\",\"payload\":{\"uniqueID\":\"prod-test-001\",\"name\":\"Console Test Widget\",\"price\":49.99,\"category\":\"Electronics\"}}"
}
```

> **Note:** The `body` value must be a **JSON-escaped string**, exactly as a Function URL sends it. Copy the payload above as-is.

**Expected response:**
```json
{
  "statusCode": 200,
  "body": "Upserted doc id='prod-test-001' into index='products'"
}
```

---

### Step 2 — Test an UPDATE event

```json
{
  "body": "{\"operation\":\"u\",\"payload\":{\"uniqueID\":\"prod-test-001\",\"name\":\"Console Test Widget\",\"price\":39.99,\"category\":\"Electronics\"}}"
}
```

**Expected response:**
```json
{
  "statusCode": 200,
  "body": "Upserted doc id='prod-test-001' into index='products'"
}
```

---

### Step 3 — Test a DELETE event

```json
{
  "body": "{\"operation\":\"d\",\"payload\":{\"uniqueID\":\"prod-test-001\"}}"
}
```

**Expected response (document existed):**
```json
{
  "statusCode": 200,
  "body": "Deleted doc id='prod-test-001' from index='products'"
}
```

**Expected response (document did not exist):**
```json
{
  "statusCode": 200,
  "body": "Doc id='prod-test-001' not found in index='products' — nothing to delete"
}
```

> **Note:** A 404 (document not found) is **not** treated as an error — the Lambda still returns `200`. However, a warning is logged to CloudWatch so phantom deletes from upstream are visible.

---

### Step 4 — Test validation errors

**Missing `uniqueID`:**
```json
{
  "body": "{\"operation\":\"u\",\"payload\":{\"name\":\"No ID Widget\"}}"
}
```
Expected: `400` — `"Payload must include 'uniqueID'"`

---

**Missing `payload` field:**
```json
{
  "body": "{\"operation\":\"u\"}"
}
```
Expected: `400` — `"Request must include a 'payload' field"`

---

**No body at all:**
```json
{
  "randomKey": "randomValue"
}
```
Expected: `400` — `"Request body is missing or could not be parsed."`

---

### Step 5 — Verify the document in OpenSearch

After a successful CREATE or UPDATE, verify the document was indexed by querying OpenSearch directly (via Dev Tools in OpenSearch Dashboards or curl):

```bash
# Check the document exists
GET products/_doc/prod-test-001

# Search by category
GET products/_search
{
  "query": {
    "term": { "category": "Electronics" }
  }
}

# Check enrichment fields were added
GET products/_search
{
  "query": { "match_all": {} },
  "_source": ["uniqueID", "cdcOperation", "cdcTimestamp", "indexedAt"]
}
```

---

### Step 6 — Test via AWS CLI (from your terminal)

Make sure you have the AWS CLI configured (`aws configure`).

**Upsert event:**
```bash
aws lambda invoke \
  --function-name YOUR_FUNCTION_NAME \
  --payload '{"body":"{\"operation\":\"u\",\"payload\":{\"uniqueID\":\"prod-cli-001\",\"name\":\"CLI Widget\",\"price\":9.99}}"}' \
  --cli-binary-format raw-in-base64-out \
  response.json && cat response.json
```

**Delete event:**
```bash
aws lambda invoke \
  --function-name YOUR_FUNCTION_NAME \
  --payload '{"body":"{\"operation\":\"d\",\"payload\":{\"uniqueID\":\"prod-cli-001\"}}"}' \
  --cli-binary-format raw-in-base64-out \
  response.json && cat response.json
```

---

### Step 7 — Check CloudWatch Logs

After any test invocation, go to **CloudWatch → Log groups → `/aws/lambda/<function-name>`** to see:

- OpenSearch client startup diagnostics (host, port, auth mode, region)
- Per-invocation processing logs (`Upserted doc id='...'`)
- Any errors with full stack traces

---

## Deployment Notes

### IAM Permissions Required

```json
{
  "Effect": "Allow",
  "Action": [
    "es:ESHttpGet",
    "es:ESHttpPut",
    "es:ESHttpPost",
    "es:ESHttpDelete"
  ],
  "Resource": "arn:aws:es:<region>:<account>:domain/<domain-name>/*"
}
```

### Lambda Configuration
- **Runtime:** Python 3.11
- **Memory:** 256 MB minimum
- **Timeout:** 30 seconds minimum (OpenSearch client timeout is 15s — Lambda timeout must exceed this)
- **VPC:** Must be in the same VPC as the OpenSearch domain (provisioned), or have internet access (serverless via VPC endpoint)
- **Trigger:** Lambda Function URL

### Recommended Production Settings

| Setting | Recommendation |
|---|---|
| Dead Letter Queue | Configure an SQS DLQ on the Lambda trigger to capture failed events |
| Concurrency | Set reserved concurrency to prevent overwhelming OpenSearch during traffic spikes |
| Secrets | Move `OPENSEARCH_USERNAME` / `OPENSEARCH_PASSWORD` to AWS Secrets Manager |
| Monitoring | Create a CloudWatch alarm on Lambda errors and `5xx` response counts |

---

## Troubleshooting

| Symptom | Likely Cause | Fix |
|---|---|---|
| `RuntimeError: AWS credentials not found` | Lambda execution role has no IAM credentials | Attach the correct IAM role to the Lambda |
| `403 Forbidden` from OpenSearch | IAM role lacks `es:ESHttp*` permissions | Update the resource-based policy on the OpenSearch domain |
| `400 Payload must include 'uniqueID'` | Product payload has no primary key | Verify the upstream service includes `uniqueID` in all payloads |
| `400 Request must include a 'payload' field` | Request body is missing the `payload` key | Ensure the caller sends `{"operation": "...", "payload": {...}}` |
| `500 Upsert failed: ...` | OpenSearch rejected the document | Check CloudWatch logs for the full error; often a mapping conflict or cluster health issue |
| `400 Request body is missing or could not be parsed.` | Event has no `body` key | Wrap the payload in a `body` field; see [Request Format](#request-format) |
| Documents not immediately searchable | `refresh=False` is set on upsert | Change to `refresh="wait_for"` if read-your-writes consistency is needed |
| Lambda times out | Client timeout (15s) exceeds Lambda timeout | Set Lambda timeout to at least 30 seconds |