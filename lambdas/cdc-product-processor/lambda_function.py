"""
Product Lambda Handler

Triggered via Lambda Function URL. Receives product data in the
request body and indexes it into an OpenSearch 'products' index.

When a product is indexed (c/u operation), the processor checks DynamoDB
(enrichment_cache table, PK=PRODUCT_ADDON) for any pending addon data
cached by the cdc-product-enrich-addon-processor and merges those fields
(uom, currency, defaultProductClass, description, status, primarySupplier)
into the product document before indexing.

Expected request body (single):
  {
    "operation": "c",          // 'c' (create, default), 'u' (update), or 'd' (delete)
    "payload": { ... }         // product document; must contain 'uniqueID'
  }

Expected request body (batch):
  {
    "operation": "c",
    "payload": [ { ... }, { ... } ]   // list of product documents
  }

Environment Variables:
  - OPENSEARCH_HOST
  - OPENSEARCH_PORT
  - OPENSEARCH_SCHEME
  - OPENSEARCH_AUTH        ('iam' or 'basic')
  - OPENSEARCH_USERNAME
  - OPENSEARCH_PASSWORD
  - OPENSEARCH_MODE        ('provisioned')
  - AWS_REGION
  - INDEX_PRODUCTS         (default: 'products')
  - DYNAMODB_TABLE         (default: 'enrichment_cache')
  - DYNAMODB_ENDPOINT      (optional, for local testing)
"""

import base64
import json
import logging
import os
from datetime import datetime, UTC
from typing import Any, Optional
import boto3
from botocore.exceptions import ClientError
from dateutil import parser as dateutil_parser
from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth

# =============================================================================
# Logging
# =============================================================================

class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        log = {
            'timestamp': datetime.now(UTC).isoformat(),
            'level':     record.levelname,
            'message':   record.getMessage(),
        }
        if record.exc_info:
            log['exception'] = self.formatException(record.exc_info)
        return json.dumps(log)

logger = logging.getLogger()
logger.setLevel(os.environ.get('LOG_LEVEL', 'INFO').upper())
for handler in logger.handlers:
    handler.setFormatter(JsonFormatter())

# =============================================================================
# Environment Variables
# =============================================================================
OPENSEARCH_HOST     = os.environ.get("OPENSEARCH_HOST", "opensearch")
OPENSEARCH_PORT     = int(os.environ.get("OPENSEARCH_PORT", "443"))
OPENSEARCH_SCHEME   = os.environ.get("OPENSEARCH_SCHEME", "https")
OPENSEARCH_AUTH     = os.environ.get("OPENSEARCH_AUTH", "iam")   # 'iam' or 'basic'
OPENSEARCH_USERNAME = os.environ.get("OPENSEARCH_USERNAME", "admin")
OPENSEARCH_PASSWORD = os.environ.get("OPENSEARCH_PASSWORD", "admin")
OPENSEARCH_MODE     = os.environ.get("OPENSEARCH_MODE", "provisioned")
AWS_REGION          = os.environ.get("AWS_REGION", "us-east-1")
INDEX_PRODUCTS      = os.environ.get("INDEX_PRODUCT", "products")
ENABLE_HARD_DELETE  = os.environ.get('ENABLE_HARD_DELETE', 'false').lower() == 'true'
DYNAMODB_TABLE      = os.environ.get("DYNAMODB_TABLE", "enrichment_cache")
DYNAMODB_ENDPOINT   = os.environ.get("DYNAMODB_ENDPOINT")

ENTITY_PRODUCT_ADDON = "PRODUCT_ADDON"

# Fields managed by the addon enrichment processor
ADDON_FIELDS = ["uom", "currency", "defaultProductClass", "description", "status", "primarySupplier"]


# =============================================================================
# OpenSearch Client (lazy singleton)
# =============================================================================
opensearch_client = None


def get_opensearch_client() -> OpenSearch:
    global opensearch_client
    if opensearch_client:
        return opensearch_client

    if OPENSEARCH_AUTH == "iam":
        credentials = boto3.Session().get_credentials()
        if not credentials:
            raise RuntimeError(
                "AWS credentials not found. "
                "Ensure the Lambda execution role has the required permissions."
            )
        auth = AWSV4SignerAuth(credentials, AWS_REGION, "es")
        opensearch_client = OpenSearch(
            hosts=[{"host": OPENSEARCH_HOST, "port": OPENSEARCH_PORT}],
            http_auth=auth,
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection,
            timeout=30,
            retry_on_timeout=True,
            max_retries=3
        )
    else:
        opensearch_client = OpenSearch(
            hosts=[{"host": OPENSEARCH_HOST, "port": OPENSEARCH_PORT}],
            http_auth=(OPENSEARCH_USERNAME, OPENSEARCH_PASSWORD),
            use_ssl=(OPENSEARCH_SCHEME == "https"),
            verify_certs=False,
            ssl_show_warn=False,
            connection_class=RequestsHttpConnection,
            timeout=30,
            retry_on_timeout=True,
            max_retries=3
        )

    return opensearch_client


# =============================================================================
# DynamoDB Client (module-level, shared across warm invocations)
# =============================================================================

_dynamodb_kwargs: dict = {"region_name": AWS_REGION}
if DYNAMODB_ENDPOINT:
    _dynamodb_kwargs["endpoint_url"] = DYNAMODB_ENDPOINT
    _dynamodb_kwargs["aws_access_key_id"] = os.environ.get("AWS_ACCESS_KEY_ID", "local")
    _dynamodb_kwargs["aws_secret_access_key"] = os.environ.get("AWS_SECRET_ACCESS_KEY", "local")

dynamodb    = boto3.resource("dynamodb", **_dynamodb_kwargs)
cache_table = dynamodb.Table(DYNAMODB_TABLE)


# =============================================================================
# Helpers
# =============================================================================

def now_ms() -> int:
    """Current UTC time in milliseconds."""
    return int(datetime.now(UTC).timestamp() * 1000)


def to_epoch_ms(value: str) -> Optional[int]:
    """Parse any date string to epoch millis using dateutil. Returns None on failure."""
    try:
        return int(dateutil_parser.parse(value).timestamp() * 1000)
    except (ValueError, OverflowError) as exc:
        logger.warning(f"Could not parse date '{value}': {exc}")
        return None


def parse_body(event: dict) -> Optional[dict]:
    """
    Extract and parse the request body from a Lambda Function URL event.
    Handles plain dict bodies, JSON strings, and base64-encoded strings.
    Returns None if the body is missing or cannot be parsed.
    """
    body = event.get("body")
    if body is None:
        return None

    # Already a dict (direct Lambda invoke with a dict body)
    if isinstance(body, dict):
        return body

    # String — may be base64-encoded (API Gateway payload format v2)
    if isinstance(body, str):
        try:
            if event.get("isBase64Encoded"):
                body = base64.b64decode(body).decode("utf-8")
            return json.loads(body)
        except json.JSONDecodeError as exc:
            logger.warning(f"Failed to JSON-parse body string: {exc}")
        except (ValueError, UnicodeDecodeError) as exc:
            logger.warning(f"Failed to base64-decode body: {exc}")
        return None

    return None


def fetch_and_merge_addon(part_number: str, payload: dict) -> dict:
    """
    Check DynamoDB for pending addon data for the given partNumber.
    If found, merge addon fields into the product payload (addon fields
    will NOT overwrite existing values already in the payload).
    After successful merge, delete the DynamoDB cache entry.
    Returns the (potentially enriched) payload.
    """
    try:
        response = cache_table.get_item(
            Key={"PK": ENTITY_PRODUCT_ADDON, "SK": part_number},
            ConsistentRead=False,
        )
        addon_item = response.get("Item")
        if not addon_item:
            return payload

        merged_fields = []
        for field in ADDON_FIELDS:
            if field in addon_item and addon_item[field] is not None:
                # Only merge if the payload doesn't already have this field set
                if field not in payload or payload[field] is None:
                    payload[field] = addon_item[field]
                    merged_fields.append(field)

        if merged_fields:
            payload["addonEnrichedAt"] = int(datetime.now(UTC).timestamp() * 1000)
            logger.info(
                f"Addon backfill merged | partNumber={part_number} "
                f"mergedFields={merged_fields}"
            )
        else:
            logger.info(f"Addon found but no new fields to merge | partNumber={part_number}")

        return payload

    except ClientError as exc:
        logger.warning(
            f"DynamoDB addon lookup failed (non-fatal) | partNumber={part_number} error={exc}"
        )
        return payload


def process_one(client: OpenSearch, operation: str, payload: dict, request_time: int) -> dict:
    """
    Process a single product payload. Returns a result dict with 'id' and 'status'.
    """
    doc_id = payload.get("uniqueID")
    if not doc_id:
        logger.error(f"Payload missing 'uniqueID': {str(payload)[:200]}")
        return {"id": None, "status": "error", "reason": "missing uniqueID"}

    # Normalize startDate to epoch millis so OpenSearch can parse it
    if "startDate" in payload and isinstance(payload["startDate"], str):
        parsed = to_epoch_ms(payload["startDate"])
        if parsed is not None:
            payload["startDate"] = parsed
        else:
            del payload["startDate"]

    # Backfill addon fields from DynamoDB if partNumber is present
    part_number = payload.get("partNumber")
    if part_number:
        payload = fetch_and_merge_addon(str(part_number).strip(), payload)

    # Enrich
    payload["cdcOperation"] = operation
    payload["cdcTimestamp"] = request_time
    payload["indexedAt"]    = now_ms()
    payload["_deleted"]     = False
    payload["_deletedAt"]   = None

    op = operation
    if op not in ("c", "u", "d"):
        logger.warning(f"Unknown operation '{op}' for id='{doc_id}', treating as upsert")
        op = "u"

    if op == "d":
        try:
            if ENABLE_HARD_DELETE:
                resp = client.delete(index=INDEX_PRODUCTS, id=doc_id, ignore=[404])
                if resp.get("result") == "not_found":
                    logger.warning(f"Doc id='{doc_id}' not found — nothing to delete")
                    return {"id": doc_id, "status": "not_found"}
                logger.info(f"Hard-deleted doc id='{doc_id}'")
            else:
                try:
                    client.update(
                        index=INDEX_PRODUCTS,
                        id=doc_id,
                        body={
                            "doc": {
                                "_deleted": True,
                                "_deletedAt": now_ms(),
                            }
                        },
                        refresh=False,
                    )
                except Exception as e:
                    if '404' not in str(e) and 'not_found' not in str(e):
                        raise
                    logger.warning(f"Doc id='{doc_id}' not found — nothing to soft-delete")
                    return {"id": doc_id, "status": "not_found"}
                logger.info(f"Soft-deleted doc id='{doc_id}'")
            return {"id": doc_id, "status": "deleted"}
        except Exception as exc:
            logger.error(f"Failed to delete doc id='{doc_id}': {exc}")
            return {"id": doc_id, "status": "error", "reason": str(exc)}

    try:
        client.update(
            index=INDEX_PRODUCTS,
            id=doc_id,
            body={"doc": payload, "doc_as_upsert": True},
            refresh=False,
        )
        logger.info(f"Upserted doc id='{doc_id}'")
        return {"id": doc_id, "status": "upserted"}
    except Exception as exc:
        logger.error(f"Failed to upsert doc id='{doc_id}': {exc}")
        return {"id": doc_id, "status": "error", "reason": str(exc)}


# =============================================================================
# Entry Point
# =============================================================================

def lambda_handler(event: dict, context: Any) -> dict:
    """
    Main Lambda entry point.
    Accepts a single product (payload: dict) or a batch (payload: list).
    """
    try:
        # Log the incoming event (truncated to avoid flooding CloudWatch)
        preview = json.dumps(event, default=str)
        logger.info(f"Incoming event ({len(preview)} chars): {preview[:2000]}")
        if len(preview) > 2000:
            logger.info("  ... (event truncated in log)")

        body = parse_body(event)
        if body is None:
            logger.error("Request body is missing or could not be parsed.")
            return {"statusCode": 400, "body": "Request body is missing or could not be parsed."}

        operation = body.get("operation", "c").strip()
        payload   = body.get("payload")

        if payload is None:
            return {"statusCode": 400, "body": "Request must include a 'payload' field"}

        # Normalise to a list so single and batch follow the same path
        if isinstance(payload, dict):
            if not payload:
                return {"statusCode": 400, "body": "'payload' must be a non-empty dict or list"}
            items = [payload]
        elif isinstance(payload, list):
            if not payload:
                return {"statusCode": 400, "body": "'payload' list must not be empty"}
            items = payload
        else:
            return {"statusCode": 400, "body": "'payload' must be a dict or list"}

        client       = get_opensearch_client()
        request_time = event.get("requestContext", {}).get("timeEpoch", now_ms())

        results  = [process_one(client, operation, item, request_time) for item in items]
        errors   = [r for r in results if r["status"] == "error"]
        success  = [r for r in results if r["status"] != "error"]

        logger.info(f"Batch complete: {len(success)} succeeded, {len(errors)} failed")

        status_code = 200 if not errors else (207 if success else 500)
        return {
            "statusCode": status_code,
            "body": {"results": results},
        }

    except Exception as exc:
        logger.error(f"Unhandled error in lambda_handler: {exc}", exc_info=True)
        return {"statusCode": 500, "body": str(exc)}
