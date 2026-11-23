"""
CDC Product Addon Processor Lambda

Triggered via Lambda Function URL. Receives addon item data and enriches
existing products in the OpenSearch 'products' index with supplementary
fields (uom, currency, defaultProductClass, description, status, primarySupplier).

If the product does not yet exist in the index (no matching partNumber),
the addon data is cached in DynamoDB (enrichment_cache table) so that the
main cdc-product-processor can backfill these fields when the product
arrives later.

Expected request body:
  {
    "operation": "u",
    "payload": {
      "items": [
        {
          "ItemID": "TPPLATINUM",
          "UnitOfMeasure": "EA",
          "CostCurrency": "USD",
          "DefaultProductClass": "A",
          "Description": "Trivantage Plus Platinum Member",
          "Status": "3000",
          "PrimarySupplier": "15087:11"
        }
      ]
    }
  }

Field Mapping:
  ItemID             -> partNumber (lookup key, not stored separately)
  UnitOfMeasure      -> uom
  CostCurrency       -> currency
  DefaultProductClass-> defaultProductClass
  Description        -> description
  Status             -> status
  PrimarySupplier    -> primarySupplier

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
DYNAMODB_TABLE      = os.environ.get("DYNAMODB_TABLE", "enrichment_cache")
DYNAMODB_ENDPOINT   = os.environ.get("DYNAMODB_ENDPOINT")

ENTITY_PRODUCT_ADDON = "PRODUCT_ADDON"

# Fields that the addon processor manages on the product document
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


def map_addon_fields(item: dict) -> dict:
    """
    Map addon item fields to product index field names.
    Returns a dict with only the mapped fields that have values.
    """
    mapping = {
        "UnitOfMeasure":       "uom",
        "CostCurrency":        "currency",
        "DefaultProductClass": "defaultProductClass",
        "Description":         "description",
        "Status":              "status",
        "PrimarySupplier":     "primarySupplier",
    }

    mapped = {}
    for source_key, target_key in mapping.items():
        value = item.get(source_key)
        if value is not None:
            mapped[target_key] = str(value).strip() if isinstance(value, str) else value

    return mapped


def find_product_doc_id(client: OpenSearch, part_number: str) -> Optional[str]:
    """
    Search the products index for a document with the given partNumber.
    Returns the document _id if found, None otherwise.
    """
    try:
        result = client.search(
            index=INDEX_PRODUCTS,
            body={
                "query": {
                    "term": {
                        "partNumber.keyword": part_number
                    }
                },
                "_source": False,
                "size": 1,
            },
        )
        hits = result.get("hits", {}).get("hits", [])
        if hits:
            return hits[0]["_id"]
        return None
    except Exception as exc:
        logger.error(f"Failed to search for partNumber='{part_number}': {exc}")
        return None


# =============================================================================
# DynamoDB Cache Operations
# =============================================================================

def cache_addon_to_dynamo(part_number: str, addon_data: dict) -> None:
    """
    Cache addon fields in DynamoDB for later backfill by the product processor.
    PK = PRODUCT_ADDON, SK = <partNumber>
    """
    try:
        item = {
            "PK": ENTITY_PRODUCT_ADDON,
            "SK": part_number,
            "updatedAt": now_ms(),
        }
        item.update(addon_data)

        cache_table.put_item(Item=item)
        logger.info(f"DynamoDB cached addon | partNumber={part_number} fields={list(addon_data.keys())}")
    except ClientError as exc:
        logger.error(
            f"DynamoDB cache write failed | partNumber={part_number} error={exc}",
            exc_info=True,
        )
        raise


def delete_addon_from_dynamo(part_number: str) -> None:
    """Delete the addon cache entry from DynamoDB."""
    try:
        cache_table.delete_item(
            Key={"PK": ENTITY_PRODUCT_ADDON, "SK": part_number}
        )
        logger.info(f"DynamoDB addon cache deleted | partNumber={part_number}")
    except ClientError as exc:
        logger.error(
            f"DynamoDB addon cache delete failed | partNumber={part_number} error={exc}",
            exc_info=True,
        )


# =============================================================================
# Process a single addon item
# =============================================================================

def process_one(client: OpenSearch, operation: str, item: dict) -> dict:
    """
    Process a single addon item. Returns a result dict with 'partNumber' and 'status'.
    """
    part_number = item.get("ItemID")
    if not part_number:
        logger.error(f"Item missing 'ItemID': {str(item)[:200]}")
        return {"partNumber": None, "status": "error", "reason": "missing ItemID"}

    part_number = str(part_number).strip()

    # Map source fields to product index fields
    addon_data = map_addon_fields(item)
    if not addon_data:
        logger.warning(f"No mappable fields found for ItemID='{part_number}'")
        return {"partNumber": part_number, "status": "skipped", "reason": "no mappable fields"}

    op = operation.strip().lower() if operation else "u"
    if op not in ("c", "u", "d"):
        logger.warning(f"Unknown operation '{op}' for ItemID='{part_number}', treating as upsert")
        op = "u"

    # --- Delete operation ---
    if op == "d":
        return _handle_delete(client, part_number)

    # --- Create / Update operation ---
    return _handle_upsert(client, part_number, addon_data)


def _handle_upsert(client: OpenSearch, part_number: str, addon_data: dict) -> dict:
    """Enrich the product if it exists, otherwise cache to DynamoDB."""
    doc_id = find_product_doc_id(client, part_number)

    if doc_id:
        # Product exists — partial update with addon fields
        try:
            update_body = dict(addon_data)
            update_body["addonEnrichedAt"] = now_ms()

            client.update(
                index=INDEX_PRODUCTS,
                id=doc_id,
                body={"doc": update_body},
                refresh=False,
            )
            logger.info(f"Enriched product | partNumber={part_number} docId={doc_id} fields={list(addon_data.keys())}")

            # Also update/create the DynamoDB cache so future re-indexes keep the addon data
            cache_addon_to_dynamo(part_number, addon_data)

            return {"partNumber": part_number, "status": "enriched", "docId": doc_id}
        except Exception as exc:
            logger.error(f"Failed to enrich product | partNumber={part_number} docId={doc_id}: {exc}")
            return {"partNumber": part_number, "status": "error", "reason": str(exc)}
    else:
        # Product not found — cache addon data to DynamoDB for backfill
        try:
            cache_addon_to_dynamo(part_number, addon_data)
            return {"partNumber": part_number, "status": "cached_for_backfill"}
        except Exception as exc:
            return {"partNumber": part_number, "status": "error", "reason": str(exc)}


def _handle_delete(client: OpenSearch, part_number: str) -> dict:
    """Remove addon fields from the product document and delete DynamoDB cache."""
    doc_id = find_product_doc_id(client, part_number)

    if doc_id:
        try:
            # Null out all addon-managed fields
            null_fields = {field: None for field in ADDON_FIELDS}
            null_fields["addonEnrichedAt"] = None

            client.update(
                index=INDEX_PRODUCTS,
                id=doc_id,
                body={"doc": null_fields},
                refresh=False,
            )
            logger.info(f"Cleared addon fields from product | partNumber={part_number} docId={doc_id}")
        except Exception as exc:
            logger.error(f"Failed to clear addon fields | partNumber={part_number} docId={doc_id}: {exc}")
            return {"partNumber": part_number, "status": "error", "reason": str(exc)}

    # Always clean up DynamoDB cache regardless of whether product exists
    delete_addon_from_dynamo(part_number)

    return {"partNumber": part_number, "status": "addon_deleted"}


# =============================================================================
# Entry Point
# =============================================================================

def lambda_handler(event: dict, context: Any) -> dict:
    """
    Main Lambda entry point.
    Accepts addon items in payload.items list.
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

        operation = body.get("operation", "u").strip()
        payload   = body.get("payload")

        if payload is None:
            return {"statusCode": 400, "body": "Request must include a 'payload' field"}

        # Extract items from payload
        if isinstance(payload, dict):
            items = payload.get("items")
        elif isinstance(payload, list):
            # Support flat list of items as well
            items = payload
        else:
            return {"statusCode": 400, "body": "'payload' must be a dict with 'items' or a list"}

        if not items or not isinstance(items, list):
            return {"statusCode": 400, "body": "'payload.items' must be a non-empty list"}

        client = get_opensearch_client()

        results = [process_one(client, operation, item) for item in items]
        errors  = [r for r in results if r["status"] == "error"]
        success = [r for r in results if r["status"] != "error"]

        logger.info(f"Batch complete: {len(success)} succeeded, {len(errors)} failed")

        status_code = 200 if not errors else (207 if success else 500)
        return {
            "statusCode": status_code,
            "body": {"results": results},
        }

    except Exception as exc:
        logger.error(f"Unhandled error in lambda_handler: {exc}", exc_info=True)
        return {"statusCode": 500, "body": str(exc)}
