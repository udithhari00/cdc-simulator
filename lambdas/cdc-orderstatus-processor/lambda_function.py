"""
Order Status Update Lambda

Triggered via Lambda Function URL. Receives order status data in the
request body, saves it to the DynamoDB enrichment cache under PK=ORDER_STATUS,
and performs two OpenSearch updates:
  1. Updates statusName and statusCode on the matching document in the orders index.
  2. Updates statusName and statusCode on each order-line document in the order-lines
     index using the per-line entries from enrich.orderlinekey.

Expected request body:
  {
    "operation": "u",           // 'c' (create), 'u' (update) — reserved for future use
    "payload": {
      "orderHeaderKey": "...",  // unique order identifier (used as DynamoDB SK and OS doc id)
      "statusName": "...",      // human-readable status name
      "statusCode": "...",      // (optional) order status code — reserved for future use
      "enrich": {               // enrichment data containing order line statuses
        "orderlinekey": [
          {"OrderLineKey": "...", "Status": "...", "StatusCode": "..."},
          ...
        ]
      }
    }
  }

Environment Variables:
  DYNAMODB_TABLE        DynamoDB table name (default: enrichment_cache)
  DYNAMODB_ENDPOINT     Optional local endpoint override
  OPENSEARCH_HOST
  OPENSEARCH_PORT       (default: 443)
  OPENSEARCH_SCHEME     (default: https)
  OPENSEARCH_AUTH       'iam' or 'basic' (default: iam)
  OPENSEARCH_USERNAME   used when OPENSEARCH_AUTH=basic
  OPENSEARCH_PASSWORD   used when OPENSEARCH_AUTH=basic
  OPENSEARCH_MODE       'provisioned' or 'serverless' (default: serverless)
  AWS_REGION            (default: us-east-1)
  INDEX_ORDER           OpenSearch orders index name (default: orders)
  INDEX_ORDER_LINES     OpenSearch order-lines index name (default: order-lines)
"""

import base64
import json
import logging
import os
from datetime import datetime, UTC
from typing import Any, Optional

import boto3
from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth


# =============================================================================
# Logging
# =============================================================================

class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        log = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level":     record.levelname,
            "message":   record.getMessage(),
        }
        if record.exc_info:
            log["exception"] = self.formatException(record.exc_info)
        return json.dumps(log)


logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO").upper())
for _handler in logger.handlers:
    _handler.setFormatter(JsonFormatter())


# =============================================================================
# Environment Variables
# =============================================================================

DYNAMODB_TABLE    = os.environ.get("DYNAMODB_TABLE", "enrichment_cache")
DYNAMODB_ENDPOINT = os.environ.get("DYNAMODB_ENDPOINT")
OPENSEARCH_HOST   = os.environ.get("OPENSEARCH_HOST", "opensearch")
OPENSEARCH_PORT   = int(os.environ.get("OPENSEARCH_PORT", "443"))
OPENSEARCH_SCHEME = os.environ.get("OPENSEARCH_SCHEME", "https")
OPENSEARCH_AUTH   = os.environ.get("OPENSEARCH_AUTH", "iam")
OPENSEARCH_USERNAME = os.environ.get("OPENSEARCH_USERNAME", "admin")
OPENSEARCH_PASSWORD = os.environ.get("OPENSEARCH_PASSWORD", "admin")
OPENSEARCH_MODE   = os.environ.get("OPENSEARCH_MODE", "serverless")
AWS_REGION        = os.environ.get("AWS_REGION", "us-east-1")
INDEX_ORDER       = os.environ.get("INDEX_ORDER", "orders")
INDEX_ORDER_LINES = os.environ.get("INDEX_ORDER_LINES", "order-lines")

ENTITY_ORDER_STATUS = "ORDER_STATUS"
ENABLE_HARD_DELETE  = os.environ.get('ENABLE_HARD_DELETE', 'false').lower() == 'true'


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
# OpenSearch Client (lazy singleton)
# =============================================================================

opensearch_client: Optional[OpenSearch] = None


def get_opensearch_client() -> OpenSearch:
    global opensearch_client
    if opensearch_client:
        return opensearch_client

    if OPENSEARCH_AUTH == "iam":
        credentials = boto3.Session().get_credentials()
        if not credentials:
            raise RuntimeError("AWS credentials not found")
        iam_service = "es" if OPENSEARCH_MODE == "provisioned" else "aoss"
        auth = AWSV4SignerAuth(credentials, AWS_REGION, iam_service)
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
# Helpers
# =============================================================================

def parse_body(event: dict) -> Optional[dict]:
    """
    Extract and parse the JSON request body from a Lambda Function URL event.
    Handles plain dict bodies, JSON strings, and base64-encoded strings.
    """
    body = event.get("body")
    if body is None:
        return None
    if isinstance(body, dict):
        return body
    if isinstance(body, str):
        try:
            if event.get("isBase64Encoded"):
                body = base64.b64decode(body).decode("utf-8")
            return json.loads(body)
        except (json.JSONDecodeError, ValueError, UnicodeDecodeError) as exc:
            logger.warning(f"Failed to parse request body: {exc}")
        return None
    return None


def now_ms() -> int:
    return int(datetime.now(UTC).timestamp() * 1000)


# =============================================================================
# DynamoDB — upsert / delete order status in cache
# =============================================================================

def save_order_status(
    order_header_key: str,
    status_name: str,
    enrich: dict,
    status_code: Optional[str] = None,
) -> None:
    item = {
        "PK":          ENTITY_ORDER_STATUS,
        "SK":          order_header_key,
        "statusName":  status_name,
        "enrich":      enrich,
        "updatedAt":   now_ms(),
    }
    if status_code:
        item["statusCode"] = status_code
    cache_table.put_item(Item=item)
    logger.info(
        f"DynamoDB saved | PK={ENTITY_ORDER_STATUS} SK={order_header_key} "
        f"statusName={status_name} statusCode={status_code} "
        f"enrichLineCount={len(enrich.get('orderlinekey', []))}"
    )


def soft_delete_cache_item(pk: str, sk: str) -> None:
    """Soft-delete a DynamoDB cache row by marking it as deleted instead of removing it."""
    cache_table.update_item(
        Key={'PK': pk, 'SK': sk},
        UpdateExpression='SET #d = :true, #da = :now',
        ExpressionAttributeNames={'#d': '_deleted', '#da': '_deletedAt'},
        ExpressionAttributeValues={':true': True, ':now': datetime.now(UTC).isoformat()},
    )


def delete_order_status(order_header_key: str) -> None:
    if ENABLE_HARD_DELETE:
        cache_table.delete_item(Key={"PK": ENTITY_ORDER_STATUS, "SK": order_header_key})
        logger.info(f"DynamoDB hard-deleted | PK={ENTITY_ORDER_STATUS} SK={order_header_key}")
    else:
        soft_delete_cache_item(ENTITY_ORDER_STATUS, order_header_key)
        logger.info(f"DynamoDB soft-deleted | PK={ENTITY_ORDER_STATUS} SK={order_header_key}")


# =============================================================================
# OpenSearch — partial update on orders and order-lines indexes
# =============================================================================

def update_order_in_index(
    order_header_key: str,
    status_name: str,
    status_code: Optional[str] = None,
) -> None:
    """Update statusName and statusCode on the orders index document."""
    client = get_opensearch_client()
    doc: dict = {
        "statusName": status_name,
    }
    if status_code:
        doc["statusCode"] = status_code
    try:
        client.update(
            index=INDEX_ORDER,
            id=order_header_key,
            body={"doc": doc},
        )
        logger.info(
            f"OpenSearch orders updated | index={INDEX_ORDER} id={order_header_key} "
            f"statusName={status_name} statusCode={status_code}"
        )
    except Exception as exc:
        # A 404 means the order hasn't been ingested yet; the status is
        # already cached in DynamoDB and will be applied on next ingestion.
        logger.warning(
            f"OpenSearch orders update skipped | index={INDEX_ORDER} id={order_header_key} "
            f"error={exc}"
        )


def update_order_lines_in_index(enrich: dict) -> None:
    """Update statusName and statusCode on each order-line document in the order-lines index."""
    client = get_opensearch_client()
    orderlinekey = enrich.get("orderlinekey", [])
    updated = 0
    skipped = 0

    for entry in orderlinekey:
        order_line_key = entry.get("OrderLineKey")
        status_name = entry.get("Status")
        status_code = entry.get("StatusCode")

        if not order_line_key:
            logger.warning(f"Skipping enrich entry — missing OrderLineKey: {entry}")
            skipped += 1
            continue

        doc: dict = {}
        if status_name:
            doc["statusName"] = status_name
        if status_code:
            doc["statusCode"] = status_code

        if not doc:
            logger.warning(f"Skipping enrich entry — no Status or StatusCode: {entry}")
            skipped += 1
            continue

        try:
            client.update(
                index=INDEX_ORDER_LINES,
                id=order_line_key,
                body={"doc": doc},
            )
            updated += 1
            logger.info(
                f"OpenSearch order-line updated | index={INDEX_ORDER_LINES} "
                f"id={order_line_key} statusName={status_name} statusCode={status_code}"
            )
        except Exception as exc:
            skipped += 1
            # A 404 means the order line hasn't been ingested yet; the status is
            # already cached in DynamoDB and will be applied on next ingestion.
            logger.warning(
                f"OpenSearch order-line update skipped | index={INDEX_ORDER_LINES} "
                f"id={order_line_key} error={exc}"
            )

    logger.info(
        f"Order-lines status update complete | "
        f"total={len(orderlinekey)} updated={updated} skipped={skipped}"
    )


def clear_order_status_in_index(order_header_key: str, enrich: Optional[dict] = None) -> None:
    """Clear statusName and statusCode from the orders index and order-lines index."""
    client = get_opensearch_client()

    # Clear from orders index
    try:
        client.update(
            index=INDEX_ORDER,
            id=order_header_key,
            body={"doc": {"statusCode": None, "statusName": None}},
        )
        logger.info(f"OpenSearch status cleared | index={INDEX_ORDER} id={order_header_key}")
    except Exception as exc:
        logger.warning(
            f"OpenSearch clear skipped | index={INDEX_ORDER} id={order_header_key} error={exc}"
        )

    # Clear from order-lines index
    if enrich and isinstance(enrich, dict):
        for entry in enrich.get("orderlinekey", []):
            order_line_key = entry.get("OrderLineKey")
            if not order_line_key:
                continue
            try:
                client.update(
                    index=INDEX_ORDER_LINES,
                    id=order_line_key,
                    body={"doc": {"statusName": None, "statusCode": None}},
                )
                logger.info(
                    f"OpenSearch order-line status cleared | "
                    f"index={INDEX_ORDER_LINES} id={order_line_key}"
                )
            except Exception as exc:
                logger.warning(
                    f"OpenSearch order-line clear skipped | "
                    f"index={INDEX_ORDER_LINES} id={order_line_key} error={exc}"
                )


# =============================================================================
# Lambda Handler
# =============================================================================

def lambda_handler(event: dict, context: Any) -> dict:
    try:
        preview = json.dumps(event, default=str)
        logger.info(f"Incoming event ({len(preview)} chars): {preview[:2000]}")

        body = parse_body(event)
        if body is None:
            return {"statusCode": 400, "body": "Request body is missing or could not be parsed"}

        operation = (body.get("operation") or "u").strip()

        payload = body.get("payload")
        if not isinstance(payload, dict) or not payload:
            return {"statusCode": 400, "body": "Request must include a non-empty 'payload' object"}

        order_header_key = (payload.get("orderHeaderKey") or "").strip()
        if not order_header_key:
            return {"statusCode": 400, "body": "Missing required field: orderHeaderKey"}

        if operation == "d":
            delete_order_status(order_header_key)
            enrich = payload.get("enrich")
            clear_order_status_in_index(order_header_key, enrich)
            return {
                "statusCode": 200,
                "body": json.dumps({"orderHeaderKey": order_header_key, "deleted": True}),
            }

        # create / update
        status_name = (payload.get("statusName") or "").strip()
        status_code = (payload.get("statusCode") or "").strip() or None  # optional
        enrich = payload.get("enrich")

        if not status_name:
            return {"statusCode": 400, "body": "Missing required field: statusName"}
            
        if enrich is None:
            enrich = {}
        elif not isinstance(enrich, dict):
            return {"statusCode": 400, "body": "Invalid 'enrich' object in payload"}

        orderlinekey = enrich.get("orderlinekey")
        if orderlinekey is not None:
            if not isinstance(orderlinekey, list):
                return {"statusCode": 400, "body": "'enrich.orderlinekey' must be a list if provided"}
    
            for idx, entry in enumerate(orderlinekey):
                if not isinstance(entry, dict):
                    return {"statusCode": 400, "body": f"enrich.orderlinekey[{idx}] must be an object"}
                pass

        save_order_status(order_header_key, status_name, enrich, status_code)
        update_order_in_index(order_header_key, status_name, status_code)
        update_order_lines_in_index(enrich)

        response_body = {
            "orderHeaderKey": order_header_key,
            "statusName":     status_name,
            "enrich":         enrich,
        }
        if status_code:
            response_body["statusCode"] = status_code

        return {
            "statusCode": 200,
            "body": json.dumps(response_body),
        }

    except Exception as exc:
        logger.error(f"Unhandled error in lambda_handler: {exc}", exc_info=True)
        return {"statusCode": 500, "body": str(exc)}