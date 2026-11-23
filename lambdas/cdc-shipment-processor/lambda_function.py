"""
CDC Shipment Processor Lambda

Consumes Debezium CDC events from MSK for the YFS_SHIPMENT table.
Each CDC event carries a single row with ORDER_HEADER_KEY, STATUS, and
other shipment-level columns.

Behavior:
  1. Caches shipment data to DynamoDB enrichment cache so that
     orderheader-processor can enrich orders that arrive later.
     - PK=YFS_SHIPMENT, SK=<ORDER_HEADER_KEY>  → shipmentStatus (string)
  2. Performs a partial update on the matching document in OpenSearch:
     - orders index (by ORDER_HEADER_KEY)
     Field updated:
       - shipmentStatus — the STATUS value from the YFS_SHIPMENT table.

Source table columns used:
    ORDER_HEADER_KEY  — links shipment to order (used as DynamoDB SK and OS doc id)
    STATUS            — shipment status value

Environment Variables:
    DYNAMODB_TABLE        DynamoDB table name (default: enrichment_cache)
    DYNAMODB_ENDPOINT     Optional local endpoint override
    OPENSEARCH_HOST       OpenSearch domain endpoint
    OPENSEARCH_PORT       (default: 443)
    OPENSEARCH_SCHEME     (default: https)
    OPENSEARCH_AUTH       'iam' or 'basic' (default: iam)
    OPENSEARCH_USERNAME   used when OPENSEARCH_AUTH=basic
    OPENSEARCH_PASSWORD   used when OPENSEARCH_AUTH=basic
    OPENSEARCH_MODE       'provisioned' or 'serverless' (default: serverless)
    AWS_REGION            (default: us-east-1)
    INDEX_ORDERS          OpenSearch orders index name (default: orders)
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

DYNAMODB_TABLE      = os.environ.get("DYNAMODB_TABLE", "enrichment_cache")
DYNAMODB_ENDPOINT   = os.environ.get("DYNAMODB_ENDPOINT")
OPENSEARCH_HOST     = os.environ.get("OPENSEARCH_HOST", "opensearch")
OPENSEARCH_PORT     = int(os.environ.get("OPENSEARCH_PORT", "443"))
OPENSEARCH_SCHEME   = os.environ.get("OPENSEARCH_SCHEME", "https")
OPENSEARCH_AUTH     = os.environ.get("OPENSEARCH_AUTH", "iam")
OPENSEARCH_USERNAME = os.environ.get("OPENSEARCH_USERNAME", "admin")
OPENSEARCH_PASSWORD = os.environ.get("OPENSEARCH_PASSWORD", "admin")
OPENSEARCH_MODE     = os.environ.get("OPENSEARCH_MODE", "serverless")
AWS_REGION          = os.environ.get("AWS_REGION", "us-east-1")
INDEX_ORDERS        = os.environ.get("INDEX_ORDERS", "orders")

ENTITY_SHIPMENT = "YFS_SHIPMENT"
ENABLE_HARD_DELETE = os.environ.get('ENABLE_HARD_DELETE', 'false').lower() == 'true'


# =============================================================================
# Topic -> Table Mapping
# =============================================================================

TOPIC_TABLE_MAP = {
    "yfs_shipment": "YFS_SHIPMENT",
}


def extract_table_from_topic(topic: str) -> str:
    """
    Extract table name from Debezium topic like:
    glenraven-dev.STERLING.yfs_shipment
    """
    table_part = topic.rsplit(".", 1)[-1].lower()
    return TOPIC_TABLE_MAP.get(table_part, table_part.upper())


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

def get_value(payload: dict, key: str) -> Optional[str]:
    """Get string value from payload, stripping trailing whitespace from Oracle CHAR fields."""
    v = payload.get(key)
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None


def now_ms() -> int:
    return int(datetime.now(UTC).timestamp() * 1000)


# =============================================================================
# Debezium Message Parser
# =============================================================================

def _parse_debezium_message(debezium_event: dict) -> tuple:
    """
    Parse a Debezium message into (operation, source_timestamp_ms, payload).

    Handles two formats:
    1. Flattened SMT format (schema/payload wrapper, columns + __op/__source_ts_ms in payload)
       - { "schema": {...}, "payload": { "COL": val, "__op": "r", "__source_ts_ms": ... } }
    2. Standard Debezium envelope (no schema wrapper)
       - { "op": "c", "ts_ms": ..., "before": null, "after": { "COL": val } }
    """
    if "schema" in debezium_event and "payload" in debezium_event:
        inner = debezium_event["payload"]
        if inner is None:
            return 'ignore', None, {}
        if "__op" in inner:
            operation        = inner.get("__op") or "c"
            source_timestamp = inner.get("__source_ts_ms")
            payload          = {k: v for k, v in inner.items() if not k.startswith("__")}
            return operation, source_timestamp, payload

    operation        = debezium_event.get("op", "c")
    source_timestamp = debezium_event.get("ts_ms")
    if operation == "d":
        payload = debezium_event.get("before", {}) or {}
    else:
        payload = debezium_event.get("after", {}) or {}
    return operation, source_timestamp, payload


# =============================================================================
# Lambda Handler
# =============================================================================

def lambda_handler(event: dict, context: Any) -> dict:
    try:
        logger.info(f"Incoming event sample: {json.dumps(event, default=str)[:5000]}")

        if event.get("eventSource") == "aws:kafka" or "records" in event:
            return process_msk_event(event)

        return process_single_event(
            table=event.get("table", "").upper(),
            operation=event.get("operation", "c"),
            payload=event.get("payload", {}),
        )

    except Exception as e:
        logger.error(f"Unhandled error in lambda_handler: {e}", exc_info=True)
        return {"statusCode": 500, "body": str(e)}


# =============================================================================
# MSK Batch Processor
# =============================================================================

def process_msk_event(event: dict) -> dict:
    records = event.get("records", {})
    processed = 0
    errors = 0

    for topic_partition, messages in records.items():
        logger.info(
            f"Partition start | topic_partition={topic_partition} message_count={len(messages)}"
        )

        for msg in messages:
            topic = msg.get("topic", topic_partition.rsplit("-", 1)[0])
            offset = msg.get("offset", "unknown")

            try:
                table = extract_table_from_topic(topic)

                if msg.get("value") is None:
                    logger.info(f"Tombstone record skipped | topic={topic} offset={offset}")
                    continue

                raw_value = base64.b64decode(msg["value"]).decode("utf-8")
                debezium_event = json.loads(raw_value)

                operation, source_timestamp, payload = _parse_debezium_message(debezium_event)

                if operation == "ignore":
                    if msg.get("key"):
                        raw_key = base64.b64decode(msg["key"]).decode("utf-8")
                        kafka_key = json.loads(raw_key)
                        key_payload = kafka_key.get("payload", {})
                        if key_payload:
                            operation = "d"
                            payload = key_payload
                            logger.info(f"Using Tombstone record for delete | topic={topic} offset={offset}")
                        else:
                            logger.info(f"Tombstone record (null payload) skipped - no key payload | topic={topic} offset={offset}")
                            continue
                    else:
                        logger.info(f"Tombstone record (null payload) skipped - no key | topic={topic} offset={offset}")
                        continue

                payload = {
                    k.upper(): v.strip() if isinstance(v, str) else v
                    for k, v in payload.items()
                }
                logger.info(
                    f"Record decoded | table={table} topic={topic} offset={offset} operation={operation}"
                )
                result = process_single_event(table, operation, payload)
                if result.get("statusCode", 0) != 200:
                    errors += 1
                else:
                    processed += 1

            except Exception as e:
                errors += 1
                logger.error(
                    f"FAILED record | topic={topic} offset={offset} error={e}",
                    exc_info=True,
                )

    logger.info(f"Batch complete | processed={processed} errors={errors}")
    return {"statusCode": 200 if errors == 0 else 207, "body": f"Processed {processed}, errors {errors}"}

# =============================================================================
# Event Router
# =============================================================================

def process_single_event(
        table: str,
        operation: str,
        payload: dict,
) -> dict:
    logger.info(f"Processing {operation} event for table {table}")
    if "YFS_SHIPMENT" in table:
        return process_shipment_event(operation, payload)
    else:
        logger.warning(f"Unknown table: {table}")
        return {"statusCode": 400, "body": f"Unknown table: {table}"}


# =============================================================================
# Shipment Processing
# =============================================================================

def process_shipment_event(operation: str, payload: dict) -> dict:
    """
    Process a YFS_SHIPMENT CDC event:
    1. Extract ORDER_HEADER_KEY, SHIPMENT_KEY, and STATUS from the payload
    2. Cache shipment status in DynamoDB (PK=YFS_SHIPMENT, SK=<ORDER_HEADER_KEY>) under shipmentStatuses map
    3. Update the orders OpenSearch index with shipmentStatus array
    """
    order_header_key = get_value(payload, "ORDER_HEADER_KEY")
    shipment_key = get_value(payload, "SHIPMENT_KEY")
    status = get_value(payload, "STATUS")

    if not order_header_key:
        logger.warning(f"Skipping shipment event - Missing ORDER_HEADER_KEY | payload={payload}")
        return {"statusCode": 200, "body": "Skipped - Missing ORDER_HEADER_KEY"}
    if not shipment_key:
        logger.warning(f"Skipping shipment event - Missing SHIPMENT_KEY | payload={payload}")
        return {"statusCode": 200, "body": "Skipped - Missing SHIPMENT_KEY"}

    logger.info(
        f"process_shipment_event | orderHeaderKey={order_header_key} shipmentKey={shipment_key} "
        f"status={status} operation={operation}"
    )

    client = get_opensearch_client()

    if operation == "d":
        # Delete: remove shipment status from cache and update OpenSearch with remaining
        statuses = remove_shipment_status_from_cache(order_header_key, shipment_key)
        update_shipment_status_in_index(client, order_header_key, statuses)
        logger.info(
            f"Shipment status deleted | orderHeaderKey={order_header_key} shipmentKey={shipment_key} remaining={statuses}"
        )
        return {"statusCode": 200, "body": f"Deleted shipment status for order {order_header_key}"}

    # Create or Update
    statuses = upsert_shipment_status_in_cache(order_header_key, shipment_key, status)
    update_shipment_status_in_index(client, order_header_key, statuses)

    logger.info(
        f"Shipment status indexed | orderHeaderKey={order_header_key} shipmentKey={shipment_key} statuses={statuses}"
    )
    return {"statusCode": 200, "body": f"Indexed shipment status for order {order_header_key}"}


# =============================================================================
# DynamoDB — cache shipment status
# =============================================================================

def upsert_shipment_status_in_cache(order_header_key: str, shipment_key: str, status: Optional[str]) -> list:
    """
    Upsert shipment status into the shipmentStatuses map in DynamoDB.
    Returns the list of unique non-null statuses for the order.
    """
    try:
        response = cache_table.update_item(
            Key={"PK": ENTITY_SHIPMENT, "SK": order_header_key},
            UpdateExpression="SET shipmentStatuses.#sk = :st, updatedAt = :now",
            ExpressionAttributeNames={"#sk": shipment_key},
            ExpressionAttributeValues={":st": status, ":now": now_ms()},
            ReturnValues="ALL_NEW"
        )
        statuses_map = response.get("Attributes", {}).get("shipmentStatuses", {})
        return sorted(list(set(s for s in statuses_map.values() if s is not None)))
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ValidationException":
            # The shipmentStatuses attribute doesn't exist yet, initialize it
            response = cache_table.update_item(
                Key={"PK": ENTITY_SHIPMENT, "SK": order_header_key},
                UpdateExpression="SET shipmentStatuses = :map, updatedAt = :now",
                ExpressionAttributeValues={":map": {shipment_key: status}, ":now": now_ms()},
                ReturnValues="ALL_NEW"
            )
            statuses_map = response.get("Attributes", {}).get("shipmentStatuses", {})
            return sorted(list(set(s for s in statuses_map.values() if s is not None)))
        logger.error(
            f"DynamoDB upsert failed | PK={ENTITY_SHIPMENT} SK={order_header_key} error={exc}",
            exc_info=True,
        )
        raise


def remove_shipment_status_from_cache(order_header_key: str, shipment_key: str) -> list:
    """
    Remove shipment status from the shipmentStatuses map in DynamoDB.
    Returns the list of remaining unique non-null statuses for the order.
    """
    try:
        response = cache_table.update_item(
            Key={"PK": ENTITY_SHIPMENT, "SK": order_header_key},
            UpdateExpression="REMOVE shipmentStatuses.#sk SET updatedAt = :now",
            ConditionExpression="attribute_exists(PK)",
            ExpressionAttributeNames={"#sk": shipment_key},
            ExpressionAttributeValues={":now": now_ms()},
            ReturnValues="ALL_NEW"
        )
        statuses_map = response.get("Attributes", {}).get("shipmentStatuses", {})
        return sorted(list(set(s for s in statuses_map.values() if s is not None)))
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
            logger.info(f"DynamoDB remove skipped — item not found | PK={ENTITY_SHIPMENT} SK={order_header_key}")
            return []
        logger.error(
            f"DynamoDB remove failed | PK={ENTITY_SHIPMENT} SK={order_header_key} error={exc}",
            exc_info=True,
        )
        raise


def soft_delete_cache_item(pk: str, sk: str) -> None:
    """Soft-delete a DynamoDB cache row by marking it as deleted instead of removing it.
    Uses a ConditionExpression to prevent creating phantom deleted records
    if the item doesn't already exist in DynamoDB."""
    try:
        cache_table.update_item(
            Key={"PK": pk, "SK": sk},
            UpdateExpression="SET #d = :true, #da = :now",
            ConditionExpression="attribute_exists(PK)",
            ExpressionAttributeNames={"#d": "_deleted", "#da": "_deletedAt"},
            ExpressionAttributeValues={":true": True, ":now": datetime.now(UTC).isoformat()},
        )
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
            logger.info(f"soft_delete_cache_item: Item not found, skipping phantom delete | pk={pk} sk={sk}")
        else:
            raise


# =============================================================================
# OpenSearch — update / clear shipmentStatus on the orders document
# =============================================================================

def update_shipment_status_in_index(
    client: OpenSearch,
    order_header_key: str,
    statuses: list,
) -> None:
    """Update the shipmentStatus field on the order document in OpenSearch with an array of statuses."""
    try:
        client.update(
            index=INDEX_ORDERS,
            id=order_header_key,
            body={"doc": {"shipmentStatus": statuses if statuses else None}},
        )
        logger.info(
            f"OpenSearch updated | index={INDEX_ORDERS} id={order_header_key} "
            f"shipmentStatuses={statuses}"
        )
    except Exception as exc:
        # A 404 means the order hasn't been ingested yet; the status is
        # already cached in DynamoDB and will be applied on next ingestion.
        logger.warning(
            f"OpenSearch update skipped | index={INDEX_ORDERS} id={order_header_key} "
            f"shipmentStatuses={statuses} error={exc}"
        )
