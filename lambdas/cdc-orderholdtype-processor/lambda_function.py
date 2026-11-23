"""
CDC Order Hold Type Processor Lambda

Consumes Debezium CDC events from MSK for the YFS_ORDER_HOLD_TYPE table.
Each CDC event carries a single row with ORDER_HEADER_KEY, ORDER_LINE_KEY,
HOLD_TYPE, and STATUS.

Behavior:
  1. Saves hold type statuses to DynamoDB enrichment cache so that
     orderheader-processor and orderline-processor can enrich orders/order-lines
     that arrive later.
     - PK=ORDER_HOLD_TYPE,       SK=ORDER_HEADER_KEY  → holdTypeStatuses (Map)
     - PK=ORDER_LINE_HOLD_TYPE,  SK=ORDER_LINE_KEY     → holdTypeStatuses (Map)
     The Map stores {holdType: statusString} pairs, e.g.
     {"FRAUD_REVIEW": "1300", "CREDIT_HOLD": "1200"}.
  2. Performs a partial update on the matching documents in OpenSearch:
     - orders index (by ORDER_HEADER_KEY)
     - order-lines index (by ORDER_LINE_KEY, if present)
     Field updated:
       - holdTypeStatuses — object (flat_object) mapping each hold type to
                            its status string.  e.g. {"FRAUD_REVIEW": "1300"}.
                            When STATUS is null, the value is stored as null.

Source table columns used:
    ORDER_HEADER_KEY  — links hold type to order
    ORDER_LINE_KEY    — links hold type to order line (may be empty)
    HOLD_TYPE         — the hold type value
    STATUS            — hold type status (e.g. 1100, 1200, or 1300); null is stored as null

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
    INDEX_ORDER            OpenSearch orders index name (default: orders)
    INDEX_ORDER_LINE       OpenSearch order-lines index name (default: order-lines)
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
INDEX_ORDER          = os.environ.get("INDEX_ORDER", "orders")
INDEX_ORDER_LINE     = os.environ.get("INDEX_ORDER_LINE","order-lines")

ENTITY_ORDER_HOLD_TYPE      = "ORDER_HOLD_TYPE"
ENTITY_ORDER_LINE_HOLD_TYPE = "ORDER_LINE_HOLD_TYPE"


# =============================================================================
# Topic -> Table Mapping
# =============================================================================

TOPIC_TABLE_MAP = {
    "yfs_order_hold_type": "YFS_ORDER_HOLD_TYPE",
}


def extract_table_from_topic(topic: str) -> str:
    """
    Extract table name from Debezium topic like:
    sterling.public.yfs_order_hold_type
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
    v = payload.get(key)
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None


def get_int_value(payload: dict, key: str) -> Optional[int]:
    """Extract an integer value from the payload, returning None if missing or empty."""
    v = payload.get(key)
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    try:
        return int(s)
    except (ValueError, TypeError):
        return None


def now_ms() -> int:
    return int(datetime.now(UTC).timestamp() * 1000)


def _status_priority(status_str: str | None) -> int:
    """
    Return numeric priority for a status string.
    Priority order: 1300 > 1200 > 1100 > null.
    None/null has the lowest priority (-1).
    """
    if status_str is None:
        return -1
    try:
        return int(status_str)
    except (ValueError, TypeError):
        return -1


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
            return "ignore", None, {}
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
    if "YFS_ORDER_HOLD_TYPE" in table:
        return process_order_hold_type_event(operation, payload)
    else:
        logger.warning(f"Unknown table: {table}")
        return {"statusCode": 400, "body": f"Unknown table: {table}"}


# =============================================================================
# Order Hold Type Processing
# =============================================================================

def process_order_hold_type_event(operation: str, payload: dict) -> dict:
    order_header_key = get_value(payload, "ORDER_HEADER_KEY")
    if not order_header_key:
        return {"statusCode": 400, "body": "Missing ORDER_HEADER_KEY"}

    hold_type = get_value(payload, "HOLD_TYPE")
    if not hold_type and operation != "d":
        return {"statusCode": 400, "body": "Missing HOLD_TYPE"}

    order_line_key = get_value(payload, "ORDER_LINE_KEY")
    raw_status = get_int_value(payload, "STATUS")  # may be None
    # Convert status to string for storage; keep None as None
    status_str = str(raw_status) if raw_status is not None else None

    client = get_opensearch_client()

    if operation == "d":
        if hold_type:
            # Remove hold type entry from DynamoDB cache
            remove_hold_type_status_from_cache(ENTITY_ORDER_HOLD_TYPE, order_header_key, hold_type)
            if order_line_key:
                remove_hold_type_status_from_cache(ENTITY_ORDER_LINE_HOLD_TYPE, order_line_key, hold_type)

            # Get remaining map from cache and sync to OpenSearch
            remaining_order = get_hold_type_statuses_from_cache(ENTITY_ORDER_HOLD_TYPE, order_header_key)
            replace_hold_type_statuses_in_index(client, INDEX_ORDER, order_header_key, remaining_order)

            if order_line_key:
                remaining_line = get_hold_type_statuses_from_cache(ENTITY_ORDER_LINE_HOLD_TYPE, order_line_key)
                replace_hold_type_statuses_in_index(client, INDEX_ORDER_LINE, order_line_key, remaining_line)

        logger.info(
            f"Hold type deleted | orderHeaderKey={order_header_key} "
            f"orderLineKey={order_line_key} holdType={hold_type} status={status_str}"
        )
        return {"statusCode": 200, "body": f"Deleted hold type for order {order_header_key}"}

    # Create or Update — upsert hold type → status into DynamoDB cache first,
    # then read back the full map and replace entirely in OpenSearch.
    # This avoids the flat_object partial-update limitation where painless
    # HashMap.put() overwrites the entire object instead of merging keys.
    upsert_hold_type_status_in_cache(ENTITY_ORDER_HOLD_TYPE, order_header_key, hold_type, status_str)
    full_order_map = get_hold_type_statuses_from_cache(ENTITY_ORDER_HOLD_TYPE, order_header_key)
    replace_hold_type_statuses_in_index(client, INDEX_ORDER, order_header_key, full_order_map)

    if order_line_key:
        upsert_hold_type_status_in_cache(ENTITY_ORDER_LINE_HOLD_TYPE, order_line_key, hold_type, status_str)
        full_line_map = get_hold_type_statuses_from_cache(ENTITY_ORDER_LINE_HOLD_TYPE, order_line_key)
        replace_hold_type_statuses_in_index(client, INDEX_ORDER_LINE, order_line_key, full_line_map)

    logger.info(
        f"Hold type indexed | orderHeaderKey={order_header_key} "
        f"orderLineKey={order_line_key} holdType={hold_type} status={status_str} "
        f"fullMap={full_order_map}"
    )
    return {"statusCode": 200, "body": f"Indexed hold type {hold_type} (status={status_str}) for order {order_header_key}"}


# =============================================================================
# DynamoDB — cache hold type statuses as a Map {holdType: statusString}
# =============================================================================

def upsert_hold_type_status_in_cache(entity: str, key: str, hold_type: str, status: str | None) -> None:
    """
    Set a single hold type → status entry in the holdTypeStatuses Map in DynamoDB.
    Uses max-wins priority logic: only overwrites if the new status has higher or
    equal priority than the existing one (1300 > 1200 > 1100 > null).
    Creates the item and/or the map attribute if they don't exist.
    Status is stored as a string (e.g. "1300") or None/null.
    """
    # Read current status and apply max-wins logic
    current_map = get_hold_type_statuses_from_cache(entity, key)
    current_status = current_map.get(hold_type)
    if current_status is not None and _status_priority(status) < _status_priority(current_status):
        logger.info(
            f"DynamoDB upsert skipped — existing status has higher priority | "
            f"PK={entity} SK={key} holdType={hold_type} "
            f"existing={current_status} incoming={status}"
        )
        return

    try:
        cache_table.update_item(
            Key={"PK": entity, "SK": key},
            UpdateExpression="SET holdTypeStatuses.#ht = :st, updatedAt = :now",
            ExpressionAttributeNames={"#ht": hold_type},
            ExpressionAttributeValues={
                ":st":  status,
                ":now": now_ms(),
            },
        )
        logger.info(
            f"DynamoDB hold type status upserted | PK={entity} SK={key} "
            f"holdType={hold_type} status={status}"
        )
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ValidationException":
            # holdTypeStatuses attribute doesn't exist yet — initialize the map
            cache_table.update_item(
                Key={"PK": entity, "SK": key},
                UpdateExpression="SET holdTypeStatuses = :map, updatedAt = :now",
                ExpressionAttributeValues={
                    ":map": {hold_type: status},
                    ":now": now_ms(),
                },
            )
            logger.info(
                f"DynamoDB hold type status initialized | PK={entity} SK={key} "
                f"holdType={hold_type} status={status}"
            )
        else:
            logger.error(
                f"DynamoDB upsert hold type status failed | "
                f"PK={entity} SK={key} holdType={hold_type} status={status} error={exc}",
                exc_info=True,
            )
    except Exception as exc:
        logger.error(
            f"DynamoDB upsert hold type status failed | "
            f"PK={entity} SK={key} holdType={hold_type} status={status} error={exc}",
            exc_info=True,
        )


def remove_hold_type_status_from_cache(entity: str, key: str, hold_type: str) -> None:
    """
    Remove a single hold type key from the holdTypeStatuses Map in DynamoDB.
    Skips if item doesn't exist (prevents phantom deletes).
    """
    try:
        cache_table.update_item(
            Key={"PK": entity, "SK": key},
            UpdateExpression="REMOVE holdTypeStatuses.#ht SET updatedAt = :now",
            ConditionExpression="attribute_exists(PK)",
            ExpressionAttributeNames={"#ht": hold_type},
            ExpressionAttributeValues={
                ":now": now_ms(),
            },
        )
        logger.info(
            f"DynamoDB hold type status removed | PK={entity} SK={key} holdType={hold_type}"
        )
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
            logger.info(
                f"DynamoDB remove skipped — item not found | "
                f"PK={entity} SK={key} holdType={hold_type}"
            )
        else:
            raise
    except Exception as exc:
        logger.error(
            f"DynamoDB remove hold type status failed | "
            f"PK={entity} SK={key} holdType={hold_type} error={exc}",
            exc_info=True,
        )


def get_hold_type_statuses_from_cache(entity: str, key: str) -> dict:
    """
    Retrieve the holdTypeStatuses Map from DynamoDB for a given entity/key.
    Returns a dict of {holdType: statusString}, or an empty dict if not found.
    """
    try:
        response = cache_table.get_item(Key={"PK": entity, "SK": key})
        item = response.get("Item")
        if not item:
            return {}
        raw = item.get("holdTypeStatuses")
        if raw and isinstance(raw, dict):
            return dict(raw)
        return {}
    except Exception as exc:
        logger.error(
            f"DynamoDB get hold type statuses failed | PK={entity} SK={key} error={exc}",
            exc_info=True,
        )
        return {}


# =============================================================================
# OpenSearch — painless script operations for holdTypeStatuses map
# =============================================================================

# Painless script to upsert a single hold type → status entry in the map
# Uses max-wins priority logic: only overwrites if new status ≥ existing
# Priority: 1300 > 1200 > 1100 > null (-1)
UPSERT_HOLD_TYPE_STATUS_SCRIPT = """
if (ctx._source.holdTypeStatuses == null) {
    ctx._source.holdTypeStatuses = new HashMap();
}
def existing = ctx._source.holdTypeStatuses.get(params.holdType);
int existingPriority = (existing != null) ? Integer.parseInt(existing.toString()) : -1;
int newPriority = (params.status != null) ? Integer.parseInt(params.status.toString()) : -1;
if (newPriority >= existingPriority) {
    ctx._source.holdTypeStatuses.put(params.holdType, params.status);
}
"""

# Painless script to remove a hold type key from the map
REMOVE_HOLD_TYPE_STATUS_SCRIPT = """
if (ctx._source.holdTypeStatuses != null) {
    ctx._source.holdTypeStatuses.remove(params.holdType);
    if (ctx._source.holdTypeStatuses.isEmpty()) {
        ctx._source.remove('holdTypeStatuses');
    }
}
"""

# Painless script to replace the entire holdTypeStatuses map
# (used after a delete to sync remaining state from DynamoDB)
REPLACE_HOLD_TYPE_STATUSES_SCRIPT = """
if (params.statuses != null && params.statuses.size() > 0) {
    ctx._source.holdTypeStatuses = params.statuses;
} else {
    ctx._source.remove('holdTypeStatuses');
}
"""


def upsert_hold_type_status_in_index(
    client: OpenSearch,
    index: str,
    doc_id: str,
    hold_type: str,
    status: str | None,
) -> None:
    """Upsert a single hold type → status entry in the holdTypeStatuses map on the document."""
    try:
        client.update(
            index=index,
            id=doc_id,
            body={
                "script": {
                    "source": UPSERT_HOLD_TYPE_STATUS_SCRIPT,
                    "lang": "painless",
                    "params": {"holdType": hold_type, "status": status},
                },
            },
        )
        logger.info(
            f"OpenSearch hold type status upserted | index={index} id={doc_id} "
            f"holdType={hold_type} status={status}"
        )
    except Exception as exc:
        # 404 means the order/order-line hasn't been ingested yet.
        # Hold type is already cached in DynamoDB and will be enriched
        # when the parent document arrives.
        logger.warning(
            f"OpenSearch upsert skipped | index={index} id={doc_id} "
            f"holdType={hold_type} status={status} error={exc}"
        )


def remove_hold_type_status_from_index(
    client: OpenSearch,
    index: str,
    doc_id: str,
    hold_type: str,
) -> None:
    """Remove a hold type key from the holdTypeStatuses map on the document."""
    try:
        client.update(
            index=index,
            id=doc_id,
            body={
                "script": {
                    "source": REMOVE_HOLD_TYPE_STATUS_SCRIPT,
                    "lang": "painless",
                    "params": {"holdType": hold_type},
                },
            },
        )
        logger.info(
            f"OpenSearch hold type status removed | index={index} id={doc_id} holdType={hold_type}"
        )
    except Exception as exc:
        logger.warning(
            f"OpenSearch remove skipped | index={index} id={doc_id} "
            f"holdType={hold_type} error={exc}"
        )


def replace_hold_type_statuses_in_index(
    client: OpenSearch,
    index: str,
    doc_id: str,
    statuses: dict,
) -> None:
    """
    Replace the entire holdTypeStatuses map on the document.
    If statuses is empty, removes the field entirely.
    Used after a delete to sync remaining state from DynamoDB.
    """
    try:
        client.update(
            index=index,
            id=doc_id,
            body={
                "script": {
                    "source": REPLACE_HOLD_TYPE_STATUSES_SCRIPT,
                    "lang": "painless",
                    "params": {"statuses": statuses},
                },
            },
        )
        logger.info(
            f"OpenSearch hold type statuses replaced | index={index} id={doc_id} "
            f"statuses={statuses}"
        )
    except Exception as exc:
        logger.warning(
            f"OpenSearch replace skipped | index={index} id={doc_id} "
            f"statuses={statuses} error={exc}"
        )