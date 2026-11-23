"""
CDC Order Invoice Processor Lambda

Consumes Debezium CDC events from MSK for the YFS_ORDER_INVOICE table.
Extracts INVOICE_NO and:
  1. Saves it to the DynamoDB enrichment cache under PK=ORDER_INVOICE,
     SK=ORDER_HEADER_KEY so that cdc-orderheader-processor can enrich
     orders that arrive later.
  2. Performs a partial update on the matching document in the OpenSearch
     orders index, keyed by ORDER_HEADER_KEY.

Source table columns used:
    ORDER_HEADER_KEY  — links invoice to order (used as DynamoDB SK and OS doc id)
    INVOICE_NO        — the invoice number to store

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
    INDEX_ORDER           OpenSearch index name (default: orders)
"""

import base64
import json
import logging
import os
from datetime import datetime, UTC
from typing import Any, Optional

import boto3
from botocore.exceptions import ClientError
from botocore.config import Config
from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth, helpers


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
OPENSEARCH_MODE     = os.environ.get("OPENSEARCH_MODE", "provisioned")
AWS_REGION          = os.environ.get("AWS_REGION", "us-east-1")
INDEX_ORDER         = os.environ.get("INDEX_ORDER", os.environ.get("INDEX_ORDERS", "orders"))

# Clean host for opensearch-py
if OPENSEARCH_HOST.startswith("https://"):
    OPENSEARCH_HOST = OPENSEARCH_HOST[len("https://"):]
elif OPENSEARCH_HOST.startswith("http://"):
    OPENSEARCH_HOST = OPENSEARCH_HOST[len("http://"):]

ENTITY_ORDER_INVOICE = "ORDER_INVOICE"
ENABLE_HARD_DELETE   = os.environ.get("ENABLE_HARD_DELETE", "false").lower() == "true"


# =============================================================================
# Topic -> Table Mapping
# =============================================================================

TOPIC_TABLE_MAP = {
    "yfs_order_invoice": "YFS_ORDER_INVOICE",
}


def extract_table_from_topic(topic: str) -> str:
    """
    Extract table name from Debezium topic like:
    sterling.public.yfs_order_invoice
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

import threading
thread_local = threading.local()

def get_opensearch_client() -> OpenSearch:
    if hasattr(thread_local, 'opensearch_client'):
        return thread_local.opensearch_client

    if OPENSEARCH_AUTH == "iam":
        credentials = boto3.Session().get_credentials()
        if not credentials:
            raise RuntimeError("AWS credentials not found")
        iam_service = "es" if OPENSEARCH_MODE == "provisioned" else "aoss"
        auth = AWSV4SignerAuth(credentials, AWS_REGION, iam_service)
        thread_local.opensearch_client = OpenSearch(
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
        thread_local.opensearch_client = OpenSearch(
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

    return thread_local.opensearch_client


# =============================================================================
# Helpers
# =============================================================================

def get_value(payload: dict, key: str) -> Optional[str]:
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
    if "YFS_ORDER_INVOICE" in table:
        return process_order_invoice_event(operation, payload)
    else:
        logger.warning(f"Unknown table: {table}")
        return {"statusCode": 400, "body": f"Unknown table: {table}"}


# =============================================================================
# Order Invoice Processing
# =============================================================================

def process_order_invoice_event(operation: str, payload: dict) -> dict:
    order_header_key = get_value(payload, "ORDER_HEADER_KEY")
    if not order_header_key:
        return {"statusCode": 400, "body": "Missing ORDER_HEADER_KEY"}

    invoice_no = get_value(payload, "INVOICE_NO")
    if not invoice_no and operation != "d":
        return {"statusCode": 400, "body": "Missing INVOICE_NO"}

    client = get_opensearch_client()

    if operation == "d":
        delete_invoice_cache(order_header_key)
        clear_invoice_in_index(client, order_header_key)
        logger.info(f"Invoice deleted | orderHeaderKey={order_header_key} invoiceNo={invoice_no}")
        return {"statusCode": 200, "body": f"Deleted invoice for order {order_header_key}"}

    # Create or Update
    save_invoice_cache(order_header_key, invoice_no)
    update_invoice_in_index(client, order_header_key, invoice_no)
    logger.info(f"Invoice indexed | orderHeaderKey={order_header_key} invoiceNo={invoice_no}")
    return {"statusCode": 200, "body": f"Indexed invoice {invoice_no} for order {order_header_key}"}

def bulk_process_order_invoice_events(events: list) -> dict:
    """
    Process an array of payload events in bulk.
    events format: [{"operation": "c", "payload": {...}}, ...]
    """
    client = get_opensearch_client()
    os_actions = []
    
    # 1. DynamoDB Bulk Write (Batch Writer handles 25-item chunking)
    # Note: Soft deletes require UpdateItem, which batch_writer doesn't support.
    # For a backfill script, deletes are rare, so we run soft-deletes sequentially.
    with cache_table.batch_writer() as batch:
        for ev in events:
            op = ev.get("operation", "c")
            payload = ev.get("payload", {})
            
            order_header_key = get_value(payload, "ORDER_HEADER_KEY")
            if not order_header_key:
                continue
            
            invoice_no = get_value(payload, "INVOICE_NO")
            
            if op == "d":
                if ENABLE_HARD_DELETE:
                    batch.delete_item(Key={"PK": ENTITY_ORDER_INVOICE, "SK": order_header_key})
                else:
                    delete_invoice_cache(order_header_key) # Sequential soft delete
                    
                os_actions.append({
                    "_op_type": "update",
                    "_index": INDEX_ORDER,
                    "_id": order_header_key,
                    "doc": {"invoiceNo": None}
                })
            else:
                if invoice_no:
                    batch.put_item(Item={
                        "PK": ENTITY_ORDER_INVOICE,
                        "SK": order_header_key,
                        "invoiceNo": invoice_no,
                        "updatedAt": now_ms(),
                    })
                    os_actions.append({
                        "_op_type": "update",
                        "_index": INDEX_ORDER,
                        "_id": order_header_key,
                        "doc": {"invoiceNo": invoice_no}
                    })

    # 2. OpenSearch Update (Simulated bulk via ThreadPool due to IAM _bulk restrictions)
    if os_actions:
        import concurrent.futures
        success = 0
        errors_list = []
        
        def os_update(action):
            try:
                if "script" in action:
                    client.update(index=action["_index"], id=action["_id"], body={"script": action["script"]})
                else:
                    client.update(index=action["_index"], id=action["_id"], body={"doc": action["doc"]})
                return True, None
            except Exception as exc:
                if getattr(exc, "status_code", None) == 404 or "404" in str(exc):
                    return True, None
                return False, exc

        with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
            futures = [executor.submit(os_update, a) for a in os_actions]
            for f in concurrent.futures.as_completed(futures):
                ok, err = f.result()
                if ok:
                    success += 1
                else:
                    errors_list.append(err)
        
        logger.info(f"OpenSearch batch update completed | success={success} errors={len(errors_list)}")
        if errors_list:
            logger.error(f"Sample OpenSearch error: {errors_list[0]}")
            
    return {"statusCode": 200, "body": f"Bulk processed {len(events)} events"}


# =============================================================================
# DynamoDB — cache invoice in enrichment_cache
# =============================================================================

def save_invoice_cache(order_header_key: str, invoice_no: str) -> None:
    """Save invoice to DynamoDB cache so orderheader-processor can enrich later."""
    cache_table.put_item(Item={
        "PK":        ENTITY_ORDER_INVOICE,
        "SK":        order_header_key,
        "invoiceNo": invoice_no,
        "updatedAt": now_ms(),
    })
    logger.info(
        f"DynamoDB saved | PK={ENTITY_ORDER_INVOICE} SK={order_header_key} invoiceNo={invoice_no}"
    )


def delete_invoice_cache(order_header_key: str) -> None:
    """Remove invoice from DynamoDB cache. Skips if item doesn't exist (prevents phantom deletes)."""
    if ENABLE_HARD_DELETE:
        cache_table.delete_item(Key={"PK": ENTITY_ORDER_INVOICE, "SK": order_header_key})
        logger.info(f"DynamoDB hard-deleted | PK={ENTITY_ORDER_INVOICE} SK={order_header_key}")
    else:
        try:
            cache_table.update_item(
                Key={"PK": ENTITY_ORDER_INVOICE, "SK": order_header_key},
                UpdateExpression="SET #d = :true, #da = :now",
                ConditionExpression="attribute_exists(PK)",
                ExpressionAttributeNames={"#d": "_deleted", "#da": "_deletedAt"},
                ExpressionAttributeValues={":true": True, ":now": datetime.now(UTC).isoformat()},
            )
            logger.info(f"DynamoDB soft-deleted | PK={ENTITY_ORDER_INVOICE} SK={order_header_key}")
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
                logger.info(
                    f"DynamoDB soft-delete skipped — item not found | "
                    f"PK={ENTITY_ORDER_INVOICE} SK={order_header_key}"
                )
            else:
                raise


# =============================================================================
# OpenSearch — partial update invoiceNo on the orders document
# =============================================================================

def update_invoice_in_index(client: OpenSearch, order_header_key: str, invoice_no: str) -> None:
    """Set invoiceNo on the orders document via partial update."""
    try:
        client.update(
            index=INDEX_ORDER,
            id=order_header_key,
            body={"doc": {"invoiceNo": invoice_no}},
        )
        logger.info(
            f"OpenSearch updated | index={INDEX_ORDER} id={order_header_key} invoiceNo={invoice_no}"
        )
    except Exception as exc:
        # 404 means the order hasn't been ingested yet; invoice is already
        # cached in DynamoDB and will be enriched when the order arrives.
        logger.warning(
            f"OpenSearch update skipped | index={INDEX_ORDER} id={order_header_key} error={exc}"
        )


def clear_invoice_in_index(client: OpenSearch, order_header_key: str) -> None:
    """Clear invoiceNo from the orders document."""
    try:
        client.update(
            index=INDEX_ORDER,
            id=order_header_key,
            body={"doc": {"invoiceNo": None}},
        )
        logger.info(f"OpenSearch invoice cleared | index={INDEX_ORDER} id={order_header_key}")
    except Exception as exc:
        logger.warning(
            f"OpenSearch clear skipped | index={INDEX_ORDER} id={order_header_key} error={exc}"
        )
