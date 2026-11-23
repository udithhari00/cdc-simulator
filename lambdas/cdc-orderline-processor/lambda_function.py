"""
Order Lines CDC Lambda Handler - Processes CDC events for the YFS_ORDER_LINE table.

Architecture change:
  - Order lines are stored in their OWN index (INDEX_ORDER_LINES = "order-lines").
  - Each document in that index represents ONE order line (not nested inside an order).
  - This gives full-text search capability over every field on every line.
  - The order header index is NOT touched by this Lambda at all.

Document structure in "order-lines" index:
    {
        "orderLineKey":       "...",       <- document _id
        "orderHeaderKey":     "...",
        "primeLineNo":        1,
        "subLineNo":          0,
        "lineType":           "PRODUCT",
        "itemId":             "...",
        ...all other columns...
        "order": {                          <- enriched from orders index
            "documentType":   "...",
            "draftOrderFlag": "...",
            "customerId":     "...",       <- same as billToId in orders index
            "orderNo":        "...",
            "orderDate":      1234567890123
        },
        "searchableText":     "...",
        "cdcOperation":       "u",          <- 'c', 'u', 'd', or 'r'
        "cdcTimestamp":       1234567890123, <- ts_ms from original Debezium event
        "indexedAt":          1234567890123
    }

Tables handled: YFS_ORDER_LINE
"""

import base64
import json
import logging
import os

from datetime import datetime ,UTC
from typing import Any, Optional

import boto3
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
OPENSEARCH_HOST     = os.environ.get('OPENSEARCH_HOST', 'opensearch')
OPENSEARCH_PORT     = int(os.environ.get('OPENSEARCH_PORT', '443'))
OPENSEARCH_SCHEME   = os.environ.get('OPENSEARCH_SCHEME', 'https')
OPENSEARCH_AUTH     = os.environ.get('OPENSEARCH_AUTH', 'iam')  # 'iam' or 'basic'
OPENSEARCH_USERNAME = os.environ.get('OPENSEARCH_USERNAME', 'admin')
OPENSEARCH_PASSWORD = os.environ.get('OPENSEARCH_PASSWORD', 'admin')
OPENSEARCH_MODE     = os.environ.get('OPENSEARCH_MODE', 'serverless')  # 'serverless' or 'provisioned'
AWS_REGION          = os.environ.get('AWS_REGION', 'us-east-1')
INDEX_ORDER_LINES   = os.environ.get('INDEX_ORDER_LINES', 'order-lines')
ENABLE_HARD_DELETE  = os.environ.get('ENABLE_HARD_DELETE', 'false').lower() == 'true'
DYNAMODB_TABLE      = os.environ.get('DYNAMODB_TABLE', 'enrichment_cache')
DYNAMODB_ENDPOINT   = os.environ.get('DYNAMODB_ENDPOINT')

ENTITY_ORDER_LINE_HOLD_TYPE = "ORDER_LINE_HOLD_TYPE"
ENTITY_ORDER_STATUS         = "ORDER_STATUS"
ENTITY_TRACKING_NO          = "TRACKING_NO"


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
# OpenSearch Client
# =============================================================================
opensearch_client = None

def get_opensearch_client():
    global opensearch_client

    if opensearch_client:
        return opensearch_client
    if OPENSEARCH_AUTH == 'iam':
        credentials = boto3.Session().get_credentials()
        if not credentials:
            raise RuntimeError("AWS credentials not found")
        iam_service = 'es' if OPENSEARCH_MODE == 'provisioned' else 'aoss'
        auth = AWSV4SignerAuth(credentials, AWS_REGION, iam_service)

        opensearch_client = OpenSearch(
            hosts=[{'host': OPENSEARCH_HOST, 'port': OPENSEARCH_PORT}],
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
            hosts=[{'host': OPENSEARCH_HOST, 'port': OPENSEARCH_PORT}],
            http_auth=(OPENSEARCH_USERNAME, OPENSEARCH_PASSWORD),
            use_ssl=(OPENSEARCH_SCHEME == 'https'),
            verify_certs=False,
            ssl_show_warn=False,
            connection_class=RequestsHttpConnection,
            timeout=30,
            retry_on_timeout=True,
            max_retries=3
        )

    return opensearch_client


# Topic name to table name mapping
TOPIC_TABLE_MAP = {
    'yfs_order_line': 'YFS_ORDER_LINE',
}


def extract_table_from_topic(topic: str) -> str:
    """Extract table name from Debezium topic like 'sterling.public.yfs_order_line'."""
    table_part = topic.rsplit('.', 1)[-1].lower()
    return TOPIC_TABLE_MAP.get(table_part, table_part.upper())


# =============================================================================
# Entry Point
# =============================================================================

def lambda_handler(event: dict, context: Any) -> dict:
    """
    Main Lambda handler — supports both MSK trigger and direct invocation.
    """
    try:
        logger.info(f"Incoming event: {json.dumps(event, default=str)[:5000]}")

        if event.get("eventSource") == "aws:kafka" or "records" in event:
            return process_msk_event(event)

        return process_single_event(
            table=event.get("table", "").upper(),
            operation=event.get("operation", "c"),
            payload=event.get("payload", {}),  
            source_timestamp=event.get("sourceTimestamp"),
        )

    except Exception as e:
        logger.error(f"Unhandled error in lambda_handler: {e}", exc_info=True)
        return {"statusCode": 500, "body": str(e)}


# =============================================================================
# MSK Batch Processor
# =============================================================================

def _parse_debezium_message(debezium_event: dict) -> tuple:
    """
    Parse a Debezium message into (operation, source_timestamp_ms, payload).

    Handles two formats:
    1. Flattened SMT format (schema/payload wrapper with __op/__source_ts_ms)
       { "schema": {...}, "payload": { "COL": val, "__op": "u", "__source_ts_ms": ... } }
    2. Standard Debezium envelope
       { "op": "c", "ts_ms": ..., "before": null, "after": { "col": val } }
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


def process_msk_event(event: dict) -> dict:
    records   = event.get("records", {})
    processed = 0
    errors    = 0

    for topic_partition, messages in records.items():
        logger.info(
            f"Partition start | "
            f"topic_partition={topic_partition} "
            f"message_count={len(messages)}"
        )

        for msg in messages:
            # Capture identifying fields BEFORE decode so they're available in error logs
            topic  = msg.get("topic", topic_partition.rsplit('-', 1)[0])
            offset = msg.get("offset", "unknown")

            try:
                table = extract_table_from_topic(topic)
                
                if msg.get("value") is None:
                    logger.info(f"Tombstone record skipped | topic={topic} offset={offset}")
                    continue

                raw_value      = base64.b64decode(msg["value"]).decode("utf-8")
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

                # Normalise column names to UPPERCASE
                payload = {k.upper(): v.strip() if isinstance(v, str) else v for k, v in payload.items()}

                logger.info(
                    f"Record decoded | "
                    f"table={table} topic={topic} offset={offset} "
                    f"operation={operation} "
                    f"source_timestamp={source_timestamp} "
                    f"payload_keys={list(payload.keys())}"
                )
                logger.info(
                    f"Payload sample | "
                    f"topic={topic} offset={offset} "
                    f"data={json.dumps(payload, default=str)[:2000]}"
                )

                result = process_single_event(table, operation, payload, source_timestamp)
                logger.info(f"Result for {table}: {result}")
                if result.get("statusCode", 0) != 200:
                    logger.error(f"Error processing record from {topic_partition}: {result['body']}")
                    errors += 1
                else:
                    processed += 1

            except Exception as e:
                errors += 1
                logger.error(
                    f"FAILED to process record | "
                    f"topic={topic} offset={offset} "
                    f"topic_partition={topic_partition} "
                    f"error_type={type(e).__name__} error={e}",
                    exc_info=True,
                )

    logger.info(f"Batch complete | processed={processed} errors={errors}")
    return {"statusCode": 200 if errors == 0 else 207, "body": f"Processed {processed}, errors {errors}"}


def process_single_event(table: str, operation: str, payload: dict, source_timestamp: Optional[int]) -> dict:
    logger.info(f"Processing {operation} event for table {table}")
    if 'YFS_ORDER_LINE' in table:
        return process_order_line_event(operation, payload, source_timestamp)
    else:
        logger.warning(f"Unknown table: {table}")
        return {'statusCode': 400, 'body': f'Unknown table: {table}'}


# =============================================================================
# Core Event Processor
# =============================================================================

def process_order_line_event(
    operation: str,
    payload: dict,
    source_timestamp: Optional[int],
) -> dict:
    """
    Process a single YFS_ORDER_LINE CDC event.

    Validates that required primary keys exist, then either deletes or upserts
    the line document in the dedicated order-lines index.
    """
    order_line_key   = get_value(payload, "ORDER_LINE_KEY")
    order_header_key = get_value(payload, "ORDER_HEADER_KEY")

    if not order_line_key:
        logger.warning(f"Skipping record — missing ORDER_LINE_KEY | operation={operation}")
        return {"statusCode": 400, "body": "Missing ORDER_LINE_KEY"}

    logger.info(
        f"Processing | "
        f"operation={operation} "
        f"order_line_key={order_line_key} "
        f"order_header_key={order_header_key}"
    )

    if operation == "d":
        return delete_order_line(order_line_key, order_header_key)

    # Transform payload into line document (using ref schemas)
    line_document = transform_order_line(payload, operation, source_timestamp)

    # Identify searchable string fields
    searchable_parts = [
        line_document.get('itemId'),
        line_document.get('itemDescription'),
        line_document.get('itemShortDescription'),
        line_document.get('customerItem'),
        line_document.get('customerPoNo'),
        line_document.get('manufacturerName'),
        line_document.get('manufacturerItem'),
        line_document.get('productClass'),
        line_document.get('productLine'),
        line_document.get('upcCode'),
        line_document.get('supplierItem'),
        line_document.get('serialNo'),
        line_document.get('extnPlannerName'),
    ]
    # Join non-empty string components to create full text search field
    line_document['searchableText'] = ' '.join(filter(None, searchable_parts))

    # Clean out Nones
    line_document = {k: v for k, v in line_document.items() if v is not None}
    
    # Explicitly clear soft-delete fields so a subsequent create/update "undeletes" the doc
    line_document['_deleted'] = False
    line_document['_deletedAt'] = None

    logger.info(
        f"Document ready | "
        f"order_line_key={order_line_key} "
        f"order_header_key={order_header_key} "
        f"field_count={len(line_document)} "
    )

    # Enrich with hold types from DynamoDB cache
    enrich_order_line_hold_types(line_document)

    # Enrich with order header details from orders index
    enrich_order_header_details(line_document)

    # Enrich with order line status from DynamoDB cache
    enrich_order_line_status(line_document)

    # Enrich with tracking info (trackingNumbers, scac, carrierServiceCode) from TRACKING_NO cache
    enrich_tracking_info(line_document)

    return upsert_order_line(order_line_key, line_document)


def transform_order_line(payload: dict, operation: str, source_timestamp: Optional[int]) -> dict:
    """Transform CDC payload to order line OpenSearch document using reference mappings."""
    return {
        # Identifiers
        'orderLineKey': get_value(payload, 'ORDER_LINE_KEY'),
        'orderHeaderKey': get_value(payload, 'ORDER_HEADER_KEY'),
        'primeLineNo': get_int_value(payload, 'PRIME_LINE_NO'),
        'subLineNo': get_int_value(payload, 'SUB_LINE_NO'),
        'lineType': get_value(payload, 'LINE_TYPE'),
        'orderClass': get_value(payload, 'ORDER_CLASS'),
        'lineSeqNo': get_value(payload, 'LINE_SEQ_NO'),

        # Item
        'itemId': get_value(payload, 'ITEM_ID'),
        'alternateItemId': get_value(payload, 'ALTERNATE_ITEM_ID'),
        'uom': get_value(payload, 'UOM'),
        'productClass': get_value(payload, 'PRODUCT_CLASS'),
        'productLine': get_value(payload, 'PRODUCT_LINE'),
        'itemDescription': get_value(payload, 'ITEM_DESCRIPTION'),
        'itemShortDescription': get_value(payload, 'ITEM_SHORT_DESCRIPTION'),
        'customerItem': get_value(payload, 'CUSTOMER_ITEM'),
        'customerItemDescription': get_value(payload, 'CUSTOMER_ITEM_DESCRIPTION'),
        'supplierItem': get_value(payload, 'SUPPLIER_ITEM'),
        'supplierItemDescription': get_value(payload, 'SUPPLIER_ITEM_DESCRIPTION'),
        'supplierCode': get_value(payload, 'SUPPLIER_CODE'),
        'upcCode': get_value(payload, 'UPC_CODE'),
        'manufacturerName': get_value(payload, 'MANUFACTURER_NAME'),
        'manufacturerItem': get_value(payload, 'MANUFACTURER_ITEM'),
        'countryOfOrigin': get_value(payload, 'COUNTRY_OF_ORIGIN'),

        # Pricing
        'unitPrice': get_float_value(payload, 'UNIT_PRICE'),
        'listPrice': get_float_value(payload, 'LIST_PRICE'),
        'retailPrice': get_float_value(payload, 'RETAIL_PRICE'),
        'unitCost': get_float_value(payload, 'UNIT_COST'),
        'costCurrency': get_value(payload, 'COST_CURRENCY'),
        'discountPercentage': get_float_value(payload, 'DISCOUNT_PERCENTAGE'),
        'discountType': get_value(payload, 'DISCOUNT_TYPE'),
        'discountReference': get_value(payload, 'DISCOUNT_REFERENCE'),
        'otherCharges': get_float_value(payload, 'OTHER_CHARGES'),
        'lineTotal': get_float_value(payload, 'LINE_TOTAL'),
        'invoicedLineTotal': get_float_value(payload, 'INVOICED_LINE_TOTAL'),
        'invoicedExtendedPrice': get_float_value(payload, 'INVOICED_EXTENDED_PRICE'),
        'tax': get_float_value(payload, 'TAX'),

        # Quantities
        'orderedQty': get_float_value(payload, 'ORDERED_QTY'),
        'originalOrderedQty': get_float_value(payload, 'ORIGINAL_ORDERED_QTY'),
        'committedQuantity': get_float_value(payload, 'COMMITTED_QUANTITY'),
        'shippedQuantity': get_float_value(payload, 'SHIPPED_QUANTITY'),
        'invoicedQuantity': get_float_value(payload, 'INVOICED_QUANTITY'),
        'receivedQuantity': get_float_value(payload, 'RECEIVED_QUANTITY'),
        'settledQuantity': get_float_value(payload, 'SETTLED_QUANTITY'),
        'settledAmount': get_float_value(payload, 'SETTLED_AMOUNT'),
        'fillQuantity': get_float_value(payload, 'FILL_QUANTITY'),
        'splitQty': get_float_value(payload, 'SPLIT_QTY'),

        # Shipping
        'shipnodeKey': get_value(payload, 'SHIPNODE_KEY'),
        'shipToKey': get_value(payload, 'SHIP_TO_KEY'),
        'shipToId': get_value(payload, 'SHIP_TO_ID'),
        'scac': get_value(payload, 'SCAC'),
        'carrierServiceCode': get_value(payload, 'CARRIER_SERVICE_CODE'),
        'carrierAccountNo': get_value(payload, 'CARRIER_ACCOUNT_NO'),
        'deliveryMethod': get_value(payload, 'DELIVERY_METHOD'),
        'deliveryCode': get_value(payload, 'DELIVERY_CODE'),
        'fulfillmentType': get_value(payload, 'FULFILLMENT_TYPE'),

        # Dates
        'reqDeliveryDate': get_timestamp_value(payload, 'REQ_DELIVERY_DATE'),
        'reqCancelDate': get_timestamp_value(payload, 'REQ_CANCEL_DATE'),
        'reqShipDate': get_timestamp_value(payload, 'REQ_SHIP_DATE'),
        'earliestShipDate': get_timestamp_value(payload, 'EARLIEST_SHIP_DATE'),
        'earliestDeliveryDate': get_timestamp_value(payload, 'EARLIEST_DELIVERY_DATE'),
        'pricingDate': get_timestamp_value(payload, 'PRICING_DATE'),
        'promisedApptStartDate': get_timestamp_value(payload, 'PROMISED_APPT_START_DATE'),
        'promisedApptEndDate': get_timestamp_value(payload, 'PROMISED_APPT_END_DATE'),

        # Flags
        'holdFlag': get_value(payload, 'HOLD_FLAG'),
        'holdReasonCode': get_value(payload, 'HOLD_REASON_CODE'),
        'pickableFlag': get_value(payload, 'PICKABLE_FLAG'),
        'taxableFlag': get_value(payload, 'TAXABLE_FLAG'),
        'giftFlag': get_value(payload, 'GIFT_FLAG'),
        'invoiceComplete': get_value(payload, 'INVOICE_COMPLETE'),
        'isPriceLocked': get_value(payload, 'IS_PRICE_LOCKED'),
        'kitCode': get_value(payload, 'KIT_CODE'),

        # Customer PO
        'customerPoNo': get_value(payload, 'CUSTOMER_PO_NO'),
        'customerPoLineNo': get_value(payload, 'CUSTOMER_PO_LINE_NO'),

        # Classification
        'departmentCode': get_value(payload, 'DEPARTMENT_CODE'),
        'segment': get_value(payload, 'SEGMENT'),
        'segmentType': get_value(payload, 'SEGMENT_TYPE'),

        # Weight
        'itemWeight': get_float_value(payload, 'ITEM_WEIGHT'),
        'itemWeightUom': get_value(payload, 'ITEM_WEIGHT_UOM'),

        # Return / Sourcing
        'returnReason': get_value(payload, 'RETURN_REASON'),
        'sourceFromOrganization': get_value(payload, 'SOURCE_FROM_ORGANIZATION'),
        'receivingNode': get_value(payload, 'RECEIVING_NODE'),

        # Lineage
        'chainedFromOrderLineKey': get_value(payload, 'CHAINED_FROM_ORDER_LINE_KEY'),
        'chainedFromOrderHeaderKey': get_value(payload, 'CHAINED_FROM_ORDER_HEADER_KEY'),
        'derivedFromOrderLineKey': get_value(payload, 'DERIVED_FROM_ORDER_LINE_KEY'),
        'derivedFromOrderHeaderKey': get_value(payload, 'DERIVED_FROM_ORDER_HEADER_KEY'),

        # Other
        'harmonizedCode': get_value(payload, 'HARMONIZED_CODE'),
        'serialNo': get_value(payload, 'SERIAL_NO'),
        'extnPlannerName': get_value(payload, 'EXTN_PLANNER_NAME'),
        'extnReqDropShip': get_value(payload, 'EXTN_REQ_DROP_SHIP'),

        # Audit
        'createdTimestamp': get_timestamp_value(payload, 'CREATETS'),
        'modifiedTimestamp': get_timestamp_value(payload, 'MODIFYTS'),
        'createUserId': get_value(payload, 'CREATEUSERID'),
        'modifyUserId': get_value(payload, 'MODIFYUSERID'),

        # CDC metadata
        'cdcOperation': operation,
        'cdcTimestamp': source_timestamp,
        'indexedAt': now_ms()
    }


# =============================================================================
# OpenSearch Operations
# =============================================================================

def upsert_order_line(order_line_key: str, line_document: dict) -> dict:
    """
    Upsert an order line as a flat top-level document in the order-lines index.

    doc_as_upsert behaviour:
      - Document does not exist → it is created with the provided fields.
      - Document already exists → only the provided fields are merged/updated
        (any existing fields not in line_document are left untouched).
    """
    try:
        client = get_opensearch_client()
        response = client.update(
            index=INDEX_ORDER_LINES,
            id=order_line_key,
            body={"doc": line_document, "doc_as_upsert": True},
            refresh=False,
        )
        logger.info(
            f"OpenSearch upsert OK | "
            f"index={INDEX_ORDER_LINES} "
            f"id={order_line_key} "
            f"result={response.get('result', 'unknown')}"
        )
        return {"statusCode": 200, "body": f"Upserted order line: {order_line_key}"}

    except Exception as e:
        logger.error(
            f"OpenSearch upsert FAILED | "
            f"index={INDEX_ORDER_LINES} "
            f"id={order_line_key} "
            f"error_type={type(e).__name__} error={e}",
            exc_info=True,
        )
        raise


def soft_delete_os_doc(client, index: str, doc_id: str) -> None:
    """Soft-delete an OpenSearch doc by setting _deleted=true instead of removing it."""
    try:
        client.update(
            index=index,
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
        logger.info(f"Soft-delete skipped — doc not found | index={index} id={doc_id}")


def delete_order_line(order_line_key: str, order_header_key: str) -> dict:
    """
    Delete an order line document from the order-lines index.
    """
    try:
        client = get_opensearch_client()
        if ENABLE_HARD_DELETE:
            client.delete(
                index=INDEX_ORDER_LINES,
                id=order_line_key,
                refresh=False,
                ignore=[404]
            )
            logger.info(
                f"OpenSearch hard-delete OK | "
                f"index={INDEX_ORDER_LINES} "
                f"id={order_line_key} "
                f"order_header_key={order_header_key}"
            )
        else:
            soft_delete_os_doc(client, INDEX_ORDER_LINES, order_line_key)
            logger.info(
                f"OpenSearch soft-delete OK | "
                f"index={INDEX_ORDER_LINES} "
                f"id={order_line_key} "
                f"order_header_key={order_header_key}"
            )
        return {"statusCode": 200, "body": f"Deleted order line: {order_line_key}"}

    except Exception as e:
        error_str = str(e)
        if "404" in error_str or "not_found" in error_str:
            logger.warning(
                f"Order line not found during delete — treating as success | "
                f"index={INDEX_ORDER_LINES} "
                f"id={order_line_key} "
                f"order_header_key={order_header_key}"
            )
            return {"statusCode": 200, "body": f"Order line already absent: {order_line_key}"}

        logger.error(
            f"OpenSearch delete FAILED | "
            f"index={INDEX_ORDER_LINES} "
            f"id={order_line_key} "
            f"order_header_key={order_header_key} "
            f"error_type={type(e).__name__} error={e}",
            exc_info=True,
        )
        raise


# =============================================================================
# DynamoDB Cache Helpers
# =============================================================================

def get_cached(entity: str, key: str) -> Optional[dict]:
    """Fetch a cached item from DynamoDB enrichment cache."""
    try:
        resp = cache_table.get_item(Key={"PK": entity, "SK": key})
        item = resp.get("Item")
        if item:
            return {k: v for k, v in item.items() if k not in ("PK", "SK", "updatedAt")}
        return None
    except Exception as e:
        logger.error(f"DynamoDB fetch failed | entity={entity} key={key} error={e}", exc_info=True)
        return None


def enrich_order_line_hold_types(line_document: dict) -> None:
    """Enrich order line with holdTypeStatuses map from DynamoDB cache."""
    order_line_key = line_document.get("orderLineKey")
    if not order_line_key:
        return
    cached = get_cached(ENTITY_ORDER_LINE_HOLD_TYPE, order_line_key)
    if not cached:
        return
    hold_type_statuses = cached.get("holdTypeStatuses")
    if hold_type_statuses and isinstance(hold_type_statuses, dict):
        line_document["holdTypeStatuses"] = dict(hold_type_statuses)


ENTITY_ORDER_HEADER = "ORDER_HEADER"


def enrich_order_header_details(line_document: dict) -> None:
    """
    Enrich order line with header-level details from DynamoDB enrichment cache.

    Looks up the cached order header using PK=ORDER_HEADER, SK=orderHeaderKey
    and adds a nested 'order' object with: documentType, draftOrderFlag,
    customerId, orderNo, orderDate.

    Note: customerId is the same as billToId in the orders index / YFS_ORDER_HEADER table.
    """
    order_header_key = line_document.get("orderHeaderKey")
    if not order_header_key:
        logger.warning("Cannot enrich order header details — missing orderHeaderKey")
        return

    cached = get_cached(ENTITY_ORDER_HEADER, order_header_key)
    if not cached:
        logger.info(
            f"Order header not found in cache — skipping enrichment | "
            f"order_header_key={order_header_key}"
        )
        return

    line_document["order"] = {
        "documentType":   cached.get("documentType"),
        "draftOrderFlag": cached.get("draftOrderFlag"),
        "customerId":     cached.get("billToId"),
        "orderNo":        cached.get("orderNo"),
        "orderDate":      cached.get("orderDate"),
    }
    logger.info(
        f"Order header enrichment OK | "
        f"order_header_key={order_header_key} "
        f"fields={list(line_document['order'].keys())}"
    )


def enrich_order_line_status(line_document: dict) -> None:
    """
    Enrich order line with statusName/statusCode from DynamoDB ORDER_STATUS cache.

    Looks up the cached order status using PK=ORDER_STATUS, SK=orderHeaderKey,
    then finds the matching entry in enrich.orderlinekey by OrderLineKey and
    sets statusName (from Status) and statusCode (from StatusCode) on the
    line document.
    """
    order_header_key = line_document.get("orderHeaderKey")
    order_line_key = line_document.get("orderLineKey")
    if not order_header_key or not order_line_key:
        return

    cached = get_cached(ENTITY_ORDER_STATUS, order_header_key)
    if not cached:
        logger.info(
            f"Order status not found in cache — skipping status enrichment | "
            f"order_header_key={order_header_key} order_line_key={order_line_key}"
        )
        return

    enrich = cached.get("enrich")
    if not enrich or not isinstance(enrich, dict):
        return

    orderlinekey = enrich.get("orderlinekey", [])
    if not isinstance(orderlinekey, list):
        return

    for entry in orderlinekey:
        if not isinstance(entry, dict):
            continue
        if entry.get("OrderLineKey") == order_line_key:
            status_name = entry.get("Status")
            status_code = entry.get("StatusCode")
            if status_name:
                line_document["statusName"] = status_name
            if status_code:
                line_document["statusCode"] = status_code
            logger.info(
                f"Order line status enrichment OK | "
                f"order_line_key={order_line_key} "
                f"statusName={status_name} statusCode={status_code}"
            )
            return

    logger.info(
        f"Order line key not found in cached status enrich — skipping | "
        f"order_header_key={order_header_key} order_line_key={order_line_key}"
    )


def enrich_tracking_info(line_document: dict) -> None:
    """
    Enrich an order line document with tracking data from the TRACKING_NO DynamoDB cache.

    Cache key: PK=TRACKING_NO, SK=<ORDER_HEADER_KEY>#<ORDER_LINE_KEY>
    Cache fields (parallel lists, same index = same container):
      - trackingNumbers   → list of tracking numbers
      - scac              → list of SCAC codes
      - carrierServiceCode→ list of carrier service codes

    These are written directly onto the line document as flat arrays.
    """
    order_header_key = line_document.get("orderHeaderKey")
    order_line_key   = line_document.get("orderLineKey")
    if not order_header_key or not order_line_key:
        return

    sk = f"{order_header_key}#{order_line_key}"
    cached = get_cached(ENTITY_TRACKING_NO, sk)
    if not cached:
        logger.info(
            f"Tracking info not found in cache — skipping | "
            f"order_header_key={order_header_key} order_line_key={order_line_key}"
        )
        return

    tracking_numbers      = cached.get("trackingNumbers") or []
    scac_list             = cached.get("scac") or []
    carrier_service_codes = cached.get("carrierServiceCode") or []

    tracking_info = []
    for trk, scac, csc in zip(tracking_numbers, scac_list, carrier_service_codes):
        if trk:
            entry = {"trackingNo": trk}
            if scac:
                entry["scac"] = scac
            if csc:
                entry["carrierServiceCode"] = csc
            tracking_info.append(entry)

    if tracking_info:
        # Sort for deterministic output
        tracking_info = sorted(tracking_info, key=lambda x: x["trackingNo"])
        line_document["trackingInfo"] = tracking_info
        logger.info(
            f"Tracking info enrichment OK | "
            f"order_line_key={order_line_key} "
            f"tracking_count={len(tracking_info)}"
        )


# =============================================================================
# Utility / Helper Functions
# =============================================================================

def get_value(payload: dict, key: str) -> Optional[str]:
    value = payload.get(key)
    if value is None:
        return None
    stripped = str(value).strip()
    return stripped if stripped else None


def get_int_value(payload: dict, key: str) -> Optional[int]:
    value = payload.get(key)
    if value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def get_float_value(payload: dict, key: str) -> Optional[float]:
    value = payload.get(key)
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def get_timestamp_value(payload: dict, key: str) -> Optional[int]:
    value = payload.get(key)
    if value is None:
        return None
    try:
        ts = int(value)
        if ts > 1_000_000_000_000_000:
            return ts // 1000
        if ts > 1_000_000_000_000:
            return ts
        return ts * 1000
    except (ValueError, TypeError):
        return None


def now_ms() -> int:
    return int(datetime.now(UTC).timestamp() * 1000)

