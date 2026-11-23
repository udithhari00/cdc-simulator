"""
CDC Lambda Handler (Shipment Container) — Processes YFS_SHIPMENT_CONTAINER CDC events from Kafka.

This Lambda:
1. Receives YFS_SHIPMENT_CONTAINER CDC events via MSK
2. Caches container data to DynamoDB (PK=YFS_SHIPMENT_CONTAINER, SK=<SHIPMENT_CONTAINER_KEY>)
3. Writes reverse lookup rows:
   - PK=YFS_SHIPMENT_LINE_BY_SHIPMENT, SK=<SHIPMENT_KEY>#<SHIPMENT_LINE_KEY>
   - PK=YFS_SHIPMENT_LINE_BY_ORDER_LINE, SK=<ORDER_LINE_KEY>#<SHIPMENT_LINE_KEY>
4. Re-derives the TRACKING_NO cache for the affected order line

DynamoDB Single Table Design:
- Table: enrichment_cache
- PK (Partition Key): entity_type
- SK (Sort Key): entity_id

Access Patterns:
- Cache shipment line:          PK=YFS_SHIPMENT_LINE, SK=<SHIPMENT_LINE_KEY>
- Reverse by shipment:          PK=YFS_SHIPMENT_LINE_BY_SHIPMENT, SK=<SHIPMENT_KEY>#<SHIPMENT_LINE_KEY>
- Reverse by order line:        PK=YFS_SHIPMENT_LINE_BY_ORDER_LINE, SK=<ORDER_LINE_KEY>#<SHIPMENT_LINE_KEY>
- Read shipment:                PK=YFS_SHIPMENT, SK=<SHIPMENT_KEY>
- Find containers by shipment:  PK=YFS_SHIPMENT_CONTAINER_BY_SHIPMENT, SK begins_with <SHIPMENT_KEY>#
- Write derived tracking:       PK=TRACKING_NO, SK=<ORDER_HEADER_KEY>#<ORDER_LINE_KEY>
"""

import base64
import json
import logging
import os

from datetime import datetime, UTC
from typing import Any, Optional

import boto3
from boto3.dynamodb.conditions import Key as DKey
from botocore.exceptions import ClientError
from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth


# ==========================================================
# LOGGING
# ==========================================================

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


# ==========================================================
# ALL STATIC VARIABLES
# ==========================================================

DYNAMODB_TABLE    = os.environ.get('DYNAMODB_TABLE', 'enrichment_cache')
DYNAMODB_ENDPOINT = os.environ.get('DYNAMODB_ENDPOINT')
AWS_REGION        = os.environ.get('AWS_REGION', 'us-east-1')

ENABLE_HARD_DELETE = os.environ.get('ENABLE_HARD_DELETE', 'false').lower() == 'true'

OPENSEARCH_HOST   = os.environ.get('OPENSEARCH_HOST', 'opensearch')
OPENSEARCH_PORT   = int(os.environ.get('OPENSEARCH_PORT', '443'))
OPENSEARCH_SCHEME = os.environ.get('OPENSEARCH_SCHEME', 'https')
OPENSEARCH_AUTH   = os.environ.get('OPENSEARCH_AUTH', 'iam')          # 'iam' or 'basic'
OPENSEARCH_USERNAME = os.environ.get('OPENSEARCH_USERNAME', 'admin')
OPENSEARCH_PASSWORD = os.environ.get('OPENSEARCH_PASSWORD', 'admin')
OPENSEARCH_MODE   = os.environ.get('OPENSEARCH_MODE', 'serverless')   # 'serverless' or 'provisioned'
INDEX_ORDER_LINES = os.environ.get('INDEX_ORDER_LINES', 'order-lines')

# Entity type constants for single-table design
ENTITY_SHIPMENT_CONTAINER                 = 'YFS_SHIPMENT_CONTAINER'
ENTITY_SHIPMENT_LINE_BY_ORDER_LINE        = 'YFS_SHIPMENT_LINE_BY_ORDER_LINE'
ENTITY_CONTAINER_DETAILS_BY_SHIPMENT_LINE = 'YFS_CONTAINER_DETAILS_BY_SHIPMENT_LINE'
ENTITY_TRACKING_NO                        = 'TRACKING_NO'


# ==========================================================
# DynamoDB
# ==========================================================

dynamodb_kwargs = {'region_name': AWS_REGION}
if DYNAMODB_ENDPOINT:
    dynamodb_kwargs['endpoint_url'] = DYNAMODB_ENDPOINT
    dynamodb_kwargs['aws_access_key_id'] = os.environ.get('AWS_ACCESS_KEY_ID', 'local')
    dynamodb_kwargs['aws_secret_access_key'] = os.environ.get('AWS_SECRET_ACCESS_KEY', 'local')

dynamodb    = boto3.resource('dynamodb', **dynamodb_kwargs)
cache_table = dynamodb.Table(DYNAMODB_TABLE)



# ==========================================================
# OpenSearch
# ==========================================================

_opensearch_client = None

def get_opensearch_client():
    global _opensearch_client
    if _opensearch_client:
        return _opensearch_client
    if OPENSEARCH_AUTH == 'iam':
        credentials = boto3.Session().get_credentials()
        iam_service = 'es' if OPENSEARCH_MODE == 'provisioned' else 'aoss'
        auth = AWSV4SignerAuth(credentials, AWS_REGION, iam_service)
        _opensearch_client = OpenSearch(
            hosts=[{'host': OPENSEARCH_HOST, 'port': OPENSEARCH_PORT}],
            http_auth=auth,
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection,
            timeout=10,
            retry_on_timeout=True,
            max_retries=2,
        )
    else:
        _opensearch_client = OpenSearch(
            hosts=[{'host': OPENSEARCH_HOST, 'port': OPENSEARCH_PORT}],
            http_auth=(OPENSEARCH_USERNAME, OPENSEARCH_PASSWORD),
            use_ssl=(OPENSEARCH_SCHEME == 'https'),
            verify_certs=False,
            ssl_show_warn=False,
            connection_class=RequestsHttpConnection,
            timeout=10,
            retry_on_timeout=True,
            max_retries=2,
        )
    return _opensearch_client


# ==========================================================
# Topic → Table Routing
# ==========================================================

TOPIC_TABLE_MAP = {
    'yfs_shipment_container': 'YFS_SHIPMENT_CONTAINER',
}


def extract_table_from_topic(topic: str) -> str:
    """Extract table name from Debezium topic like 'glenraven-dev.STERLING.yfs_shipment_line'."""
    table_part = topic.rsplit('.', 1)[-1].lower()
    return TOPIC_TABLE_MAP.get(table_part, table_part.upper())


# ==========================================================
# Lambda Handler
# ==========================================================

def lambda_handler(event: dict, context: Any) -> dict:
    try:
        logger.info(f"Incoming event: {json.dumps(event, default=str)[:5000]}")

        if event.get('eventSource') == 'aws:kafka' or 'records' in event:
            return process_msk_event(event)

        return process_single_event(
            table=event.get('table', '').upper(),
            operation=event.get('operation', 'c'),
            payload=event.get('payload', {}),
            source_timestamp=event.get('sourceTimestamp')
        )

    except Exception as e:
        logger.error(f"Unhandled error in lambda_handler: {e}", exc_info=True)
        return {'statusCode': 500, 'body': str(e)}


# ==========================================================
# MSK Batch Processor
# ==========================================================

def _parse_debezium_message(debezium_event: dict) -> tuple:
    """
    Parse a Debezium message into (operation, source_timestamp_ms, payload).

    Handles two formats:
    1. Flattened SMT format (schema/payload wrapper with __op/__source_ts_ms)
       { "schema": {...}, "payload": { "COL": val, "__op": "u", "__source_ts_ms": ... } }
    2. Standard Debezium envelope
       { "op": "c", "ts_ms": ..., "before": null, "after": { "col": val } }
    """
    if 'schema' in debezium_event and 'payload' in debezium_event:
        inner = debezium_event['payload']
        if inner is None:
            return 'ignore', None, {}
        if '__op' in inner:
            operation        = inner.get('__op') or 'c'
            source_timestamp = inner.get('__source_ts_ms')
            payload          = {k: v for k, v in inner.items() if not k.startswith('__')}
            return operation, source_timestamp, payload

    operation        = debezium_event.get('op', 'c')
    source_timestamp = debezium_event.get('ts_ms')
    if operation == 'd':
        payload = debezium_event.get('before', {}) or {}
    else:
        payload = debezium_event.get('after', {}) or {}
    return operation, source_timestamp, payload


def process_msk_event(event: dict) -> dict:
    records   = event.get('records', {})
    processed = 0
    errors    = 0
    batch_tracking_updates: dict[str, list[tuple]] = {}
    container_cache: dict[str, dict] = {}
    affected_shipment_keys = set()

    for topic_partition, messages in records.items():
        logger.info(
            f"Partition start | topic_partition={topic_partition} message_count={len(messages)}"
        )

        for msg in messages:
            topic  = msg.get('topic', topic_partition.rsplit('-', 1)[0])
            offset = msg.get('offset', 'unknown')

            try:
                table = extract_table_from_topic(topic)

                if msg.get('value') is None:
                    logger.info(f'Tombstone record skipped | topic={topic} offset={offset}')
                    continue

                raw_value      = base64.b64decode(msg['value']).decode('utf-8')
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

                payload = {k.upper(): v.strip() if isinstance(v, str) else v for k, v in payload.items()}

                logger.info(
                    f"Record decoded | table={table} topic={topic} offset={offset} "
                    f"operation={operation} source_timestamp={source_timestamp}"
                )

                result = process_single_event(table, operation, payload, source_timestamp, container_cache)
                if result.get('statusCode', 0) != 200:
                    logger.error(f"Error processing record | topic={topic} offset={offset} body={result['body']}")
                    errors += 1
                else:
                    processed += 1
                    # Collect affected keys for deferred processing
                    sk = result.get('shipment_key')
                    if sk:
                        affected_shipment_keys.add(sk)

            except Exception as e:
                errors += 1
                logger.error(
                    f"FAILED record | topic={topic} offset={offset} "
                    f"topic_partition={topic_partition} error_type={type(e).__name__} error={e}",
                    exc_info=True
                )


    # ── Phase 2: Deferred Batch-level Re-derivation ──
    for sk in affected_shipment_keys:
        try:
            response = cache_table.query(
                KeyConditionExpression=DKey('PK').eq('YFS_SHIPMENT_LINE_BY_SHIPMENT') & DKey('SK').begins_with(f'{sk}#')
            )
            for i in response.get('Items', []):
                if not i.get('_deleted') and i.get('orderLineKey'):
                    olk = i.get('orderLineKey')
                    ohk = i.get('orderHeaderKey') or ''
                    if olk not in batch_tracking_updates:
                        entries = rebuild_tracking_cache(olk, ohk, container_cache)
                        if entries is not None:
                            batch_tracking_updates[olk] = entries
        except Exception as e:
            logger.warning(f"Batch Phase 2 failed | shipment_key={sk} error={e}")

    # ── Push all aggregated tracking updates at the end of the MSK batch ──
    if batch_tracking_updates:
        bulk_push_tracking_info_to_opensearch(batch_tracking_updates)

    logger.info(f"Batch complete | processed={processed} errors={errors}")
    return {"statusCode": 200 if errors == 0 else 207, "body": f"Processed {processed}, errors {errors}"}

# ==========================================================
# Event Router
# ==========================================================

def process_single_event(table: str, operation: str, payload: dict, source_timestamp=None, container_cache: Optional[dict] = None) -> dict:
    logger.info(f"Processing {operation} event for table {table}")

    if 'YFS_SHIPMENT_CONTAINER' in table:
        return process_shipment_container_event(payload, operation, container_cache)
    else:
        logger.warning(f"Unknown table: {table}")
        return {'statusCode': 400, 'body': f'Unknown table: {table}'}


# =============================================================================
# Shipment Line Processing
# =============================================================================

def process_shipment_container_event(payload: dict, operation: str, container_cache: Optional[dict] = None) -> dict:
    if container_cache is None:
        container_cache = {}
        
    shipment_container_key = get_value(payload, 'SHIPMENT_CONTAINER_KEY')
    shipment_key           = get_value(payload, 'SHIPMENT_KEY')
    
    logger.info(f"process_shipment_container_event | shipment_container_key={shipment_container_key} operation={operation}")
    
    if not shipment_container_key:
        return {'statusCode': 400, 'body': 'Missing SHIPMENT_CONTAINER_KEY'}

    if operation == 'd':
        if ENABLE_HARD_DELETE:
            cache_table.delete_item(Key={'PK': ENTITY_SHIPMENT_CONTAINER, 'SK': shipment_container_key})
        else:
            soft_delete_cache_item(ENTITY_SHIPMENT_CONTAINER, shipment_container_key)
    else:
        item = {
            'PK':                   ENTITY_SHIPMENT_CONTAINER,
            'SK':                   shipment_container_key,
            'shipmentContainerKey': shipment_container_key,
            'shipmentKey':          shipment_key,
            'trackingNo':           get_value(payload, 'TRACKING_NO'),
            'scac':                 get_value(payload, 'SCAC'),
            'carrierServiceCode':   get_value(payload, 'CARRIER_SERVICE_CODE'),
            'parentContainerKey':   get_value(payload, 'PARENT_CONTAINER_KEY'),
            '_deleted':             False,
            'updatedAt':            now_ms(),
        }
        item = {k: v for k, v in item.items() if v is not None}
        cache_table.put_item(Item=item)

    return {
        'statusCode': 200, 
        'body': f'Processed shipment container: {shipment_container_key}',
        'shipment_key': shipment_key
    }

# =============================================================================
# Tracking Cache Derivation
# =============================================================================

def rebuild_tracking_cache(order_line_key: str, order_header_key: str = '', container_cache: Optional[dict] = None) -> Optional[list[tuple]]:
    """
    Rebuild the TRACKING_NO cache entry for a given ORDER_LINE_KEY.

    Returns:
        list of (trackingNo, scac, carrierServiceCode) tuples if tracking data
        was found and written, empty list [] if no tracking data (cache cleared),
        or None if the function failed or had nothing to process.

    Steps:
    1. Query YFS_SHIPMENT_LINE_BY_ORDER_LINE to find all shipment line keys for this order line
    2. For each shipment line key:
       a. Query YFS_CONTAINER_DETAILS_BY_SHIPMENT_LINE to find child container keys
       b. For each child container:
          - Get YFS_SHIPMENT_CONTAINER (child) -> get parentContainerKey
          - Get YFS_SHIPMENT_CONTAINER (parent) -> get trackingNo, scac, carrierServiceCode
    3. Aggregate and deduplicate
    4. Write PK=TRACKING_NO, SK=<ORDER_HEADER_KEY>#<ORDER_LINE_KEY>
    """
    if container_cache is None:
        container_cache = {}

    try:
        # Step 1: Find all shipment lines for this order line
        response = cache_table.query(
            KeyConditionExpression=DKey('PK').eq(ENTITY_SHIPMENT_LINE_BY_ORDER_LINE) & DKey('SK').begins_with(f'{order_line_key}#')
        )
        line_items = [i for i in response.get('Items', []) if not i.get('_deleted')]

        if not line_items:
            logger.info(f"rebuild_tracking_cache: No shipment lines for order_line_key={order_line_key}")
            return None

        # Resolve order_header_key from the first line item if not provided
        if not order_header_key:
            order_header_key = line_items[0].get('orderHeaderKey', '')

        # Collect unique (tracking_no, scac, carrier_service_code) tuples
        dedup: dict[tuple, tuple] = {}
        shipment_line_keys = {item.get('shipmentLineKey') for item in line_items if item.get('shipmentLineKey')}

        for slk in shipment_line_keys:
            # Step 2: Find container details for this shipment line
            details_resp = cache_table.query(
                KeyConditionExpression=DKey('PK').eq(ENTITY_CONTAINER_DETAILS_BY_SHIPMENT_LINE) & DKey('SK').begins_with(f'{slk}#')
            )
            details_items = [d for d in details_resp.get('Items', []) if not d.get('_deleted')]

            for detail in details_items:
                child_sk = detail.get('shipmentContainerKey')
                if not child_sk:
                    continue

                # Get child container
                if child_sk not in container_cache:
                    child_resp = cache_table.get_item(Key={'PK': ENTITY_SHIPMENT_CONTAINER, 'SK': child_sk})
                    container_cache[child_sk] = child_resp.get('Item')

                child_container = container_cache[child_sk]
                if not child_container or child_container.get('_deleted'):
                    continue

                # The authoritative tracking is on the parent container (if one exists).
                # If there's no parent, this container might be the top-level one.
                parent_sk = child_container.get('parentContainerKey')

                if parent_sk:
                    if parent_sk not in container_cache:
                        parent_resp = cache_table.get_item(Key={'PK': ENTITY_SHIPMENT_CONTAINER, 'SK': parent_sk})
                        container_cache[parent_sk] = parent_resp.get('Item')
                    target_container = container_cache[parent_sk]
                else:
                    target_container = child_container

                if not target_container or target_container.get('_deleted'):
                    continue

                trk = target_container.get('trackingNo', '')
                if trk:
                    scac = target_container.get('scac', '')
                    csc  = target_container.get('carrierServiceCode', '')
                    dedup_key = (trk, scac, csc)
                    dedup[dedup_key] = (trk, scac, csc)

        # Step 3: Sort and build parallel lists
        sorted_entries = sorted(dedup.values(), key=lambda x: x[0])
        tracking_numbers      = [e[0] for e in sorted_entries]
        scac_list             = [e[1] for e in sorted_entries]
        carrier_service_codes = [e[2] for e in sorted_entries]

        tracking_sk = f"{order_header_key}#{order_line_key}"

        # Step 4: Write TRACKING_NO cache entry
        if tracking_numbers:
            tracking_item = {
                'PK':                 ENTITY_TRACKING_NO,
                'SK':                 tracking_sk,
                'orderHeaderKey':     order_header_key,
                'orderLineKey':       order_line_key,
                'trackingNumbers':    tracking_numbers,
                'scac':               scac_list,
                'carrierServiceCode': carrier_service_codes,
                'updatedAt':          now_ms(),
            }
            cache_table.put_item(Item=tracking_item)
            logger.info(
                f"rebuild_tracking_cache: Written | order_line_key={order_line_key} "
                f"order_header_key={order_header_key} tracking_count={len(tracking_numbers)}"
            )
            return sorted_entries
        else:
            if ENABLE_HARD_DELETE:
                cache_table.delete_item(Key={'PK': ENTITY_TRACKING_NO, 'SK': tracking_sk})
            else:
                soft_delete_cache_item(ENTITY_TRACKING_NO, tracking_sk)
            logger.info(f"rebuild_tracking_cache: No tracking data, cleared cache | order_line_key={order_line_key}")
            return []

    except Exception as e:
        logger.error(
            f"rebuild_tracking_cache FAILED | order_line_key={order_line_key} error={e}",
            exc_info=True,
        )
        return None


def bulk_push_tracking_info_to_opensearch(
    tracking_updates: dict[str, list[tuple]],
) -> None:
    """
    Batch-update the order-lines OpenSearch index with tracking info.

    Instead of N individual update calls:
    1. One mget call to check which order-line docs exist
    2. One _bulk update call for only the existing docs

    Processes in chunks of 1000 to avoid OpenSearch request size limits.

    tracking_updates: {order_line_key: [(trackingNo, scac, csc), ...]}
                      Empty list means clear tracking data.
    """
    if not tracking_updates:
        return

    try:
        client = get_opensearch_client()
        all_keys = list(tracking_updates.keys())
        chunk_size = 1000
        
        total_skipped = 0
        total_success = 0
        total_failed  = 0

        for i in range(0, len(all_keys), chunk_size):
            chunk_keys = all_keys[i:i + chunk_size]

            # ── Step 1: mget to find which docs exist ───────────────────────
            mget_body = {
                'docs': [{'_index': INDEX_ORDER_LINES, '_id': k, '_source': False} for k in chunk_keys]
            }
            mget_response = client.mget(body=mget_body)

            existing_keys = set()
            for doc in mget_response.get('docs', []):
                if doc.get('found'):
                    existing_keys.add(doc['_id'])

            skipped = len(chunk_keys) - len(existing_keys)
            total_skipped += skipped
            
            if not existing_keys:
                continue

            # ── Step 2: Build bulk update actions for existing docs only ────
            bulk_actions = []
            for order_line_key in existing_keys:
                entries = tracking_updates[order_line_key]

                if entries:
                    tracking_info = []
                    for trk, scac, csc in entries:
                        entry: dict = {'trackingNo': trk}
                        if scac:
                            entry['scac'] = scac
                        if csc:
                            entry['carrierServiceCode'] = csc
                        tracking_info.append(entry)
                    update_doc: dict = {'trackingInfo': tracking_info}
                    # Also populate the top-level fields from the first entry if available
                    if entries[0][1]:
                        update_doc['scac'] = entries[0][1]
                    if entries[0][2]:
                        update_doc['carrierServiceCode'] = entries[0][2]
                else:
                    update_doc = {
                        'trackingInfo': None,
                        'scac': None,
                        'carrierServiceCode': None
                    }

                # Bulk update action pair
                bulk_actions.append({'update': {'_index': INDEX_ORDER_LINES, '_id': order_line_key}})
                bulk_actions.append({'doc': update_doc})

            # ── Step 3: Execute bulk update ─────────────────────────────────
            if bulk_actions:
                bulk_response = client.bulk(body=bulk_actions, refresh=False)
                errors = bulk_response.get('errors', False)
                items  = bulk_response.get('items', [])

                success_count = sum(1 for it in items if it.get('update', {}).get('status', 0) in (200, 201))
                fail_count    = len(items) - success_count
                
                total_success += success_count
                total_failed += fail_count

                if errors:
                    failed_items = [
                        it for it in items
                        if it.get('update', {}).get('status', 0) not in (200, 201)
                    ]
                    logger.warning(
                        f"bulk_push_tracking chunk: Completed with errors | "
                        f"success={success_count} failed={fail_count} "
                        f"sample_error={failed_items[0] if failed_items else 'N/A'}"
                    )
                    
        # Final summary log
        logger.info(
            f"bulk_push_tracking COMPLETE | "
            f"total_updated={total_success} total_skipped={total_skipped} "
            f"total_failed={total_failed} total_keys={len(all_keys)}"
        )

    except Exception as e:
        logger.warning(
            f"bulk_push_tracking FAILED | "
            f"total_keys={len(tracking_updates)} error={e}",
            exc_info=True,
        )


# =============================================================================
# DynamoDB Helpers
# =============================================================================

def soft_delete_cache_item(pk: str, sk: str) -> None:
    """Soft-delete a DynamoDB cache row by marking it as deleted instead of removing it.
    Uses a ConditionExpression to prevent creating phantom deleted records
    if the item doesn't already exist in DynamoDB."""
    try:
        cache_table.update_item(
            Key={'PK': pk, 'SK': sk},
            UpdateExpression='SET #d = :true, #da = :now',
            ConditionExpression='attribute_exists(PK)',
            ExpressionAttributeNames={'#d': '_deleted', '#da': '_deletedAt'},
            ExpressionAttributeValues={':true': True, ':now': datetime.now(UTC).isoformat()},
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            logger.info(f"soft_delete_cache_item: Item not found, skipping phantom delete | pk={pk} sk={sk}")
        else:
            raise


# =============================================================================
# Utility Functions
# =============================================================================

def get_value(payload: dict, key: str) -> Optional[str]:
    """Get string value from payload, stripping trailing whitespace from Oracle CHAR fields."""
    value = payload.get(key)
    if value is None:
        return None
    s = str(value).strip()
    return s if s else None


def now_ms() -> int:
    return int(datetime.now(UTC).timestamp() * 1000)
