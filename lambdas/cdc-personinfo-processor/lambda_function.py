"""
CDC Lambda Handler (PersonInfo) - processes YFS_PERSON_INFO CDC events via MSK.

Consolidated Fan-Out (3 calls covering 5 conceptual phases):
  Phase 1      Cache person info to DynamoDB (PK=PERSON_INFO, SK=<personInfoKey>)
  Phase 2      Single update_by_query on orders — billTo/shipTo/soldTo nested
               (queries on billToKey / shipToKey / soldToKey)
  Phase 3+4+5  Single update_by_query on organizations covering:
               - corporate address (corporateAddressKey, hash-gated)
               - contact           (contactAddressKey, hash-gated)
               - billTo/shipTo/soldTo nested roles (*.personInfoKey)
               Painless emits ctx.op='noop' for matched docs whose hash already
               matches, so updated/noops are reported per call.

All fan-out phases are non-fatal: failures are logged as warnings and processing continues.

DynamoDB Single Table Design:
- Table: enrichment_cache  (configurable via DYNAMODB_TABLE env var)
- PK=PERSON_INFO, SK=<personInfoKey>
"""

import base64
import hashlib
import json
import logging
import os
import time

from datetime import datetime, UTC
from typing import Any, Optional

import boto3
from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth


# ==========================================================
# ALL STATIC VARIABLES
# ==========================================================

DYNAMODB_TABLE      = os.environ.get('DYNAMODB_TABLE', 'enrichment_cache')
DYNAMODB_ENDPOINT   = os.environ.get('DYNAMODB_ENDPOINT')
OPENSEARCH_HOST     = os.environ.get('OPENSEARCH_HOST', 'opensearch')
OPENSEARCH_PORT     = int(os.environ.get('OPENSEARCH_PORT', '443'))
OPENSEARCH_SCHEME   = os.environ.get('OPENSEARCH_SCHEME', 'https')
OPENSEARCH_AUTH     = os.environ.get('OPENSEARCH_AUTH', 'iam')   # 'iam' or 'basic'
OPENSEARCH_USERNAME = os.environ.get('OPENSEARCH_USERNAME', 'admin')
OPENSEARCH_PASSWORD = os.environ.get('OPENSEARCH_PASSWORD', 'admin')
OPENSEARCH_MODE     = os.environ.get('OPENSEARCH_MODE', 'serverless')  # 'provisioned' or 'serverless'
AWS_REGION          = os.environ.get('AWS_REGION', 'us-east-1')

INDEX_ORDERS       = os.environ.get('INDEX_ORDERS', 'orders')
INDEX_ORGANIZATION = os.environ.get('INDEX_ORGANIZATION', 'organizations')

ENTITY_PERSON_INFO = 'PERSON_INFO'

ENABLE_HARD_DELETE = os.environ.get('ENABLE_HARD_DELETE', 'false').lower() == 'true'


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
# OpenSearch Client (lazy singleton)
# ==========================================================

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


# ==========================================================
# Painless Scripts
# ==========================================================

# Mirrors build_searchable_text() in cdc-lambda-orderheader.
# Prepend field-assignment statements before appending this script.
PAINLESS_REBUILD_ORDER_SEARCHABLE_TEXT = """
    def parts = [];

    if (ctx._source.orderNo != null) parts.add(ctx._source.orderNo);
    if (ctx._source.orderHeaderKey != null) parts.add(ctx._source.orderHeaderKey);
    if (ctx._source.enterpriseKey != null) parts.add(ctx._source.enterpriseKey);
    if (ctx._source.orderName != null) parts.add(ctx._source.orderName);

    if (ctx._source.customer != null) {
        if (ctx._source.customer.customerFirstName != null) parts.add(ctx._source.customer.customerFirstName);
        if (ctx._source.customer.customerLastName != null) parts.add(ctx._source.customer.customerLastName);
        if (ctx._source.customer.customerEmailId != null) parts.add(ctx._source.customer.customerEmailId);
        if (ctx._source.customer.customerPhoneNo != null) parts.add(ctx._source.customer.customerPhoneNo);
        if (ctx._source.customer.customerPoNo != null) parts.add(ctx._source.customer.customerPoNo);
        if (ctx._source.customer.organizationCode != null) parts.add(ctx._source.customer.organizationCode);
        if (ctx._source.customer.organizationName != null) parts.add(ctx._source.customer.organizationName);
    }

    if (ctx._source.sellerOrganizationName != null) parts.add(ctx._source.sellerOrganizationName);
    if (ctx._source.buyerOrganizationName != null) parts.add(ctx._source.buyerOrganizationName);
    if (ctx._source.sellerOrganizationCode != null) parts.add(ctx._source.sellerOrganizationCode);
    if (ctx._source.buyerOrganizationCode != null) parts.add(ctx._source.buyerOrganizationCode);

    if (ctx._source.orderStatus != null) parts.add(ctx._source.orderStatus);

    if (ctx._source.billTo != null) {
        if (ctx._source.billTo.fullName != null) parts.add(ctx._source.billTo.fullName);
        if (ctx._source.billTo.emailId != null) parts.add(ctx._source.billTo.emailId);
        if (ctx._source.billTo.mobilePhone != null) parts.add(ctx._source.billTo.mobilePhone);
        if (ctx._source.billTo.dayPhone != null) parts.add(ctx._source.billTo.dayPhone);
        if (ctx._source.billTo.eveningPhone != null) parts.add(ctx._source.billTo.eveningPhone);
        if (ctx._source.billTo.company != null) parts.add(ctx._source.billTo.company);
        if (ctx._source.billTo.city != null) parts.add(ctx._source.billTo.city);
        if (ctx._source.billTo.state != null) parts.add(ctx._source.billTo.state);
    }

    if (ctx._source.shipTo != null) {
        if (ctx._source.shipTo.fullName != null) parts.add(ctx._source.shipTo.fullName);
        if (ctx._source.shipTo.emailId != null) parts.add(ctx._source.shipTo.emailId);
        if (ctx._source.shipTo.mobilePhone != null) parts.add(ctx._source.shipTo.mobilePhone);
        if (ctx._source.shipTo.dayPhone != null) parts.add(ctx._source.shipTo.dayPhone);
        if (ctx._source.shipTo.eveningPhone != null) parts.add(ctx._source.shipTo.eveningPhone);
        if (ctx._source.shipTo.company != null) parts.add(ctx._source.shipTo.company);
        if (ctx._source.shipTo.city != null) parts.add(ctx._source.shipTo.city);
        if (ctx._source.shipTo.state != null) parts.add(ctx._source.shipTo.state);
    }

    if (ctx._source.soldTo != null) {
        if (ctx._source.soldTo.fullName != null) parts.add(ctx._source.soldTo.fullName);
        if (ctx._source.soldTo.emailId != null) parts.add(ctx._source.soldTo.emailId);
        if (ctx._source.soldTo.mobilePhone != null) parts.add(ctx._source.soldTo.mobilePhone);
        if (ctx._source.soldTo.dayPhone != null) parts.add(ctx._source.soldTo.dayPhone);
        if (ctx._source.soldTo.eveningPhone != null) parts.add(ctx._source.soldTo.eveningPhone);
        if (ctx._source.soldTo.company != null) parts.add(ctx._source.soldTo.company);
        if (ctx._source.soldTo.city != null) parts.add(ctx._source.soldTo.city);
        if (ctx._source.soldTo.state != null) parts.add(ctx._source.soldTo.state);
    }

    ctx._source.searchableText = String.join(' ', parts);
"""

# Mirrors searchableText build in cdc-lambda-organization and cdc-lambda-customeraddnladdress.
PAINLESS_REBUILD_ORG_SEARCHABLE_TEXT = """
    def parts = [];

    if (ctx._source.organizationCode != null) parts.add(ctx._source.organizationCode);
    if (ctx._source.organizationName != null) parts.add(ctx._source.organizationName);

    if (ctx._source.customer != null) {
        if (ctx._source.customer.customerId != null) parts.add(ctx._source.customer.customerId);
        if (ctx._source.customer.customerRewardsNo != null) parts.add(ctx._source.customer.customerRewardsNo);
    }
    if (ctx._source.extnAplusCustomerId != null) parts.add(ctx._source.extnAplusCustomerId);

    if (ctx._source.contact != null) {
        if (ctx._source.contact.fullName != null) parts.add(ctx._source.contact.fullName);
        if (ctx._source.contact.email != null) parts.add(ctx._source.contact.email);
        if (ctx._source.contact.phone != null) parts.add(ctx._source.contact.phone);
    }

    // customerContact is an array of contact objects
    if (ctx._source.customerContact != null) {
        for (def c : ctx._source.customerContact) {
            if (c.contactName != null) parts.add(c.contactName);
            if (c.contactEmail != null) parts.add(c.contactEmail);
            if (c.contactPhone != null) parts.add(c.contactPhone);
        }
    }

    if (ctx._source.billTo != null) {
        if (ctx._source.billTo.fullName != null) parts.add(ctx._source.billTo.fullName);
        if (ctx._source.billTo.emailId != null) parts.add(ctx._source.billTo.emailId);
        if (ctx._source.billTo.dayPhone != null) parts.add(ctx._source.billTo.dayPhone);
        if (ctx._source.billTo.city != null) parts.add(ctx._source.billTo.city);
        if (ctx._source.billTo.state != null) parts.add(ctx._source.billTo.state);
        if (ctx._source.billTo.company != null) parts.add(ctx._source.billTo.company);
    }

    if (ctx._source.shipTo != null) {
        if (ctx._source.shipTo.fullName != null) parts.add(ctx._source.shipTo.fullName);
        if (ctx._source.shipTo.emailId != null) parts.add(ctx._source.shipTo.emailId);
        if (ctx._source.shipTo.dayPhone != null) parts.add(ctx._source.shipTo.dayPhone);
        if (ctx._source.shipTo.city != null) parts.add(ctx._source.shipTo.city);
        if (ctx._source.shipTo.state != null) parts.add(ctx._source.shipTo.state);
        if (ctx._source.shipTo.company != null) parts.add(ctx._source.shipTo.company);
    }

    if (ctx._source.soldTo != null) {
        if (ctx._source.soldTo.fullName != null) parts.add(ctx._source.soldTo.fullName);
        if (ctx._source.soldTo.emailId != null) parts.add(ctx._source.soldTo.emailId);
        if (ctx._source.soldTo.dayPhone != null) parts.add(ctx._source.soldTo.dayPhone);
        if (ctx._source.soldTo.city != null) parts.add(ctx._source.soldTo.city);
        if (ctx._source.soldTo.state != null) parts.add(ctx._source.soldTo.state);
        if (ctx._source.soldTo.company != null) parts.add(ctx._source.soldTo.company);
    }

    if (ctx._source.extnSalesPerson != null) parts.add(ctx._source.extnSalesPerson);
    if (ctx._source.extnInsideSalesRep != null) parts.add(ctx._source.extnInsideSalesRep);
    if (ctx._source.extnMarketSegment != null) parts.add(ctx._source.extnMarketSegment);

    ctx._source.searchableText = String.join(' ', parts);
"""


# ==========================================================
# Topic → Table Routing
# ==========================================================

TOPIC_TABLE_MAP = {
    'yfs_person_info': 'YFS_PERSON_INFO',
}


def extract_table_from_topic(topic: str) -> str:
    """Extract table name from Debezium topic like 'glenraven-dev.STERLING.yfs_person_info'."""
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

                result = process_single_event(table, operation, payload, source_timestamp)
                if result.get('statusCode', 0) != 200:
                    logger.error(f"Error processing record | topic={topic} offset={offset} body={result['body']}")
                    errors += 1
                else:
                    processed += 1

            except Exception as e:
                errors += 1
                logger.error(
                    f"FAILED record | topic={topic} offset={offset} "
                    f"topic_partition={topic_partition} error_type={type(e).__name__} error={e}",
                    exc_info=True
                )

    logger.info(f"Batch complete | processed={processed} errors={errors}")
    return {"statusCode": 200 if errors == 0 else 207, "body": f"Processed {processed}, errors {errors}"}

# ==========================================================
# Event Router
# ==========================================================

def process_single_event(table: str, operation: str, payload: dict, source_timestamp=None) -> dict:
    logger.info(f"Processing {operation} event for table {table}")

    if 'YFS_PERSON_INFO' in table:
        return process_person_info(payload, operation)
    else:
        logger.warning(f"Unknown table: {table}")
        return {'statusCode': 400, 'body': f'Unknown table: {table}'}


# =============================================================================
# PersonInfo Processing
# =============================================================================

def process_person_info(payload: dict, operation: str) -> dict:
    """
    PersonInfo write flow (3 calls covering 5 conceptual phases):
    - Phase 1     : Cache to DynamoDB (PK=PERSON_INFO, SK=<personInfoKey>)
    - Phase 2     : Fan-out to orders  (billToKey / shipToKey / soldToKey)
    - Phase 3+4+5 : Fan-out to orgs    (corporateAddressKey + contactAddressKey + role.personInfoKey)
    """
    start_total = time.perf_counter()
    person_info_key = get_value(payload, 'PERSON_INFO_KEY')

    if not person_info_key:
        logger.warning(f"Missing PERSON_INFO_KEY. Payload keys: {list(payload.keys())}")
        return {'statusCode': 400, 'body': 'Missing PERSON_INFO_KEY'}

    logger.info(f"process_person_info | key={person_info_key} operation={operation}")

    # PHASE 1: Update DynamoDB cache
    phase1_start = time.perf_counter()
    if operation == 'd':
        if ENABLE_HARD_DELETE:
            cache_table.delete_item(Key={'PK': ENTITY_PERSON_INFO, 'SK': person_info_key})
            logger.info(f"Phase 1: Hard-deleted person info from cache | key={person_info_key}")
        else:
            soft_delete_cache_item(ENTITY_PERSON_INFO, person_info_key)
            logger.info(f"Phase 1: Soft-deleted person info from cache | key={person_info_key}")
    else:
        first_name  = get_value(payload, 'FIRST_NAME')
        middle_name = get_value(payload, 'MIDDLE_NAME')
        last_name   = get_value(payload, 'LAST_NAME')
        item = {
            'PK':           ENTITY_PERSON_INFO,
            'SK':           person_info_key,
            'personInfoKey': person_info_key,
            'firstName':    first_name,
            'middleName':   middle_name,
            'lastName':     last_name,
            'fullName':     ' '.join(filter(None, [first_name, middle_name, last_name])) or None,
            'company':      get_value(payload, 'COMPANY'),
            'addressLine1': get_value(payload, 'ADDRESS_LINE1'),
            'addressLine2': get_value(payload, 'ADDRESS_LINE2'),
            'addressLine3': get_value(payload, 'ADDRESS_LINE3'),
            'addressLine4': get_value(payload, 'ADDRESS_LINE4'),
            'addressLine5': get_value(payload, 'ADDRESS_LINE5'),
            'addressLine6': get_value(payload, 'ADDRESS_LINE6'),
            'city':         get_value(payload, 'CITY'),
            'state':        get_value(payload, 'STATE'),
            'zipCode':      get_value(payload, 'ZIP_CODE'),
            'country':      get_value(payload, 'COUNTRY'),
            'emailId':      get_value(payload, 'EMAILID'),
            'dayPhone':     get_value(payload, 'DAY_PHONE'),
            'eveningPhone': get_value(payload, 'EVENING_PHONE'),
            'mobilePhone':  get_value(payload, 'MOBILE_PHONE'),
            'addressHash':  compute_address_hash(payload),
            'contactHash':  compute_contact_hash(payload),
            'updatedAt':    datetime.now(UTC).isoformat()
        }
        item = {k: v for k, v in item.items() if v is not None}
        try:
            cache_table.put_item(Item=item)
            logger.info(f"Phase 1: DynamoDB write SUCCESS | key={person_info_key}")
        except Exception as e:
            logger.error(f"Phase 1: DynamoDB write FAILED | key={person_info_key} error={e}", exc_info=True)
            raise
    phase1_ms = (time.perf_counter() - phase1_start) * 1000
    logger.info(f"TIMING phase1_dynamodb | key={person_info_key} duration_ms={phase1_ms:.1f}")

    # PHASE 2: Single fan-out to orders (billTo / shipTo / soldTo in one query)
    update_orders_for_person(person_info_key, payload, operation)

    # PHASE 3+4+5: Single fan-out to organizations
    # (corporate address + contact + billTo/shipTo/soldTo roles in one query)
    update_org_for_person(person_info_key, payload, operation)

    total_ms = (time.perf_counter() - start_total) * 1000
    logger.info(f"TIMING process_person_info_total | key={person_info_key} operation={operation} duration_ms={total_ms:.1f}")

    return {'statusCode': 200, 'body': f'Processed person info: {person_info_key}'}


def build_order_address(payload: dict) -> dict:
    """Phase 2 shape: nested address for orders billTo/shipTo/soldTo (no hash)."""
    first  = get_value(payload, 'FIRST_NAME')
    middle = get_value(payload, 'MIDDLE_NAME')
    last   = get_value(payload, 'LAST_NAME')
    return {
        'personInfoKey': get_value(payload, 'PERSON_INFO_KEY'),
        'firstName':     first,
        'middleName':    middle,
        'lastName':      last,
        'fullName':      ' '.join(filter(None, [first, middle, last])) or None,
        'emailId':       get_value(payload, 'EMAILID'),
        'dayPhone':      get_value(payload, 'DAY_PHONE'),
        'eveningPhone':  get_value(payload, 'EVENING_PHONE'),
        'mobilePhone':   get_value(payload, 'MOBILE_PHONE'),
        'company':       get_value(payload, 'COMPANY'),
        'addressLine1':  get_value(payload, 'ADDRESS_LINE1'),
        'addressLine2':  get_value(payload, 'ADDRESS_LINE2'),
        'addressLine3':  get_value(payload, 'ADDRESS_LINE3'),
        'addressLine4':  get_value(payload, 'ADDRESS_LINE4'),
        'addressLine5':  get_value(payload, 'ADDRESS_LINE5'),
        'addressLine6':  get_value(payload, 'ADDRESS_LINE6'),
        'city':          get_value(payload, 'CITY'),
        'state':         get_value(payload, 'STATE'),
        'zipCode':       get_value(payload, 'ZIP_CODE'),
        'country':       get_value(payload, 'COUNTRY'),
    }


def build_corporate_address(payload: dict) -> dict:
    """Phase 3 shape: corporate address fields + addressHash (None values dropped)."""
    new_hash = compute_address_hash(payload)
    return {k: v for k, v in {
        'addressLine1': get_value(payload, 'ADDRESS_LINE1'),
        'addressLine2': get_value(payload, 'ADDRESS_LINE2'),
        'addressLine3': get_value(payload, 'ADDRESS_LINE3'),
        'addressLine4': get_value(payload, 'ADDRESS_LINE4'),
        'addressLine5': get_value(payload, 'ADDRESS_LINE5'),
        'addressLine6': get_value(payload, 'ADDRESS_LINE6'),
        'city':         get_value(payload, 'CITY'),
        'state':        get_value(payload, 'STATE'),
        'zipCode':      get_value(payload, 'ZIP_CODE'),
        'country':      get_value(payload, 'COUNTRY'),
        'addressHash':  new_hash,
    }.items() if v is not None}


def build_contact(payload: dict) -> dict:
    """Phase 4 shape: contact fields + contactHash. Phone priority: mobile → day → evening."""
    new_hash = compute_contact_hash(payload)
    first    = get_value(payload, 'FIRST_NAME')
    middle   = get_value(payload, 'MIDDLE_NAME')
    last     = get_value(payload, 'LAST_NAME')
    return {k: v for k, v in {
        'firstName':    first,
        'lastName':     last,
        'fullName':     ' '.join(filter(None, [first, middle, last])) or None,
        'email':        get_value(payload, 'EMAILID'),
        'phone':        (
            get_value(payload, 'MOBILE_PHONE') or
            get_value(payload, 'DAY_PHONE') or
            get_value(payload, 'EVENING_PHONE')
        ),
        'dayPhone':     get_value(payload, 'DAY_PHONE'),
        'eveningPhone': get_value(payload, 'EVENING_PHONE'),
        'mobilePhone':  get_value(payload, 'MOBILE_PHONE'),
        'company':      get_value(payload, 'COMPANY'),
        'addressLine1': get_value(payload, 'ADDRESS_LINE1'),
        'addressLine2': get_value(payload, 'ADDRESS_LINE2'),
        'addressLine3': get_value(payload, 'ADDRESS_LINE3'),
        'addressLine4': get_value(payload, 'ADDRESS_LINE4'),
        'addressLine5': get_value(payload, 'ADDRESS_LINE5'),
        'addressLine6': get_value(payload, 'ADDRESS_LINE6'),
        'city':         get_value(payload, 'CITY'),
        'state':        get_value(payload, 'STATE'),
        'zipCode':      get_value(payload, 'ZIP_CODE'),
        'country':      get_value(payload, 'COUNTRY'),
        'contactHash':  new_hash,
    }.items() if v is not None}


def build_role_address(payload: dict) -> dict:
    """Phase 5 shape: nested address for org billTo/shipTo/soldTo (no hash, no middleName field)."""
    first  = get_value(payload, 'FIRST_NAME')
    middle = get_value(payload, 'MIDDLE_NAME')
    last   = get_value(payload, 'LAST_NAME')
    return {
        'personInfoKey': get_value(payload, 'PERSON_INFO_KEY'),
        'firstName':     first,
        'lastName':      last,
        'fullName':      ' '.join(filter(None, [first, middle, last])) or None,
        'emailId':       get_value(payload, 'EMAILID'),
        'dayPhone':      get_value(payload, 'DAY_PHONE'),
        'eveningPhone':  get_value(payload, 'EVENING_PHONE'),
        'mobilePhone':   get_value(payload, 'MOBILE_PHONE'),
        'company':       get_value(payload, 'COMPANY'),
        'addressLine1':  get_value(payload, 'ADDRESS_LINE1'),
        'addressLine2':  get_value(payload, 'ADDRESS_LINE2'),
        'addressLine3':  get_value(payload, 'ADDRESS_LINE3'),
        'addressLine4':  get_value(payload, 'ADDRESS_LINE4'),
        'addressLine5':  get_value(payload, 'ADDRESS_LINE5'),
        'addressLine6':  get_value(payload, 'ADDRESS_LINE6'),
        'city':          get_value(payload, 'CITY'),
        'state':         get_value(payload, 'STATE'),
        'zipCode':       get_value(payload, 'ZIP_CODE'),
        'country':       get_value(payload, 'COUNTRY'),
    }


def update_orders_for_person(person_info_key: str, payload: dict, operation: str) -> None:
    """
    Phase 2: single update_by_query covering billToKey / shipToKey / soldToKey.
    A doc whose person key matches multiple roles is updated once, not 3x.
    """
    start = time.perf_counter()
    try:
        client = get_opensearch_client()
        address_obj = None if operation == 'd' else build_order_address(payload)

        script_source = """
            boolean changed = false;
            if (ctx._source.billToKey == params.key) {
                ctx._source.billTo = params.address; changed = true;
            }
            if (ctx._source.shipToKey == params.key) {
                ctx._source.shipTo = params.address; changed = true;
            }
            if (ctx._source.soldToKey == params.key) {
                ctx._source.soldTo = params.address; changed = true;
            }
            if (!changed) { ctx.op = 'noop'; return; }
        """ + PAINLESS_REBUILD_ORDER_SEARCHABLE_TEXT

        result = client.update_by_query(
            index=INDEX_ORDERS,
            body={
                "query": {
                    "bool": {
                        "should": [
                            {"term": {"billToKey": person_info_key}},
                            {"term": {"shipToKey": person_info_key}},
                            {"term": {"soldToKey": person_info_key}},
                        ],
                        "minimum_should_match": 1
                    }
                },
                "script": {
                    "source": script_source,
                    "lang":   "painless",
                    "params": {"key": person_info_key, "address": address_obj}
                }
            },
            conflicts='proceed',
            refresh=True
        )
        updated = result.get('updated', 0)
        noops   = result.get('noops',   0)
        elapsed_ms = (time.perf_counter() - start) * 1000
        logger.info(f"Orders fan-out | key={person_info_key} updated={updated} noops={noops} duration_ms={elapsed_ms:.1f}")

    except Exception as e:
        elapsed_ms = (time.perf_counter() - start) * 1000
        logger.warning(f"Orders fan-out failed | key={person_info_key} duration_ms={elapsed_ms:.1f} error={e}")


def update_org_for_person(person_info_key: str, payload: dict, operation: str) -> None:
    """
    Phase 3+4+5 combined: one update_by_query covering corporate address,
    contact, and billTo/shipTo/soldTo roles. Hash-gating for phases 3 and 4
    happens in Painless via ctx.op = 'noop' rather than a query-side must_not.
    """
    start = time.perf_counter()
    try:
        client = get_opensearch_client()

        if operation == 'd':
            corp_address = None
            contact      = None
            role_address = None
        else:
            corp_address = build_corporate_address(payload)   # phase 3 shape + addressHash
            contact      = build_contact(payload)             # phase 4 shape + contactHash
            role_address = build_role_address(payload)        # phase 5 shape (no hash)

        script_source = """
            boolean changed = false;

            // Phase 3: corporate address (hash-gated)
            if (ctx._source.corporateAddressKey == params.key) {
                if (params.corpAddress == null) {
                    if (ctx._source.containsKey('address')) {
                        ctx._source.remove('address'); changed = true;
                    }
                } else {
                    def cur = ctx._source.address;
                    if (cur == null || cur.addressHash != params.corpAddress.addressHash) {
                        ctx._source.address = params.corpAddress; changed = true;
                    }
                }
            }

            // Phase 4: contact (hash-gated)
            if (ctx._source.contactAddressKey == params.key) {
                if (params.contact == null) {
                    if (ctx._source.containsKey('contact')) {
                        ctx._source.remove('contact'); changed = true;
                    }
                } else {
                    def cur = ctx._source.contact;
                    if (cur == null || cur.contactHash != params.contact.contactHash) {
                        ctx._source.contact = params.contact; changed = true;
                    }
                }
            }

            // Phase 5: billTo / shipTo / soldTo (no hash; mirror old behavior)
            for (String role : new String[] {'billTo', 'shipTo', 'soldTo'}) {
                def slot = ctx._source[role];
                if (slot != null && slot.personInfoKey == params.key) {
                    ctx._source[role] = params.roleAddress;
                    changed = true;
                }
            }

            if (!changed) { ctx.op = 'noop'; return; }
        """ + PAINLESS_REBUILD_ORG_SEARCHABLE_TEXT

        result = client.update_by_query(
            index=INDEX_ORGANIZATION,
            body={
                "query": {
                    "bool": {
                        "should": [
                            {"term": {"corporateAddressKey":  person_info_key}},
                            {"term": {"contactAddressKey":    person_info_key}},
                            {"term": {"billTo.personInfoKey": person_info_key}},
                            {"term": {"shipTo.personInfoKey": person_info_key}},
                            {"term": {"soldTo.personInfoKey": person_info_key}},
                        ],
                        "minimum_should_match": 1
                    }
                },
                "script": {
                    "source": script_source,
                    "lang":   "painless",
                    "params": {
                        "key":         person_info_key,
                        "corpAddress": corp_address,
                        "contact":     contact,
                        "roleAddress": role_address,
                    }
                }
            },
            conflicts='proceed',
            refresh=True
        )
        updated = result.get('updated', 0)
        noops   = result.get('noops',   0)
        elapsed_ms = (time.perf_counter() - start) * 1000
        logger.info(f"Org fan-out | key={person_info_key} updated={updated} noops={noops} duration_ms={elapsed_ms:.1f}")

    except Exception as e:
        elapsed_ms = (time.perf_counter() - start) * 1000
        logger.warning(f"Org fan-out failed | key={person_info_key} duration_ms={elapsed_ms:.1f} error={e}")


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


def compute_address_hash(payload: dict) -> str:
    """Stable 16-char hash of address fields written into org.address.addressHash."""
    fields = {
        'addressLine1': get_value(payload, 'ADDRESS_LINE1'),
        'addressLine2': get_value(payload, 'ADDRESS_LINE2'),
        'addressLine3': get_value(payload, 'ADDRESS_LINE3'),
        'addressLine4': get_value(payload, 'ADDRESS_LINE4'),
        'addressLine5': get_value(payload, 'ADDRESS_LINE5'),
        'addressLine6': get_value(payload, 'ADDRESS_LINE6'),
        'city':         get_value(payload, 'CITY'),
        'state':        get_value(payload, 'STATE'),
        'zipCode':      get_value(payload, 'ZIP_CODE'),
        'country':      get_value(payload, 'COUNTRY'),
    }
    return hashlib.sha256(json.dumps(fields, sort_keys=True).encode()).hexdigest()[:16]


def compute_contact_hash(payload: dict) -> str:
    """Stable 16-char hash of contact fields written into org.contact.contactHash."""
    fields = {
        'firstName':   get_value(payload, 'FIRST_NAME'),
        'middleName':  get_value(payload, 'MIDDLE_NAME'),
        'lastName':    get_value(payload, 'LAST_NAME'),
        'emailId':     get_value(payload, 'EMAILID'),
        'mobilePhone': get_value(payload, 'MOBILE_PHONE'),
        'dayPhone':    get_value(payload, 'DAY_PHONE'),
        'eveningPhone': get_value(payload, 'EVENING_PHONE'),
    }
    return hashlib.sha256(json.dumps(fields, sort_keys=True).encode()).hexdigest()[:16]


def soft_delete_cache_item(pk: str, sk: str) -> None:
    """Soft-delete a DynamoDB cache row by marking it as deleted instead of removing it."""
    cache_table.update_item(
        Key={'PK': pk, 'SK': sk},
        UpdateExpression='SET #d = :true, #da = :now',
        ExpressionAttributeNames={'#d': '_deleted', '#da': '_deletedAt'},
        ExpressionAttributeValues={':true': True, ':now': datetime.now(UTC).isoformat()},
    )
