"""
CDC Lambda Handler (Customer Contact) - Processes YFS_CUSTOMER_CONTACT CDC events from Kafka.

This Lambda:
1. Receives YFS_CUSTOMER_CONTACT CDC events via MSK
2. Caches contact info to DynamoDB
3. Writes CUSTOMER_CONTACT_BY_ORG_KEY record (keyed by buyerOrganizationCode)
4. Fan-out updates contact fields to matching organization documents in OpenSearch

DynamoDB Single Table Design:
- Table: enrichment_cache
- PK (Partition Key): entity_type
- SK (Sort Key): entity_id

Access Patterns:
- Get customer contact: PK=CUSTOMER_CONTACT, SK=<customer_key>#<customer_contact_key>
- Get contact by org:   PK=CUSTOMER_CONTACT_BY_ORG_KEY, SK=<buyer_org_code>
- Read customer:        PK=CUSTOMER, SK=<customer_key>  (to get buyerOrganizationCode)
"""

import base64
import hashlib
import json
import logging
import os

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

INDEX_ORGANIZATION  = os.environ.get('INDEX_ORGANIZATION', 'organizations')

# Entity type constants for single-table design
ENTITY_CUSTOMER_CONTACT            = 'CUSTOMER_CONTACT'
ENTITY_CUSTOMER                    = 'CUSTOMER'
ENTITY_CUSTOMER_CONTACT_BY_ORG_KEY = 'CUSTOMER_CONTACT_BY_ORG_KEY'

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

# Mirrors build_searchable_text() in cdc-lambda-organization.
# NOTE: customer.* fields are NOT written by this lambda — they are written by cdc-customer-processor.
# However, they must be READ here because searchableText is a flat denormalized string that gets
# completely overwritten. Without reading customer.* from the existing doc, those terms would be
# dropped from searchableText every time a contact update triggers this rebuild.
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

# Replaces the nested customerContact object on the org doc, then rebuilds searchableText.
PAINLESS_UPDATE_ORG_CUSTOMER_CONTACT_FIELDS = """
    if (params.customerContact == null) {
        ctx._source.remove('customerContact');
    } else {
        ctx._source.customerContact = params.customerContact;
    }
""" + PAINLESS_REBUILD_ORG_SEARCHABLE_TEXT


# ==========================================================
# Topic → Table Routing
# ==========================================================

TOPIC_TABLE_MAP = {
    'yfs_customer_contact': 'YFS_CUSTOMER_CONTACT',
}


def extract_table_from_topic(topic: str) -> str:
    """Extract table name from Debezium topic like 'glenraven-dev.STERLING.yfs_customer_contact'."""
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

    if 'YFS_CUSTOMER_CONTACT' in table:
        return process_customer_contact(payload, operation)
    else:
        logger.warning(f"Unknown table: {table}")
        return {'statusCode': 400, 'body': f'Unknown table: {table}'}


# =============================================================================
# Customer Contact Processing
# =============================================================================

def process_customer_contact(payload: dict, operation: str) -> dict:
    """
    Three-Phase Write for CustomerContact:
    - Phase 1: Cache contact info to DynamoDB (PK=CUSTOMER_CONTACT, SK=<customerKey>#<customerContactKey>)
               Also reads CUSTOMER record to obtain buyerOrganizationCode.
    - Phase 2: Write CUSTOMER_CONTACT_BY_ORG_KEY record (PK=CUSTOMER_CONTACT_BY_ORG_KEY, SK=<buyerOrganizationCode>)
    - Phase 3: Fan-out contact fields to matching organization document (nested under customerContact)

    Note: EVENING_FAX_N0 column (DDL typo — zero not O) is intentionally excluded.
    """
    customer_key         = get_value(payload, 'CUSTOMER_KEY')
    customer_contact_key = get_value(payload, 'CUSTOMER_CONTACT_KEY')
    logger.info(f"process_customer_contact | customer_key={customer_key} customer_contact_key={customer_contact_key} operation={operation}")

    if not customer_key:
        logger.warning(f"Missing CUSTOMER_KEY in customer contact. Payload keys: {list(payload.keys())}")
        return {'statusCode': 400, 'body': 'Missing CUSTOMER_KEY'}

    # Composite sort key: customerKey#customerContactKey
    composite_sk = f"{customer_key}#{customer_contact_key}"

    # ---------------------------------------------------------------
    # PHASE 1: Cache contact to DynamoDB + read CUSTOMER for buyerOrganizationCode
    # ---------------------------------------------------------------
    if operation == 'd':
        if ENABLE_HARD_DELETE:
            cache_table.delete_item(Key={'PK': ENTITY_CUSTOMER_CONTACT, 'SK': composite_sk})
            logger.info(f"Phase 1: Hard-deleted customer contact | sk={composite_sk}")
        else:
            soft_delete_cache_item(ENTITY_CUSTOMER_CONTACT, composite_sk)
            logger.info(f"Phase 1: Soft-deleted customer contact | sk={composite_sk}")
        contact_obj = None
    else:
        first_name   = get_value(payload, 'FIRST_NAME')
        middle_name  = get_value(payload, 'MIDDLE_NAME')
        last_name    = get_value(payload, 'LAST_NAME')
        contact_name = ' '.join(filter(None, [first_name, middle_name, last_name])) or None

        item = {
            'PK': ENTITY_CUSTOMER_CONTACT,
            'SK': composite_sk,
            'customerKey':         customer_key,
            'customerContactKey':  customer_contact_key,
            'contactName':         contact_name,
            'contactEmail':        get_value(payload, 'EMAILID'),
            'contactPhone':        (
                get_value(payload, 'MOBILE_PHONE') or
                get_value(payload, 'DAY_PHONE') or
                get_value(payload, 'EVENING_PHONE')
            ),
            'contactJobTitle':     get_value(payload, 'JOB_TITLE'),
            'contactDepartment':   get_value(payload, 'DEPARTMENT'),
            'contactType':         get_value(payload, 'EXTN_CONTACT_TYPE'),
            'contactCompany':      get_value(payload, 'COMPANY'),
            'contactDob':          get_value(payload, 'DOB'),
            'statementEmail':      get_value(payload, 'EXTN_STATEMENT_EMAIL'),
            'updatedAt':           datetime.now(UTC).isoformat()
        }
        item = {k: v for k, v in item.items() if v is not None}
        try:
            cache_table.put_item(Item=item)
            logger.info(f"Phase 1: DynamoDB write SUCCESS | sk={composite_sk}")
        except Exception as e:
            logger.error(f"Phase 1: DynamoDB write FAILED | sk={composite_sk} error={e}", exc_info=True)
            raise

        contact_obj = {
            'contactName':       item.get('contactName'),
            'contactEmail':      item.get('contactEmail'),
            'contactPhone':      item.get('contactPhone'),
            'contactJobTitle':   item.get('contactJobTitle'),
            'contactDepartment': item.get('contactDepartment'),
            'contactCompany':    item.get('contactCompany'),
            'contactDob':        item.get('contactDob'),
        }

    # Read CUSTOMER record to obtain buyerOrganizationCode
    buyer_org_code = get_buyer_org_code_from_customer(customer_key)

    # ---------------------------------------------------------------
    # PHASE 2: Write CUSTOMER_CONTACT_BY_ORG_KEY record
    # ---------------------------------------------------------------
    if buyer_org_code and operation != 'd':
        contact_hash = compute_customer_contact_hash(contact_obj)
        org_key_item = {
            'PK': ENTITY_CUSTOMER_CONTACT_BY_ORG_KEY,
            'SK': buyer_org_code,
            'customerKey':             customer_key,
            'customerContactKey':      customer_contact_key,
            'contactName':             contact_obj.get('contactName') if contact_obj else None,
            'contactEmail':            contact_obj.get('contactEmail') if contact_obj else None,
            'contactPhone':            contact_obj.get('contactPhone') if contact_obj else None,
            'contactJobTitle':         contact_obj.get('contactJobTitle') if contact_obj else None,
            'contactDepartment':       contact_obj.get('contactDepartment') if contact_obj else None,
            'contactCompany':          contact_obj.get('contactCompany') if contact_obj else None,
            'contactDob':              contact_obj.get('contactDob') if contact_obj else None,
            'contactType':             get_value(payload, 'EXTN_CONTACT_TYPE'),
            'statementEmail':          get_value(payload, 'EXTN_STATEMENT_EMAIL'),
            'customerContactHash':     contact_hash,
            'updatedAt':               datetime.now(UTC).isoformat()
        }
        org_key_item = {k: v for k, v in org_key_item.items() if v is not None}
        try:
            cache_table.put_item(Item=org_key_item)
            logger.info(f"Phase 2: CUSTOMER_CONTACT_BY_ORG_KEY write SUCCESS | org={buyer_org_code}")
        except Exception as e:
            logger.error(f"Phase 2: CUSTOMER_CONTACT_BY_ORG_KEY write FAILED | org={buyer_org_code} error={e}", exc_info=True)
            raise
    elif buyer_org_code and operation == 'd':
        try:
            if ENABLE_HARD_DELETE:
                cache_table.delete_item(Key={'PK': ENTITY_CUSTOMER_CONTACT_BY_ORG_KEY, 'SK': buyer_org_code})
                logger.info(f"Phase 2: Hard-deleted CUSTOMER_CONTACT_BY_ORG_KEY | org={buyer_org_code}")
            else:
                soft_delete_cache_item(ENTITY_CUSTOMER_CONTACT_BY_ORG_KEY, buyer_org_code)
                logger.info(f"Phase 2: Soft-deleted CUSTOMER_CONTACT_BY_ORG_KEY | org={buyer_org_code}")
        except Exception as e:
            logger.error(f"Phase 2: CUSTOMER_CONTACT_BY_ORG_KEY delete FAILED | org={buyer_org_code} error={e}", exc_info=True)
    elif not buyer_org_code:
        logger.info(f"Phase 2: Skipped — no buyerOrganizationCode found for customer_key={customer_key}")

    # ---------------------------------------------------------------
    # PHASE 3: Fan-out to org index (nested under customerContact)
    # ---------------------------------------------------------------
    try:
        client = get_opensearch_client()
        contacts_array = build_contacts_array(customer_key)
        
        result = client.update_by_query(
            index=INDEX_ORGANIZATION,
            body={
                "query": {"term": {"customer.customerKey": customer_key}},
                "script": {
                    "source": PAINLESS_UPDATE_ORG_CUSTOMER_CONTACT_FIELDS,
                    "lang":   "painless",
                    "params": {"customerContact": contacts_array}
                }
            },
            conflicts='proceed',
            refresh=True
        )
        updated = result.get('updated', 0)
        if updated > 0:
            logger.info(f"Phase 3: Updated {updated} org(s) with {len(contacts_array) if contacts_array else 0} contact(s) | key={customer_key}")
        else:
            logger.info(f"Phase 3: No org found for customerKey={customer_key}")
    except Exception as e:
        logger.warning(f"Phase 3 failed for customer contact | key={customer_key} error={e}")

    return {'statusCode': 200, 'body': f'Processed customer contact: {customer_key}'}


# =============================================================================
# DynamoDB Cache Lookups
# =============================================================================

def build_contacts_array(customer_key: str) -> Optional[list]:
    """
    Query all CUSTOMER_CONTACT records where SK begins_with <customerKey>#.
    Returns a list of contact objects, or None if no records found.
    """
    try:
        from boto3.dynamodb.conditions import Key as DKey
        response = cache_table.query(
            KeyConditionExpression=DKey('PK').eq(ENTITY_CUSTOMER_CONTACT) & DKey('SK').begins_with(f'{customer_key}#')
        )
        items = response.get('Items', [])
        if not items:
            return None

        contacts = []
        for item in items:
            if item.get('_deleted'):
                continue
            contact = {k: v for k, v in {
                'customerContactKey':  item.get('customerContactKey'),
                'contactName':         item.get('contactName'),
                'contactEmail':        item.get('contactEmail'),
                'contactPhone':        item.get('contactPhone'),
                'contactJobTitle':     item.get('contactJobTitle'),
                'contactDepartment':   item.get('contactDepartment'),
                'contactCompany':      item.get('contactCompany'),
                'contactDob':          item.get('contactDob'),
                'contactType':         item.get('contactType'),
                'statementEmail':      item.get('statementEmail'),
            }.items() if v is not None}
            contacts.append(contact)

        logger.info(f"build_contacts_array: Found {len(contacts)} contact(s) for key={customer_key}")
        return contacts

    except Exception as e:
        logger.warning(f"build_contacts_array FAILED | key={customer_key} error={e}")
        return None

def get_cached_customer_contact(composite_sk: str) -> Optional[dict]:
    """Fetch a cached customer contact by composite SK (customerKey#customerContactKey)."""
    try:
        response = cache_table.get_item(Key={'PK': ENTITY_CUSTOMER_CONTACT, 'SK': composite_sk})
        item = response.get('Item')
        if item and item.get('_deleted'):
            return None
        return {k: v for k, v in item.items() if k not in ('PK', 'SK', 'updatedAt', '_deleted', '_deletedAt')} if item else None
    except Exception as e:
        logger.warning(f"Error fetching customer contact | sk={composite_sk} error={e}")
        return None


def get_buyer_org_code_from_customer(customer_key: str) -> Optional[str]:
    """Read CUSTOMER record from DynamoDB to obtain buyerOrganizationCode."""
    try:
        response = cache_table.get_item(Key={'PK': ENTITY_CUSTOMER, 'SK': customer_key})
        item = response.get('Item')
        if item:
            org_code = item.get('buyerOrganizationCode')
            logger.info(f"get_buyer_org_code_from_customer: FOUND | key={customer_key} org={org_code}")
            return org_code
        logger.info(f"get_buyer_org_code_from_customer: NO ENTRY | key={customer_key}")
        return None
    except Exception as e:
        logger.warning(f"get_buyer_org_code_from_customer FAILED | key={customer_key} error={e}", exc_info=True)
        return None


def get_cached_customer_contact_by_org_key(buyer_org_code: str) -> Optional[dict]:
    """Fetch cached customer contact keyed by buyerOrganizationCode."""
    try:
        response = cache_table.get_item(Key={'PK': ENTITY_CUSTOMER_CONTACT_BY_ORG_KEY, 'SK': buyer_org_code})
        item = response.get('Item')
        if item and item.get('_deleted'):
            logger.info(f"get_cached_customer_contact_by_org_key: SOFT-DELETED | org={buyer_org_code}")
            return None
        if item:
            result = {k: v for k, v in item.items() if k not in ('PK', 'SK', 'updatedAt', '_deleted', '_deletedAt')}
            logger.info(f"get_cached_customer_contact_by_org_key: FOUND | org={buyer_org_code}")
            return result
        logger.info(f"get_cached_customer_contact_by_org_key: NO ENTRY | org={buyer_org_code}")
        return None
    except Exception as e:
        logger.warning(f"get_cached_customer_contact_by_org_key FAILED | org={buyer_org_code} error={e}", exc_info=True)
        return None


# =============================================================================
# Utility Functions
# =============================================================================

def compute_customer_contact_hash(contact_obj: Optional[dict]) -> str:
    """
    Stable 16-char hex hash of the customer contact fields that get fanned out.
    Used to skip fan-outs when nothing meaningful has changed.
    """
    if contact_obj is None:
        contact_obj = {}
    fields = {
        'contactName':       contact_obj.get('contactName'),
        'contactEmail':      contact_obj.get('contactEmail'),
        'contactPhone':      contact_obj.get('contactPhone'),
        'contactJobTitle':   contact_obj.get('contactJobTitle'),
        'contactDepartment': contact_obj.get('contactDepartment'),
        'contactCompany':    contact_obj.get('contactCompany'),
        'contactDob':        contact_obj.get('contactDob'),
    }
    canonical = json.dumps(fields, sort_keys=True)
    return hashlib.sha256(canonical.encode()).hexdigest()[:16]


def get_value(payload: dict, key: str) -> Optional[str]:
    """Get string value from payload, stripping trailing whitespace from Oracle CHAR fields."""
    value = payload.get(key)
    if value is None:
        return None
    s = str(value).strip()
    return s if s else None


def now_ms() -> int:
    return int(datetime.now(UTC).timestamp() * 1000)


def soft_delete_cache_item(pk: str, sk: str) -> None:
    """Soft-delete a DynamoDB cache row by marking it as deleted instead of removing it."""
    cache_table.update_item(
        Key={'PK': pk, 'SK': sk},
        UpdateExpression='SET #d = :true, #da = :now',
        ExpressionAttributeNames={'#d': '_deleted', '#da': '_deletedAt'},
        ExpressionAttributeValues={':true': True, ':now': datetime.now(UTC).isoformat()},
    )
