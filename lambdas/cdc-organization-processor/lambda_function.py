"""
CDC Lambda Handler (Organization) - Processes YFS_ORGANIZATION CDC events from Kafka.

This Lambda:
1. Receives YFS_ORGANIZATION CDC events from the Java CDC processor
2. Caches organization data to DynamoDB
3. Fan-out updates to all orders referencing this organization
4. Indexes organization to OpenSearch (enriched with address/contact/customer)

DynamoDB Single Table Design:
- Table: enrichment_cache
- PK (Partition Key): entity_type
- SK (Sort Key): entity_id

Access Patterns:
- Get organization: PK=ORGANIZATION, SK=<organization_key>
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
OPENSEARCH_AUTH     = os.environ.get('OPENSEARCH_AUTH', 'iam')  # 'iam' or 'basic'
OPENSEARCH_USERNAME = os.environ.get('OPENSEARCH_USERNAME', 'admin')
OPENSEARCH_PASSWORD = os.environ.get('OPENSEARCH_PASSWORD', 'admin')
OPENSEARCH_MODE     = os.environ.get('OPENSEARCH_MODE', 'serverless')  # 'serverless' or 'provisioned'
AWS_REGION          = os.environ.get('AWS_REGION', 'us-east-1')

INDEX_ORDERS       = os.environ.get('INDEX_ORDERS', 'orders')
INDEX_ORGANIZATION = os.environ.get('INDEX_ORGANIZATION', 'organizations')

# Entity type constants for single-table design
ENTITY_ORGANIZATION                = 'ORGANIZATION'
ENTITY_PERSON_INFO                 = 'PERSON_INFO'
ENTITY_CUSTOMER_BY_ORG_KEY         = 'CUSTOMER_BY_ORG_KEY'
ENTITY_CUSTOMER_CONTACT_BY_ORG_KEY = 'CUSTOMER_CONTACT_BY_ORG_KEY'
ENTITY_CUSTOMER_ADDNL_ADDRESS      = 'CUSTOMER_ADDNL_ADDRESS'
ENTITY_CUSTOMER_CONTACT            = 'CUSTOMER_CONTACT'

# Maps DynamoDB address type suffix → org doc nested field name
ADDRESS_TYPE_TO_FIELD = {'BillTo': 'billTo', 'ShipTo': 'shipTo', 'SoldTo': 'soldTo'}

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
    # Local development only
    dynamodb_kwargs['endpoint_url'] = DYNAMODB_ENDPOINT
    dynamodb_kwargs['aws_access_key_id'] = os.environ.get('AWS_ACCESS_KEY_ID', 'local')
    dynamodb_kwargs['aws_secret_access_key'] = os.environ.get('AWS_SECRET_ACCESS_KEY', 'local')

dynamodb    = boto3.resource('dynamodb', **dynamodb_kwargs)
cache_table = dynamodb.Table(DYNAMODB_TABLE)


# ==========================================================
# OpenSearch Client
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


# Used by all update_by_query fan-out operations to keep searchableText consistent.
PAINLESS_REBUILD_ORDER_SEARCHABLE_TEXT = """
    // Rebuild searchableText - mirrors build_searchable_text() in cdc-lambda-orderheader.
    // NOTE: prepend field-specific assignment before appending this script, e.g.:
    //   ctx._source.sellerOrganizationName = params.name;
    //   ctx._source.sellerOrgNameHash = params.nameHash;
    def parts = [];

    // Order identifiers
    if (ctx._source.orderNo != null) parts.add(ctx._source.orderNo);
    if (ctx._source.orderHeaderKey != null) parts.add(ctx._source.orderHeaderKey);
    if (ctx._source.enterpriseKey != null) parts.add(ctx._source.enterpriseKey);
    if (ctx._source.orderName != null) parts.add(ctx._source.orderName);

    // Customer object (nested under 'customer')
    if (ctx._source.customer != null) {
        if (ctx._source.customer.customerFirstName != null) parts.add(ctx._source.customer.customerFirstName);
        if (ctx._source.customer.customerLastName != null) parts.add(ctx._source.customer.customerLastName);
        if (ctx._source.customer.customerEmailId != null) parts.add(ctx._source.customer.customerEmailId);
        if (ctx._source.customer.customerPhoneNo != null) parts.add(ctx._source.customer.customerPhoneNo);
        if (ctx._source.customer.customerPoNo != null) parts.add(ctx._source.customer.customerPoNo);
        if (ctx._source.customer.organizationCode != null) parts.add(ctx._source.customer.organizationCode);
        if (ctx._source.customer.organizationName != null) parts.add(ctx._source.customer.organizationName);
    }

    // Organization names and codes
    if (ctx._source.sellerOrganizationName != null) parts.add(ctx._source.sellerOrganizationName);
    if (ctx._source.buyerOrganizationName != null) parts.add(ctx._source.buyerOrganizationName);
    if (ctx._source.sellerOrganizationCode != null) parts.add(ctx._source.sellerOrganizationCode);
    if (ctx._source.buyerOrganizationCode != null) parts.add(ctx._source.buyerOrganizationCode);

    // Order status
    if (ctx._source.orderStatus != null) parts.add(ctx._source.orderStatus);

    // BillTo nested object
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

    // ShipTo nested object
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

    // SoldTo nested object
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

# Topic name to table name mapping
TOPIC_TABLE_MAP = {
    'yfs_organization': 'YFS_ORGANIZATION',
}


def extract_table_from_topic(topic: str) -> str:
    """Extract table name from Debezium topic like 'sterling.public.yfs_organization'."""
    table_part = topic.rsplit('.', 1)[-1].lower()
    return TOPIC_TABLE_MAP.get(table_part, table_part.upper())


# ==========================================================
# Lambda Handler
# ==========================================================

def lambda_handler(event: dict, context: Any) -> dict:
    """
    Main Lambda handler - supports both MSK trigger and direct invocation.

    MSK trigger event structure:
    {
        "eventSource": "aws:kafka",
        "records": {
            "sterling.public.yfs_organization-0": [
                {
                    "topic": "sterling.public.yfs_organization",
                    "value": "<base64-encoded Debezium JSON>"
                }
            ]
        }
    }

    Debezium payload (after base64 decode):
    {
        "before": null,
        "after": { ...column values... },
        "source": { "table": "yfs_organization", ... },
        "op": "c" | "u" | "d" | "r",
        "ts_ms": 1234567890123
    }
    """
    try:
        logger.info(f"Incoming event sample: {json.dumps(event, default=str)[:5000]}")

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
    1. Flattened SMT format (schema/payload wrapper, columns + __op/__source_ts_ms in payload)
       - Produced when Debezium ExtractField SMT is applied
       - { "schema": {...}, "payload": { "COL": val, "__op": "r", "__source_ts_ms": ... } }
    2. Standard Debezium envelope (no schema wrapper)
       - { "op": "c", "ts_ms": ..., "before": null, "after": { "COL": val } }
    """
    if 'schema' in debezium_event and 'payload' in debezium_event:
        inner = debezium_event['payload']
        if inner is None:
            return 'ignore', None, {}
        if '__op' in inner:
            # Flattened SMT: column data lives directly in payload alongside __op/__source_ts_ms
            operation        = inner.get('__op') or 'c'
            source_timestamp = inner.get('__source_ts_ms')
            payload          = {k: v for k, v in inner.items() if not k.startswith('__')}
            return operation, source_timestamp, payload

    # Standard Debezium envelope
    operation        = debezium_event.get('op', 'c')
    source_timestamp = debezium_event.get('ts_ms')
    if operation == 'd':
        payload = debezium_event.get('before', {}) or {}
    else:
        payload = debezium_event.get('after', {}) or {}
    return operation, source_timestamp, payload


def process_msk_event(event: dict) -> dict:
    """Process batch of MSK records from event source mapping."""
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
                    f"Record decoded | table={table} topic={topic} offset={offset} operation={operation}"
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
                    f"FAILED record | topic={topic} offset={offset} error_type={type(e).__name__} error={e}",
                    exc_info=True
                )

    logger.info(f"Batch complete | processed={processed} errors={errors}")
    return {"statusCode": 200 if errors == 0 else 207, "body": f"Processed {processed}, errors {errors}"}

# ==========================================================
# Event Router
# ==========================================================

def process_single_event(table: str, operation: str, payload: dict, source_timestamp=None) -> dict:
    """Route a single CDC event to the appropriate processor."""
    logger.info(f"Processing {operation} event for table {table}")

    if 'YFS_ORGANIZATION' in table:
        return process_organization(payload, operation, source_timestamp)
    else:
        logger.warning(f"Unknown table: {table}")
        return {'statusCode': 400, 'body': f'Unknown table: {table}'}


# =============================================================================
# Organization Processing
# =============================================================================

def process_organization(payload: dict, operation: str, source_timestamp=None) -> dict:
    """
    Three-Phase Write for Organization:
    - Phase 1: Cache organization to DynamoDB (including address keys)
    - Phase 2: Fan-out update to all orders referencing this Organization
    - Phase 3: Index organization to OpenSearch (enriched with address/contact)

    Stores: PK=ORGANIZATION, SK=<organization_key>

    We use ORGANIZATION_KEY as SK because orders reference orgs by ORGANIZATION_KEY
    (BUYER_ORGANIZATION_CODE, SELLER_ORGANIZATION_CODE columns in YFS_ORDER_HEADER
    store ORGANIZATION_KEY values, not ORGANIZATION_CODE values).
    """
    start_total = time.perf_counter()
    organization_code = get_value(payload, 'ORGANIZATION_CODE')
    organization_key  = get_value(payload, 'ORGANIZATION_KEY')
    logger.info(
        f"process_organization | key={organization_key} code={organization_code} operation={operation}"
    )

    if not organization_key:
        logger.warning(f"Missing ORGANIZATION_KEY. Payload keys: {list(payload.keys())}")
        return {'statusCode': 400, 'body': 'Missing ORGANIZATION_KEY'}

    organization_name     = get_value(payload, 'ORGANIZATION_NAME')
    corporate_address_key = get_value(payload, 'CORPORATE_ADDRESS_KEY')
    contact_address_key   = get_value(payload, 'CONTACT_ADDRESS_KEY')
    new_name_hash         = compute_org_name_hash(organization_name)

    # PHASE 1: Update DynamoDB cache
    phase1_start = time.perf_counter()
    if operation == 'd':
        if ENABLE_HARD_DELETE:
            cache_table.delete_item(Key={'PK': ENTITY_ORGANIZATION, 'SK': organization_key})
            logger.info(f"Phase 1: Hard-deleted organization from cache | key={organization_key}")
        else:
            soft_delete_cache_item(ENTITY_ORGANIZATION, organization_key)
            logger.info(f"Phase 1: Soft-deleted organization from cache | key={organization_key}")
        name_changed = True
    else:
        # Check existing hash to decide whether fan-out is needed
        existing_hash = get_cached_org_name_hash(organization_key)
        name_changed  = existing_hash != new_name_hash
        logger.info(
            f"Phase 1: Hash check | key={organization_key} existing={existing_hash} "
            f"new={new_name_hash} name_changed={name_changed}"
        )

        item = {
            'PK': ENTITY_ORGANIZATION,
            'SK': organization_key,
            'organizationKey':          organization_key,
            'organizationCode':         organization_code,
            'organizationName':         organization_name,
            'orgNameHash':              new_name_hash,
            'parentOrganizationCode':   get_value(payload, 'PARENT_ORGANIZATION_CODE'),
            'primaryEnterpriseKey':     get_value(payload, 'PRIMARY_ENTERPRISE_KEY'),
            'corporateAddressKey':      corporate_address_key,
            'contactAddressKey':        contact_address_key,
            'updatedAt':                datetime.now(UTC).isoformat()
        }
        item = {k: v for k, v in item.items() if v is not None}
        logger.info(f"Phase 1: DynamoDB write | PK={ENTITY_ORGANIZATION} SK={organization_key} name={organization_name}")
        try:
            cache_table.put_item(Item=item)
            logger.info(f"Phase 1: DynamoDB write SUCCESS | key={organization_key}")
        except Exception as e:
            logger.error(f"Phase 1: DynamoDB write FAILED | key={organization_key} error={e}", exc_info=True)
            raise

    phase1_ms = (time.perf_counter() - phase1_start) * 1000
    logger.info(f"TIMING phase1_dynamodb | key={organization_key} duration_ms={phase1_ms:.1f}")

    # PHASE 2: Fan-out update to all orders referencing this Organization.
    # Only triggered when the org name actually changed.
    if name_changed:
        update_orders_with_organization(organization_key, organization_name, operation, new_name_hash)
    else:
        logger.info(f"Phase 2: Skipped — name unchanged | key={organization_key}")

    # PHASE 3: Index organization to OpenSearch
    index_organization(organization_code, organization_key, organization_name, payload, operation,
                       corporate_address_key, contact_address_key, source_timestamp)

    total_ms = (time.perf_counter() - start_total) * 1000
    logger.info(f"TIMING process_organization_total | key={organization_key} operation={operation} duration_ms={total_ms:.1f}")

    return {'statusCode': 200, 'body': f'Processed organization: {organization_key}'}


def update_orders_with_organization(
        organization_key: str,
        organization_name: Optional[str],
        operation: str,
        name_hash: str
) -> None:
    """
    Phase 2: Fan-out organization name change to all referencing order documents.

    Only called when the org name hash has changed.

    Query strategy:
    - Match orders where the org key equals this org's key AND the stored hash differs
      from the new hash — this skips docs that are already up to date.
    - The Painless script writes the new name AND the new hash so subsequent fan-outs
      are even cheaper.

    Field paths in the order document (from cdc-lambda-orderheader):
    - seller: top-level sellerOrganizationCode / sellerOrganizationName / sellerOrgNameHash
    - buyer:  top-level buyerOrganizationCode  / buyerOrganizationName  / buyerOrgNameHash
    - customer org: nested customer.organizationCode / customer.organizationName / customer.orgNameHash
    """
    try:
        start_phase2 = time.perf_counter()
        client   = get_opensearch_client()
        new_name = None if operation == 'd' else organization_name

        result_seller = client.update_by_query(
            index=INDEX_ORDERS,
            body={
                "query": {
                    "bool": {
                        "must": {"term": {"sellerOrganizationCode": organization_key}},
                        "must_not": {"term": {"sellerOrgNameHash": name_hash}}
                    }
                },
                "script": {
                    "source": (
                        "ctx._source.sellerOrganizationName = params.name;"
                        "ctx._source.sellerOrgNameHash = params.nameHash;"
                        + PAINLESS_REBUILD_ORDER_SEARCHABLE_TEXT
                    ),
                    "lang": "painless",
                    "params": {"name": new_name, "nameHash": name_hash}
                }
            },
            conflicts='proceed',
            refresh=False
        )

        result_buyer = client.update_by_query(
            index=INDEX_ORDERS,
            body={
                "query": {
                    "bool": {
                        "must": {"term": {"buyerOrganizationCode": organization_key}},
                        "must_not": {"term": {"buyerOrgNameHash": name_hash}}
                    }
                },
                "script": {
                    "source": (
                        "ctx._source.buyerOrganizationName = params.name;"
                        "ctx._source.buyerOrgNameHash = params.nameHash;"
                        + PAINLESS_REBUILD_ORDER_SEARCHABLE_TEXT
                    ),
                    "lang": "painless",
                    "params": {"name": new_name, "nameHash": name_hash}
                }
            },
            conflicts='proceed',
            refresh=False
        )

        # customer org is nested under the 'customer' object in the order document
        result_customer = client.update_by_query(
            index=INDEX_ORDERS,
            body={
                "query": {
                    "bool": {
                        "must": {"term": {"customer.organizationCode": organization_key}},
                        "must_not": {"term": {"customer.orgNameHash": name_hash}}
                    }
                },
                "script": {
                    "source": (
                        "ctx._source.customer.organizationName = params.name;"
                        "ctx._source.customer.orgNameHash = params.nameHash;"
                        + PAINLESS_REBUILD_ORDER_SEARCHABLE_TEXT
                    ),
                    "lang": "painless",
                    "params": {"name": new_name, "nameHash": name_hash}
                }
            },
            conflicts='proceed',
            refresh=False
        )

        seller_updated   = result_seller.get('updated', 0)
        buyer_updated    = result_buyer.get('updated', 0)
        customer_updated = result_customer.get('updated', 0)
        total_updated    = seller_updated + buyer_updated + customer_updated

        elapsed_ms = (time.perf_counter() - start_phase2) * 1000
        if total_updated > 0:
            logger.info(
                f"Phase 2: Orders fan-out | key={organization_key} updated={total_updated} "
                f"seller={seller_updated} buyer={buyer_updated} customer={customer_updated} duration_ms={elapsed_ms:.1f}"
            )
        else:
            logger.info(
                f"Phase 2: Orders fan-out up to date | key={organization_key} nameHash={name_hash} duration_ms={elapsed_ms:.1f}"
            )

    except Exception as e:
        elapsed_ms = (time.perf_counter() - start_phase2) * 1000
        status_code = getattr(e, 'status_code', None)
        error_info  = getattr(e, 'info', getattr(e, 'body', ''))
        logger.warning(
            f"Phase 2 failed for Organization | key={organization_key} status={status_code} error={e} info={error_info} duration_ms={elapsed_ms:.1f}"
        )


def index_organization(organization_code: str, organization_key: Optional[str],
                       organization_name: Optional[str], payload: dict, operation: str,
                       corporate_address_key: Optional[str], contact_address_key: Optional[str],
                       source_timestamp=None) -> None:
    """
    Phase 3: Index organization to OpenSearch, enriched with corporate address and contact info.

    Enrichment chain:
    - Organization → CORPORATE_ADDRESS_KEY → PersonInfo (address fields)
    - Organization → CONTACT_ADDRESS_KEY → PersonInfo (contact name/email/phone)
    - Organization → BUYER_ORGANIZATION_CODE → Customer (customer fields)
    """
    try:
        start_phase3 = time.perf_counter()
        client = get_opensearch_client()

        if operation == 'd':
            if ENABLE_HARD_DELETE:
                client.delete(index=INDEX_ORGANIZATION, id=organization_code, ignore=[404])
                elapsed_ms = (time.perf_counter() - start_phase3) * 1000
                logger.info(f"Phase 3: Hard-deleted organization from OpenSearch | id={organization_code} duration_ms={elapsed_ms:.1f}")
            else:
                soft_delete_os_doc(client, INDEX_ORGANIZATION, organization_code)
                elapsed_ms = (time.perf_counter() - start_phase3) * 1000
                logger.info(f"Phase 3: Soft-deleted organization from OpenSearch | id={organization_code} duration_ms={elapsed_ms:.1f}")
            return

        org_doc = {
            'organizationKey':           organization_key,
            'organizationCode':          organization_code,
            'organizationName':          organization_name,
            'parentOrganizationCode':    get_value(payload, 'PARENT_ORGANIZATION_CODE'),
            'xrefOrganizationCode':      get_value(payload, 'XREF_ORGANIZATION_CODE'),
            'resourceIdentifier':        get_value(payload, 'RESOURCE_IDENTIFIER'),
            'isNode':                    get_value(payload, 'IS_NODE'),
            'defaultDistributionRuleId': get_value(payload, 'DEFAULT_DISTRIBUTION_RULE_ID'),
            'authorityType':             get_value(payload, 'AUTHORITY_TYPE'),
            'corporateAddressKey':       corporate_address_key,
            'contactAddressKey':         contact_address_key,
            'billingAddressKey':         get_value(payload, 'BILLING_ADDRESS_KEY'),
            'primaryEnterpriseKey':      get_value(payload, 'PRIMARY_ENTERPRISE_KEY'),
            'dunsNumber':                get_value(payload, 'DUNS_NUMBER'),
            'primaryUrl':                get_value(payload, 'PRIMARY_URL'),
            'activateFlag':              get_value(payload, 'ACTIVATE_FLAG'),
            'isEnterprise':              get_value(payload, 'IS_ENTERPRISE'),
            'isHubOrganization':         get_value(payload, 'IS_HUB_ORGANIZATION'),
            'isBuyer':                   get_value(payload, 'IS_BUYER'),
            'isSeller':                  get_value(payload, 'IS_SELLER'),
            'isCarrier':                 get_value(payload, 'IS_CARRIER'),
            'isLegalEntity':             get_value(payload, 'IS_LEGAL_ENTITY'),
            'taxExemptFlag':             get_value(payload, 'TAX_EXEMPT_FLAG'),
            'taxpayerId':                get_value(payload, 'TAXPAYER_ID'),
            'taxExemptionCertificate':   get_value(payload, 'TAX_EXEMPTION_CERTIFICATE'),
            'taxJurisdiction':           get_value(payload, 'TAX_JURISDICTION'),
            'primarySicCode':            get_value(payload, 'PRIMARY_SIC_CODE'),
            'accountWithHub':            get_value(payload, 'ACCOUNT_WITH_HUB'),
            'catalogOrganizationCode':   get_value(payload, 'CATALOG_ORGANIZATION_CODE'),
            'inventoryOrganizationCode': get_value(payload, 'INVENTORY_ORGANIZATION_CODE'),
            'pricingOrganizationCode':   get_value(payload, 'PRICING_ORGANIZATION_CODE'),
            'defaultFulfillmentType':    get_value(payload, 'DEFAULT_FULFILLMENT_TYPE'),
            'defaultPriceProgramKey':    get_value(payload, 'DEFAULT_PRICE_PROGRAM_KEY'),
            'createdTimestamp':          get_timestamp_value(payload, 'CREATETS'),
            'modifiedTimestamp':         get_timestamp_value(payload, 'MODIFYTS'),
            # GlenRaven EXTN_* fields
            'extnPreferredDc':           get_value(payload, 'EXTN_PREFERRED_DC'),
            'extnNaftaRequired':         get_value(payload, 'EXTN_NAFTA_REQUIRED'),
            'extnSalesPerson':           get_value(payload, 'EXTN_SALES_PERSON'),
            'extnPaymentTerms':          get_value(payload, 'EXTN_PAYMENT_TERMS'),
            'extnPaymentTermDesc':       get_value(payload, 'EXTN_PAYMENT_TERM_DESC'),
            'extnInternalNote':          get_value(payload, 'EXTN_INTERNAL_NOTE'),
            'extnExternalPrintNote':     get_value(payload, 'EXTN_EXTERNAL_PRINT_NOTE'),
            'extnDefaultCarrier':        get_value(payload, 'EXTN_DEFAULT_CARRIER'),
            'extnDefaultCarrierService': get_value(payload, 'EXTN_DEFAULT_CARRIER_SERVICE'),
            'extnDefaultFreightTerms':   get_value(payload, 'EXTN_DEFAULT_FREIGHT_TERMS'),
            'extnAplusCustomerId':       get_value(payload, 'EXTN_APLUS_CUSTOMER_ID'),
            'extnRemitToId':             get_value(payload, 'EXTN_REMIT_TO_ID'),
            'extnNotificationType':      get_value(payload, 'EXTN_NOTIFICATION_TYPE'),
            'extnPbCarrierCode':         get_value(payload, 'EXTN_PB_CARRIER_CODE'),
            'extnFob':                   get_value(payload, 'EXTN_FOB'),
            'extnShippingPreference':    get_value(payload, 'EXTN_SHIPPING_PREFERENCE'),
            'extnMarketSegment':         get_value(payload, 'EXTN_MARKET_SEGMENT'),
            'extnThresholdPercentage':   get_value(payload, 'EXTN_THRESHOLD_PERCENTAGE'),
            'extnTimeLimit':             get_value(payload, 'EXTN_TIME_LIMIT'),
            'extnExportCustomer':        get_value(payload, 'EXTN_EXPORT_CUSTOMER'),
            'extnImportVendor':          get_value(payload, 'EXTN_IMPORT_VENDOR'),
            'extnInsideSalesRep':        get_value(payload, 'EXTN_INSIDE_SALES_REP'),
            'cdcOperation': operation,
            'cdcTimestamp': source_timestamp,
            'indexedAt':    now_ms()
        }

        # Enrich with corporate address (nested — full person + address from PersonInfo)
        if corporate_address_key:
            address_info = get_cached_person_info(corporate_address_key)
            if address_info:
                first_name  = address_info.get('firstName')
                middle_name = address_info.get('middleName')
                last_name   = address_info.get('lastName')
                org_doc['address'] = {k: v for k, v in {
                    'firstName':     first_name,
                    'lastName':      last_name,
                    'fullName':      ' '.join(filter(None, [first_name, middle_name, last_name])) or None,
                    'emailId':       address_info.get('emailId'),
                    'dayPhone':      address_info.get('dayPhone'),
                    'eveningPhone':  address_info.get('eveningPhone'),
                    'mobilePhone':   address_info.get('mobilePhone'),
                    'company':       address_info.get('company'),
                    'addressLine1':  address_info.get('addressLine1'),
                    'addressLine2':  address_info.get('addressLine2'),
                    'addressLine3':  address_info.get('addressLine3'),
                    'addressLine4':  address_info.get('addressLine4'),
                    'addressLine5':  address_info.get('addressLine5'),
                    'addressLine6':  address_info.get('addressLine6'),
                    'city':          address_info.get('city'),
                    'state':         address_info.get('state'),
                    'zipCode':       address_info.get('zipCode'),
                    'country':       address_info.get('country'),
                    'addressHash':   address_info.get('addressHash'),
                }.items() if v is not None}

        # Enrich with contact person (nested — full person + address from PersonInfo)
        if contact_address_key:
            contact_info = get_cached_person_info(contact_address_key)
            if contact_info:
                first_name  = contact_info.get('firstName')
                middle_name = contact_info.get('middleName')
                last_name   = contact_info.get('lastName')
                org_doc['contact'] = {k: v for k, v in {
                    'firstName':     first_name,
                    'lastName':      last_name,
                    'fullName':      build_full_name(contact_info),
                    'email':         contact_info.get('emailId'),
                    'phone':         get_primary_phone(contact_info),
                    'dayPhone':      contact_info.get('dayPhone'),
                    'eveningPhone':  contact_info.get('eveningPhone'),
                    'mobilePhone':   contact_info.get('mobilePhone'),
                    'company':       contact_info.get('company'),
                    'addressLine1':  contact_info.get('addressLine1'),
                    'addressLine2':  contact_info.get('addressLine2'),
                    'addressLine3':  contact_info.get('addressLine3'),
                    'addressLine4':  contact_info.get('addressLine4'),
                    'addressLine5':  contact_info.get('addressLine5'),
                    'addressLine6':  contact_info.get('addressLine6'),
                    'city':          contact_info.get('city'),
                    'state':         contact_info.get('state'),
                    'zipCode':       contact_info.get('zipCode'),
                    'country':       contact_info.get('country'),
                    'contactHash':   contact_info.get('contactHash'),
                }.items() if v is not None}

        # Enrich with customer fields (nested, look up by organization_key = BUYER_ORGANIZATION_CODE)
        if organization_key:
            logger.info(f"Phase 3: Looking up customer by org key | PK={ENTITY_CUSTOMER_BY_ORG_KEY} SK={organization_key}")
            customer_data = get_cached_customer_by_org_key(organization_key)
            if customer_data:
                org_doc['customer'] = {k: v for k, v in {
                    'customerId':                    customer_data.get('customerId'),
                    'customerKey':                   customer_data.get('customerKey'),
                    'customerType':                  customer_data.get('customerType'),
                    'customerStatus':                customer_data.get('customerStatus'),
                    'customerClassificationCode':    customer_data.get('customerClassificationCode'),
                    'externalCustomerId':            customer_data.get('externalCustomerId'),
                    'customerVertical':              customer_data.get('customerVertical'),
                    'customerLevel':                 customer_data.get('customerLevel'),
                    'customerRelationshipType':      customer_data.get('customerRelationshipType'),
                    'customerRegisteredDate':        customer_data.get('customerRegisteredDate'),
                    'customerRewardsNo':             customer_data.get('customerRewardsNo'),
                    'extnEligiblePaymentMethods':    customer_data.get('extnEligiblePaymentMethods'),
                    'extnPartnershipLevel':          customer_data.get('extnPartnershipLevel'),
                    'extnPartnershipExpirationDate': customer_data.get('extnPartnershipExpirationDate'),
                    'extnPartnershipStartDate':      customer_data.get('extnPartnershipStartDate'),
                    'extnAutoRenewPartnership':      customer_data.get('extnAutoRenewPartnership'),
                    'extnNonPfasRequired':           customer_data.get('extnNonPfasRequired'),
                    'customerHash':                  customer_data.get('customerHash'),
                }.items() if v is not None}
                logger.info(
                    f"Phase 3: Enriched org with customer | "
                    f"org={organization_code} customerId={customer_data.get('customerId')}"
                )
            else:
                logger.info(
                    f"Phase 3: No customer found in cache | org_key={organization_key} "
                    f"(will be set via fan-out when customer event arrives)"
                )
        else:
            logger.warning(f"Phase 3: organization_key is None for org {organization_code}, cannot look up customer")

        # Enrich with billTo/shipTo/soldTo using customerKey from customer_data (already fetched above)
        if customer_data:
            customer_key = customer_data.get('customerKey')
            if customer_key:
                logger.info(f"Phase 3: Enriching addresses via customerKey={customer_key} | org_key={organization_key}")
                enrich_org_with_addresses(org_doc, customer_key)
                enrich_org_with_contacts(org_doc, customer_key)
            else:
                logger.info(f"Phase 3: customer_data has no customerKey | org_key={organization_key}")
        else:
            logger.info(
                f"Phase 3: No customer_data found | org_key={organization_key} "
                f"(billTo/shipTo/soldTo will be set when customer event arrives)"
            )

        # Build searchable text
        customer = org_doc.get('customer', {})
        contact  = org_doc.get('contact', {})
        bill_to  = org_doc.get('billTo', {})
        ship_to  = org_doc.get('shipTo', {})
        sold_to  = org_doc.get('soldTo', {})

        customer_contacts = org_doc.get('customerContact', [])
        contact_terms = []
        for c in customer_contacts:
            contact_terms.extend([c.get('contactName'), c.get('contactEmail'), c.get('contactPhone')])

        org_doc['searchableText'] = ' '.join(filter(None, [
            org_doc.get('organizationCode'),
            org_doc.get('organizationName'),
            customer.get('customerId'),
            customer.get('customerRewardsNo'),
            org_doc.get('extnAplusCustomerId'),
            contact.get('fullName'),
            contact.get('email'),
            contact.get('phone'),
            *contact_terms,
            bill_to.get('fullName'),
            bill_to.get('emailId'),
            bill_to.get('company'),
            ship_to.get('fullName'),
            ship_to.get('emailId'),
            ship_to.get('company'),
            sold_to.get('fullName'),
            sold_to.get('emailId'),
            sold_to.get('company'),
            org_doc.get('extnSalesPerson'),
            org_doc.get('extnInsideSalesRep'),
            org_doc.get('extnMarketSegment'),
        ]))

        org_doc = {k: v for k, v in org_doc.items() if v is not None}
        # Explicitly clear soft-delete fields so a subsequent create/update "undeletes" the doc
        org_doc['_deleted'] = False
        org_doc['_deletedAt'] = None
        
        logger.info(
            f"Phase 3: Indexing org | index={INDEX_ORGANIZATION} id={organization_code} "
            f"cdcTimestamp={org_doc.get('cdcTimestamp')} indexedAt={org_doc.get('indexedAt')}"
        )
        resp = client.update(
            index=INDEX_ORGANIZATION,
            id=organization_code,
            body={"doc": org_doc, "doc_as_upsert": True},
            refresh=True
        )
        elapsed_ms = (time.perf_counter() - start_phase3) * 1000
        logger.info(f"Phase 3: OpenSearch response: {resp} duration_ms={elapsed_ms:.1f}")

    except Exception as e:
        elapsed_ms = (time.perf_counter() - start_phase3) * 1000
        logger.error(f"Phase 3 FAILED for Organization | id={organization_code} error={e} duration_ms={elapsed_ms:.1f}", exc_info=True)


# =============================================================================
# DynamoDB Cache Lookups
# =============================================================================

def get_cached_person_info(person_info_key: str) -> Optional[dict]:
    """Retrieve person info from DynamoDB cache."""
    try:
        response = cache_table.get_item(Key={'PK': ENTITY_PERSON_INFO, 'SK': person_info_key})
        item = response.get('Item')
        if item and item.get('_deleted'):
            return None
        if item:
            return {k: v for k, v in item.items() if k not in ('PK', 'SK', 'updatedAt', '_deleted', '_deletedAt')}
        return None
    except Exception as e:
        logger.warning(f"Error fetching person info | key={person_info_key} error={e}")
        return None


def get_cached_customer_by_org_key(buyer_org_code: str) -> Optional[dict]:
    """Retrieve customer fields from DynamoDB cache by buyer organisation code."""
    try:
        response = cache_table.get_item(
            Key={'PK': ENTITY_CUSTOMER_BY_ORG_KEY, 'SK': buyer_org_code}
        )
        item = response.get('Item')
        if item and item.get('_deleted'):
            logger.info(f"get_cached_customer_by_org_key: SOFT-DELETED | org_key={buyer_org_code}")
            return None
        if item:
            result = {k: v for k, v in item.items() if k not in ('PK', 'SK', 'updatedAt', '_deleted', '_deletedAt')}
            logger.info(
                f"get_cached_customer_by_org_key: FOUND | org_key={buyer_org_code} "
                f"customerId={result.get('customerId')}"
            )
            return result
        logger.info(f"get_cached_customer_by_org_key: NO ENTRY | PK={ENTITY_CUSTOMER_BY_ORG_KEY} SK={buyer_org_code}")
        return None
    except Exception as e:
        logger.warning(f"get_cached_customer_by_org_key FAILED | key={buyer_org_code} error={e}", exc_info=True)
        return None


def get_cached_customer_contact_by_org_key(org_key: str) -> Optional[dict]:
    """Retrieve customer contact record from DynamoDB cache by organization key (reverse lookup)."""
    try:
        response = cache_table.get_item(
            Key={'PK': ENTITY_CUSTOMER_CONTACT_BY_ORG_KEY, 'SK': org_key}
        )
        item = response.get('Item')
        if item and item.get('_deleted'):
            logger.info(f"get_cached_customer_contact_by_org_key: SOFT-DELETED | org_key={org_key}")
            return None
        if item:
            result = {k: v for k, v in item.items() if k not in ('PK', 'SK', 'updatedAt', '_deleted', '_deletedAt')}
            logger.info(
                f"get_cached_customer_contact_by_org_key: FOUND | org_key={org_key} "
                f"customerKey={result.get('customerKey')}"
            )
            return result
        logger.info(f"get_cached_customer_contact_by_org_key: NO ENTRY | PK={ENTITY_CUSTOMER_CONTACT_BY_ORG_KEY} SK={org_key}")
        return None
    except Exception as e:
        logger.warning(f"get_cached_customer_contact_by_org_key FAILED | org_key={org_key} error={e}", exc_info=True)
        return None


def enrich_org_with_addresses(org_doc: dict, customer_key: str) -> None:
    """
    Query CUSTOMER_ADDNL_ADDRESS records for the given customerKey,
    look up PersonInfo for each, and populate billTo/shipTo/soldTo on the org doc.
    """
    try:
        customer_key = str(customer_key).strip()
        from boto3.dynamodb.conditions import Key as DKey
        response = cache_table.query(
            KeyConditionExpression=DKey('PK').eq(ENTITY_CUSTOMER_ADDNL_ADDRESS) & DKey('SK').begins_with(f'{customer_key}#')
        )
        items = response.get('Items', [])
        if not items:
            logger.info(f"Phase 3: No CUSTOMER_ADDNL_ADDRESS records for customerKey={customer_key}")
            return

        for item in items:
            if item.get('_deleted'):
                continue
            sk = item.get('SK', '')
            address_type = sk.split('#', 1)[1] if '#' in sk else None
            field_name = ADDRESS_TYPE_TO_FIELD.get(address_type)
            if not field_name:
                logger.info(f"Phase 3: Unknown address type '{address_type}' | sk={sk}, skipping")
                continue

            person_info_key = item.get('personInfoKey')
            if not person_info_key:
                logger.info(f"Phase 3: No personInfoKey for {address_type} | sk={sk}")
                continue

            person_info = get_cached_person_info(person_info_key)
            if not person_info:
                logger.info(f"Phase 3: PersonInfo not in cache for {address_type} | key={person_info_key}")
                continue

            first_name  = person_info.get('firstName')
            middle_name = person_info.get('middleName')
            last_name   = person_info.get('lastName')
            org_doc[field_name] = {k: v for k, v in {
                'personInfoKey': person_info_key,
                'firstName':     first_name,
                'lastName':      last_name,
                'fullName':      ' '.join(filter(None, [first_name, middle_name, last_name])) or None,
                'emailId':       person_info.get('emailId'),
                'dayPhone':      person_info.get('dayPhone'),
                'eveningPhone':  person_info.get('eveningPhone'),
                'mobilePhone':   person_info.get('mobilePhone'),
                'company':       person_info.get('company'),
                'addressLine1':  person_info.get('addressLine1'),
                'addressLine2':  person_info.get('addressLine2'),
                'addressLine3':  person_info.get('addressLine3'),
                'addressLine4':  person_info.get('addressLine4'),
                'addressLine5':  person_info.get('addressLine5'),
                'addressLine6':  person_info.get('addressLine6'),
                'city':          person_info.get('city'),
                'state':         person_info.get('state'),
                'zipCode':       person_info.get('zipCode'),
                'country':       person_info.get('country'),
            }.items() if v is not None}
            logger.info(f"Phase 3: Enriched org with {field_name} | customerKey={customer_key} personInfoKey={person_info_key}")

    except Exception as e:
        logger.warning(f"Phase 3: enrich_org_with_addresses FAILED | customerKey={customer_key} error={e}", exc_info=True)


def enrich_org_with_contacts(org_doc: dict, customer_key: str) -> None:
    """
    Query CUSTOMER_CONTACT records for the given customerKey and populate customerContact array on the org doc.
    """
    try:
        customer_key = str(customer_key).strip()
        from boto3.dynamodb.conditions import Key as DKey
        response = cache_table.query(
            KeyConditionExpression=DKey('PK').eq(ENTITY_CUSTOMER_CONTACT) & DKey('SK').begins_with(f'{customer_key}#')
        )
        items = response.get('Items', [])
        if not items:
            logger.info(f"Phase 3: No CUSTOMER_CONTACT records for customerKey={customer_key}")
            return

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

        org_doc['customerContact'] = contacts
        logger.info(f"Phase 3: Enriched org with {len(contacts)} customerContact(s) | customerKey={customer_key}")

    except Exception as e:
        logger.warning(f"Phase 3: enrich_org_with_contacts FAILED | customerKey={customer_key} error={e}", exc_info=True)


# =============================================================================
# Utility Functions
# =============================================================================

def build_full_name(person_info: dict) -> Optional[str]:
    return ' '.join(filter(None, [
        person_info.get('firstName'),
        person_info.get('middleName'),
        person_info.get('lastName'),
    ])) or None


def get_primary_phone(person_info: dict) -> Optional[str]:
    return (
        person_info.get('mobilePhone') or
        person_info.get('dayPhone') or
        person_info.get('eveningPhone')
    )


def get_value(payload: dict, key: str) -> Optional[str]:
    """Get string value from payload, stripping trailing whitespace from Oracle CHAR fields."""
    value = payload.get(key)
    if value is None:
        return None
    s = str(value).strip()
    return s if s else None


def get_timestamp_value(payload: dict, key: str) -> Optional[int]:
    """Get timestamp as epoch_millis from payload. Handles Debezium microsecond timestamps."""
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


def compute_org_name_hash(name: Optional[str]) -> str:
    """Stable 16-char hex hash of org name. Used to skip fan-outs when name hasn't changed."""
    return hashlib.sha256((name or '').encode()).hexdigest()[:16]


def get_cached_org_name_hash(organization_key: str) -> Optional[str]:
    """Return the stored orgNameHash from DynamoDB, or None if not present."""
    try:
        response = cache_table.get_item(Key={'PK': ENTITY_ORGANIZATION, 'SK': organization_key})
        item = response.get('Item')
        if item and item.get('_deleted'):
            return None
        return item.get('orgNameHash') if item else None
    except Exception as e:
        logger.warning(f"get_cached_org_name_hash FAILED | key={organization_key} error={e}")
        return None


def soft_delete_cache_item(pk: str, sk: str) -> None:
    """Soft-delete a DynamoDB cache row by marking it as deleted instead of removing it."""
    cache_table.update_item(
        Key={'PK': pk, 'SK': sk},
        UpdateExpression='SET #d = :true, #da = :now',
        ExpressionAttributeNames={'#d': '_deleted', '#da': '_deletedAt'},
        ExpressionAttributeValues={':true': True, ':now': datetime.now(UTC).isoformat()},
    )


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
