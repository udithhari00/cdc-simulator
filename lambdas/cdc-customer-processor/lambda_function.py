"""
CDC Lambda Handler (Customer) - Processes YFS_CUSTOMER CDC events from Kafka.

This Lambda:
1. Receives YFS_CUSTOMER CDC events via MSK
2. Caches customer data to DynamoDB
3. Fan-out updates customer fields to organization documents (hash-gated)
4. Enriches org doc with address data from CUSTOMER_ADDNL_ADDRESS + PersonInfo
5. Enriches org doc with contact data from CUSTOMER_CONTACT

Customer data is not indexed in its own index — it exists solely to enrich
the organization index via fan-out.

DynamoDB Single Table Design:
- Table: enrichment_cache
- PK (Partition Key): entity_type
- SK (Sort Key): entity_id

Access Patterns:
- Get customer:          PK=CUSTOMER,                  SK=<customer_key>
- Get customer by org:   PK=CUSTOMER_BY_ORG_KEY,        SK=<buyer_org_code>
- Get addnl address:     PK=CUSTOMER_ADDNL_ADDRESS,     SK=<customer_key>#<address_type>  (read)
- Get person info:       PK=PERSON_INFO,                 SK=<person_info_key>               (read)
- Get customer contacts: PK=CUSTOMER_CONTACT,            SK begins_with <customer_key>#     (read)
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

INDEX_ORGANIZATION  = os.environ.get('INDEX_ORGANIZATION', 'organizations')

# Entity type constants for single-table design
ENTITY_CUSTOMER                    = 'CUSTOMER'
ENTITY_CUSTOMER_BY_ORG_KEY         = 'CUSTOMER_BY_ORG_KEY'
ENTITY_CUSTOMER_ADDNL_ADDRESS      = 'CUSTOMER_ADDNL_ADDRESS'
ENTITY_PERSON_INFO                 = 'PERSON_INFO'
ENTITY_CUSTOMER_CONTACT            = 'CUSTOMER_CONTACT'

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

# Replaces the nested customer object on the org doc, then rebuilds searchableText.
PAINLESS_UPDATE_ORG_CUSTOMER_FIELDS = """
    if (params.customer == null) {
        ctx._source.remove('customer');
    } else {
        ctx._source.customer = params.customer;
    }
""" + PAINLESS_REBUILD_ORG_SEARCHABLE_TEXT

# Replaces a single address field (billTo/shipTo/soldTo) on the org doc.
PAINLESS_UPDATE_ORG_ADDRESS_FIELD = """
    ctx._source[params.addressField] = params.address;
""" + PAINLESS_REBUILD_ORG_SEARCHABLE_TEXT

# Replaces the customerContact array on the org doc.
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
    'yfs_customer': 'YFS_CUSTOMER',
}


def extract_table_from_topic(topic: str) -> str:
    """Extract table name from Debezium topic like 'glenraven-dev.STERLING.yfs_customer'."""
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

    if 'YFS_CUSTOMER' in table:
        return process_customer(payload, operation, source_timestamp)
    else:
        logger.warning(f"Unknown table: {table}")
        return {'statusCode': 400, 'body': f'Unknown table: {table}'}


# =============================================================================
# Customer Processing
# =============================================================================

def process_customer(payload: dict, operation: str, source_timestamp=None) -> dict:
    """
    Four-Phase Write for Customer:
    - Phase 1: Cache customer to DynamoDB (with customerHash for fan-out optimization)
    - Phase 2: Fan-out customer fields to matching organization doc (hash-gated)
    - Phase 3: Enrich org doc with address data (CUSTOMER_ADDNL_ADDRESS + PersonInfo)
    - Phase 4: Enrich org doc with contact data (CUSTOMER_CONTACT array)

    DynamoDB entries written:
      PK=CUSTOMER,            SK=<customer_key>    — base customer record
      PK=CUSTOMER_BY_ORG_KEY, SK=<buyer_org_code>  — customer fields keyed by org
    """
    start_total = time.perf_counter()
    customer_id  = get_value(payload, 'CUSTOMER_ID')
    customer_key = get_value(payload, 'CUSTOMER_KEY')
    logger.info(f"process_customer | id={customer_id} key={customer_key} operation={operation}")

    if not customer_id:
        logger.warning(f"Missing CUSTOMER_ID. Payload keys: {list(payload.keys())}")
        return {'statusCode': 400, 'body': 'Missing CUSTOMER_ID'}

    buyer_org_code = get_value(payload, 'BUYER_ORGANIZATION_CODE')

    # ---------------------------------------------------------------
    # PHASE 1: DynamoDB cache
    # ---------------------------------------------------------------
    phase1_start = time.perf_counter()
    if operation == 'd':
        if ENABLE_HARD_DELETE:
            cache_table.delete_item(Key={'PK': ENTITY_CUSTOMER, 'SK': customer_key})
            logger.info(f"Phase 1: Hard-deleted customer from cache | key={customer_key}")
            # Also remove the customer_id-keyed row so orderheader lookups are clean
            if customer_id:
                cache_table.delete_item(Key={'PK': ENTITY_CUSTOMER, 'SK': customer_id})
                logger.info(f"Phase 1: Hard-deleted CUSTOMER#id row | id={customer_id}")
        else:
            soft_delete_cache_item(ENTITY_CUSTOMER, customer_key)
            logger.info(f"Phase 1: Soft-deleted customer from cache | key={customer_key}")
            if customer_id:
                soft_delete_cache_item(ENTITY_CUSTOMER, customer_id)
                logger.info(f"Phase 1: Soft-deleted CUSTOMER#id row | id={customer_id}")
        if buyer_org_code:
            if ENABLE_HARD_DELETE:
                cache_table.delete_item(Key={'PK': ENTITY_CUSTOMER_BY_ORG_KEY, 'SK': buyer_org_code})
                logger.info(f"Phase 1: Hard-deleted CUSTOMER_BY_ORG_KEY | org={buyer_org_code}")
            else:
                soft_delete_cache_item(ENTITY_CUSTOMER_BY_ORG_KEY, buyer_org_code)
                logger.info(f"Phase 1: Soft-deleted CUSTOMER_BY_ORG_KEY | org={buyer_org_code}")
        changed  = True
        new_hash = compute_customer_hash(payload)
    else:
        new_hash      = compute_customer_hash(payload)
        existing_hash = get_cached_customer_hash(buyer_org_code) if buyer_org_code else None
        changed       = existing_hash != new_hash
        logger.info(
            f"Phase 1: Hash check | org={buyer_org_code} existing={existing_hash} "
            f"new={new_hash} changed={changed}"
        )

        item = {
            'PK': ENTITY_CUSTOMER,
            'SK': customer_key,
            'customerKey':           customer_key,
            'customerId':            customer_id,
            'customerType':          get_value(payload, 'CUSTOMER_TYPE'),
            'buyerOrganizationCode': buyer_org_code,
            'billingAddressKey':     get_value(payload, 'BILLING_ADDRESS_KEY'),
            'contactAddressKey':     get_value(payload, 'CONTACT_ADDRESS_KEY'),
            'status':                get_value(payload, 'STATUS'),
            'updatedAt':             datetime.now(UTC).isoformat()
        }
        item = {k: v for k, v in item.items() if v is not None}
        try:
            cache_table.put_item(Item=item)
            logger.info(f"Phase 1: DynamoDB write SUCCESS | key={customer_key}")
        except Exception as e:
            logger.error(f"Phase 1: DynamoDB write FAILED | key={customer_key} error={e}", exc_info=True)
            raise

        # Write a second row keyed by CUSTOMER_ID so that cdc-orderheader-processor
        # can look up by BILL_TO_ID (= CUSTOMER_ID) via: PK=CUSTOMER, SK=<customer_id>
        if customer_id and customer_id != customer_key:
            id_item = {
                'PK':                    ENTITY_CUSTOMER,
                'SK':                    customer_id,
                'customerKey':           customer_key,
                'customerId':            customer_id,
                'customerType':          get_value(payload, 'CUSTOMER_TYPE'),
                'buyerOrganizationCode': buyer_org_code,
                'updatedAt':             datetime.now(UTC).isoformat()
            }
            id_item = {k: v for k, v in id_item.items() if v is not None}
            try:
                cache_table.put_item(Item=id_item)
                logger.info(f"Phase 1: CUSTOMER#id row write SUCCESS | id={customer_id}")
            except Exception as e:
                logger.error(f"Phase 1: CUSTOMER#id row write FAILED | id={customer_id} error={e}", exc_info=True)
                raise

        if buyer_org_code:
            org_key_item = {
                'PK': ENTITY_CUSTOMER_BY_ORG_KEY,
                'SK': buyer_org_code,
                'customerKey':                   customer_key,
                'customerId':                    customer_id,
                'customerType':                  get_value(payload, 'CUSTOMER_TYPE'),
                'customerStatus':                get_value(payload, 'STATUS'),
                'customerClassificationCode':    get_value(payload, 'CUSTOMER_CLASSIFICATION_CODE'),
                'externalCustomerId':            get_value(payload, 'EXTERNAL_CUSTOMER_ID'),
                'customerVertical':              get_value(payload, 'VERTICAL'),
                'customerLevel':                 get_value(payload, 'CUSTOMER_LEVEL'),
                'customerRelationshipType':      get_value(payload, 'RELATIONSHIP_TYPE'),
                'customerRegisteredDate':        get_timestamp_value(payload, 'REGISTERED_DATE'),
                'customerRewardsNo':             get_value(payload, 'CUSTOMER_REWARDS_NO'),
                'extnEligiblePaymentMethods':    get_value(payload, 'EXTN_ELIGIBLE_PAYMENT_METHODS'),
                'extnPartnershipLevel':          get_value(payload, 'EXTN_PARTNERSHIP_LEVEL'),
                'extnPartnershipExpirationDate': get_timestamp_value(payload, 'EXTN_PARTNERSHIP_EXPIRY_DATE'),
                'extnPartnershipStartDate':      get_timestamp_value(payload, 'EXTN_PARTNERSHIP_START_DATE'),
                'extnPartnershipOgStartDate':    get_timestamp_value(payload, 'EXTN_PARTNERSHIP_OG_START_DATE'),
                'extnAutoRenewPartnership':      get_value(payload, 'EXTN_AUTO_RENEW_PARTNERSHIP'),
                'extnNonPfasRequired':           get_value(payload, 'EXTN_NON_PFAS_REQUIRED'),
                'customerHash':                  new_hash,
                'updatedAt':                     datetime.now(UTC).isoformat()
            }
            org_key_item = {k: v for k, v in org_key_item.items() if v is not None}
            try:
                cache_table.put_item(Item=org_key_item)
                logger.info(f"Phase 1: CUSTOMER_BY_ORG_KEY write SUCCESS | org={buyer_org_code}")
            except Exception as e:
                logger.error(f"Phase 1: CUSTOMER_BY_ORG_KEY write FAILED | org={buyer_org_code} error={e}", exc_info=True)
                raise

    phase1_ms = (time.perf_counter() - phase1_start) * 1000
    logger.info(f"TIMING phase1_dynamodb | id={customer_id} duration_ms={phase1_ms:.1f}")

    # ---------------------------------------------------------------
    # PHASE 2: Fan-out to org index (only when customer data changed)
    # ---------------------------------------------------------------
    if changed:
        start_phase2 = time.perf_counter()
        update_org_with_customer(payload, operation, new_hash)
        phase2_ms = (time.perf_counter() - start_phase2) * 1000
        logger.info(f"TIMING phase2_os_customer | id={customer_id} duration_ms={phase2_ms:.1f}")
    else:
        logger.info(f"Phase 2: Skipped — customer data unchanged | org={buyer_org_code}")

    # ---------------------------------------------------------------
    # PHASE 3: Enrich org doc with address data (BillTo/ShipTo/SoldTo)
    # ---------------------------------------------------------------
    if customer_key:
        start_phase3 = time.perf_counter()
        update_org_with_addresses(customer_key, operation)
        phase3_ms = (time.perf_counter() - start_phase3) * 1000
        logger.info(f"TIMING phase3_os_addresses | id={customer_id} duration_ms={phase3_ms:.1f}")

    # ---------------------------------------------------------------
    # PHASE 4: Enrich org doc with customer contacts (array)
    # ---------------------------------------------------------------
    if customer_key:
        start_phase4 = time.perf_counter()
        update_org_with_customer_contacts(customer_key, operation)
        phase4_ms = (time.perf_counter() - start_phase4) * 1000
        logger.info(f"TIMING phase4_os_contacts | id={customer_id} duration_ms={phase4_ms:.1f}")

    total_ms = (time.perf_counter() - start_total) * 1000
    logger.info(f"TIMING process_customer_total | id={customer_id} operation={operation} duration_ms={total_ms:.1f}")

    return {'statusCode': 200, 'body': f'Processed customer: {customer_id}'}


def update_org_with_customer(payload: dict, operation: str, new_hash: str) -> None:
    """
    Phase 2: Fan-out customer fields onto the matching organization document.

    Only called when customer data has changed (hash differs).
    Uses must_not hash filter so already-current org docs are skipped server-side.
    Fan-out failures are non-fatal.
    """
    buyer_org_key = get_value(payload, 'BUYER_ORGANIZATION_CODE')
    if not buyer_org_key:
        return

    try:
        client = get_opensearch_client()

        if operation == 'd':
            customer_obj = None
        else:
            customer_obj = {k: v for k, v in {
                'customerId':                    get_value(payload, 'CUSTOMER_ID'),
                'customerKey':                   get_value(payload, 'CUSTOMER_KEY'),
                'customerType':                  get_value(payload, 'CUSTOMER_TYPE'),
                'customerStatus':                get_value(payload, 'STATUS'),
                'customerClassificationCode':    get_value(payload, 'CUSTOMER_CLASSIFICATION_CODE'),
                'externalCustomerId':            get_value(payload, 'EXTERNAL_CUSTOMER_ID'),
                'customerVertical':              get_value(payload, 'VERTICAL'),
                'customerLevel':                 get_value(payload, 'CUSTOMER_LEVEL'),
                'customerRelationshipType':      get_value(payload, 'RELATIONSHIP_TYPE'),
                'customerRegisteredDate':        get_timestamp_value(payload, 'REGISTERED_DATE'),
                'customerRewardsNo':             get_value(payload, 'CUSTOMER_REWARDS_NO'),
                'extnEligiblePaymentMethods':    get_value(payload, 'EXTN_ELIGIBLE_PAYMENT_METHODS'),
                'extnPartnershipLevel':          get_value(payload, 'EXTN_PARTNERSHIP_LEVEL'),
                'extnPartnershipExpirationDate': get_timestamp_value(payload, 'EXTN_PARTNERSHIP_EXPIRY_DATE'),
                'extnPartnershipStartDate':      get_timestamp_value(payload, 'EXTN_PARTNERSHIP_START_DATE'),
                'extnPartnershipOgStartDate':    get_timestamp_value(payload, 'EXTN_PARTNERSHIP_OG_START_DATE'),
                'extnAutoRenewPartnership':      get_value(payload, 'EXTN_AUTO_RENEW_PARTNERSHIP'),
                'extnNonPfasRequired':           get_value(payload, 'EXTN_NON_PFAS_REQUIRED'),
                'customerHash':                  new_hash,
            }.items() if v is not None}

        result = client.update_by_query(
            index=INDEX_ORGANIZATION,
            body={
                "query": {
                    "bool": {
                        "must":     {"term": {"organizationKey": buyer_org_key}},
                        "must_not": {"term": {"customer.customerHash": new_hash}}
                    }
                },
                "script": {
                    "source": PAINLESS_UPDATE_ORG_CUSTOMER_FIELDS,
                    "lang":   "painless",
                    "params": {"customer": customer_obj}
                }
            },
            conflicts='proceed',
            refresh=True
        )
        updated = result.get('updated', 0)
        if updated > 0:
            logger.info(f"Phase 2: Updated {updated} org(s) | org={buyer_org_key} hash={new_hash}")
        else:
            logger.info(f"Phase 2: All orgs already up to date | org={buyer_org_key} hash={new_hash}")

    except Exception as e:
        status_code = getattr(e, 'status_code', None)
        error_info  = getattr(e, 'info', getattr(e, 'body', ''))
        logger.warning(
            f"Phase 2 failed for Customer | org={buyer_org_key} status={status_code} error={e} info={error_info}"
        )


# =============================================================================
# Phase 3: Address Enrichment
# =============================================================================

# Maps DynamoDB address type suffix → org doc nested field name
ADDRESS_TYPE_TO_FIELD = {'BillTo': 'billTo', 'ShipTo': 'shipTo', 'SoldTo': 'soldTo'}


def update_org_with_addresses(customer_key: str, operation: str) -> None:
    """
    Phase 3: Query all CUSTOMER_ADDNL_ADDRESS records for this customerKey,
    look up PersonInfo for each, and update the org doc with billTo/shipTo/soldTo.
    """
    customer_key = str(customer_key).strip()
    try:
        client = get_opensearch_client()

        if operation == 'd':
            # On delete, null out all three address fields
            for field_name in ADDRESS_TYPE_TO_FIELD.values():
                client.update_by_query(
                    index=INDEX_ORGANIZATION,
                    body={
                        "query": {"term": {"customer.customerKey": customer_key}},
                        "script": {
                            "source": PAINLESS_UPDATE_ORG_ADDRESS_FIELD,
                            "lang":   "painless",
                            "params": {"addressField": field_name, "address": None}
                        }
                    },
                    conflicts='proceed',
                    refresh=True
                )
            logger.info(f"Phase 3: Nulled out all address fields on delete | key={customer_key}")
            return

        # Query all address records for this customerKey
        from boto3.dynamodb.conditions import Key as DKey
        response = cache_table.query(
            KeyConditionExpression=DKey('PK').eq(ENTITY_CUSTOMER_ADDNL_ADDRESS) & DKey('SK').begins_with(f'{customer_key}#')
        )
        items = response.get('Items', [])
        if not items:
            logger.info(f"Phase 3: No cached addresses for key={customer_key}, skipping")
            return

        for item in items:
            if item.get('_deleted'):
                continue
            # Extract address type from SK: <customerKey>#<addressType>
            sk = item.get('SK', '')
            address_type = sk.split('#', 1)[1] if '#' in sk else None
            field_name = ADDRESS_TYPE_TO_FIELD.get(address_type)
            if not field_name:
                logger.info(f"Phase 3: Unknown address type '{address_type}' | sk={sk}, skipping")
                continue

            address_obj = build_address_from_item(item)
            if address_obj is None:
                logger.info(f"Phase 3: No cached {address_type} address for key={customer_key}, skipping")
                continue

            result = client.update_by_query(
                index=INDEX_ORGANIZATION,
                body={
                    "query": {"term": {"customer.customerKey": customer_key}},
                    "script": {
                        "source": PAINLESS_UPDATE_ORG_ADDRESS_FIELD,
                        "lang":   "painless",
                        "params": {"addressField": field_name, "address": address_obj}
                    }
                },
                conflicts='proceed',
                refresh=True
            )
            updated = result.get('updated', 0)
            if updated > 0:
                logger.info(f"Phase 3: Updated {updated} org(s) {field_name} | key={customer_key}")
            else:
                logger.info(f"Phase 3: No org found for {field_name} | key={customer_key}")

    except Exception as e:
        logger.warning(f"Phase 3 failed for addresses | key={customer_key} error={e}")


def build_address_from_item(item: dict) -> Optional[dict]:
    """
    Given a CUSTOMER_ADDNL_ADDRESS DynamoDB item, look up its PersonInfo
    and build the full address object.
    """
    try:
        person_info_key = item.get('personInfoKey')
        if not person_info_key:
            logger.warning(f"build_address_from_item: No personInfoKey | SK={item.get('SK')}")
            return None

        person_info = get_cached_person_info(person_info_key)
        if not person_info:
            logger.warning(f"build_address_from_item: PersonInfo not in cache | key={person_info_key}")
            return None

        first_name  = person_info.get('firstName')
        middle_name = person_info.get('middleName')
        last_name   = person_info.get('lastName')
        return {k: v for k, v in {
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

    except Exception as e:
        logger.warning(f"build_address_from_item FAILED | SK={item.get('SK')} error={e}")
        return None


# =============================================================================
# Phase 4: Contact Array Enrichment
# =============================================================================

def update_org_with_customer_contacts(customer_key: str, operation: str) -> None:
    """
    Phase 4: Query all CUSTOMER_CONTACT records for this customerKey
    and update the org doc with a customerContact array.
    """
    try:
        client = get_opensearch_client()

        if operation == 'd':
            contacts_array = None
        else:
            contacts_array = build_contacts_array(customer_key)
            if not contacts_array:
                logger.info(f"Phase 4: No cached contacts for key={customer_key}, skipping")
                return

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
            logger.info(f"Phase 4: Updated {updated} org(s) with {len(contacts_array) if contacts_array else 0} contact(s) | key={customer_key}")
        else:
            logger.info(f"Phase 4: No org found for customerKey={customer_key}")

    except Exception as e:
        logger.warning(f"Phase 4 failed for contacts | key={customer_key} error={e}")


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


# =============================================================================
# DynamoDB Cache Lookups
# =============================================================================

def get_cached_customer(customer_key: str) -> Optional[dict]:
    try:
        response = cache_table.get_item(Key={'PK': ENTITY_CUSTOMER, 'SK': customer_key})
        item = response.get('Item')
        return {k: v for k, v in item.items() if k not in ('PK', 'SK', 'updatedAt')} if item else None
    except Exception as e:
        logger.warning(f"Error fetching customer | key={customer_key} error={e}")
        return None


def get_cached_customer_by_org_key(buyer_org_code: str) -> Optional[dict]:
    try:
        response = cache_table.get_item(Key={'PK': ENTITY_CUSTOMER_BY_ORG_KEY, 'SK': buyer_org_code})
        item = response.get('Item')
        if item and item.get('_deleted'):
            logger.info(f"get_cached_customer_by_org_key: SOFT-DELETED | org={buyer_org_code}")
            return None
        if item:
            result = {k: v for k, v in item.items() if k not in ('PK', 'SK', 'updatedAt', '_deleted', '_deletedAt')}
            logger.info(f"get_cached_customer_by_org_key: FOUND | org={buyer_org_code} customerId={result.get('customerId')}")
            return result
        logger.info(f"get_cached_customer_by_org_key: NO ENTRY | org={buyer_org_code}")
        return None
    except Exception as e:
        logger.warning(f"get_cached_customer_by_org_key FAILED | org={buyer_org_code} error={e}", exc_info=True)
        return None


def get_cached_person_info(person_info_key: str) -> Optional[dict]:
    """Retrieve person info from DynamoDB cache."""
    try:
        response = cache_table.get_item(Key={'PK': ENTITY_PERSON_INFO, 'SK': person_info_key})
        item = response.get('Item')
        if item and item.get('_deleted'):
            return None
        return {k: v for k, v in item.items() if k not in ('PK', 'SK', 'updatedAt', '_deleted', '_deletedAt')} if item else None
    except Exception as e:
        logger.warning(f"Error fetching person info | key={person_info_key} error={e}")
        return None


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


def get_timestamp_value(payload: dict, key: str) -> Optional[int]:
    """Get timestamp as epoch_millis. Handles Debezium microsecond timestamps."""
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


def compute_customer_hash(payload: dict) -> str:
    """
    Stable 16-char hex hash of the customer fields that get fanned out to the org index.
    Used to skip fan-outs when nothing meaningful has changed.
    """
    fields = {
        'customerId':                    get_value(payload, 'CUSTOMER_ID'),
        'customerKey':                   get_value(payload, 'CUSTOMER_KEY'),
        'customerType':                  get_value(payload, 'CUSTOMER_TYPE'),
        'customerStatus':                get_value(payload, 'STATUS'),
        'customerClassificationCode':    get_value(payload, 'CUSTOMER_CLASSIFICATION_CODE'),
        'externalCustomerId':            get_value(payload, 'EXTERNAL_CUSTOMER_ID'),
        'customerVertical':              get_value(payload, 'VERTICAL'),
        'customerLevel':                 get_value(payload, 'CUSTOMER_LEVEL'),
        'customerRelationshipType':      get_value(payload, 'RELATIONSHIP_TYPE'),
        'customerRewardsNo':             get_value(payload, 'CUSTOMER_REWARDS_NO'),
        'extnEligiblePaymentMethods':    get_value(payload, 'EXTN_ELIGIBLE_PAYMENT_METHODS'),
        'extnPartnershipLevel':          get_value(payload, 'EXTN_PARTNERSHIP_LEVEL'),
        'extnAutoRenewPartnership':      get_value(payload, 'EXTN_AUTO_RENEW_PARTNERSHIP'),
        'extnNonPfasRequired':           get_value(payload, 'EXTN_NON_PFAS_REQUIRED'),
    }
    canonical = json.dumps(fields, sort_keys=True)
    return hashlib.sha256(canonical.encode()).hexdigest()[:16]


def get_cached_customer_hash(buyer_org_code: str) -> Optional[str]:
    """Return the stored customerHash from DynamoDB CUSTOMER_BY_ORG_KEY entry, or None."""
    try:
        response = cache_table.get_item(Key={'PK': ENTITY_CUSTOMER_BY_ORG_KEY, 'SK': buyer_org_code})
        item = response.get('Item')
        if item and item.get('_deleted'):
            return None
        return item.get('customerHash') if item else None
    except Exception as e:
        logger.warning(f"get_cached_customer_hash FAILED | org={buyer_org_code} error={e}")
        return None


def soft_delete_cache_item(pk: str, sk: str) -> None:
    """Soft-delete a DynamoDB cache row by marking it as deleted instead of removing it."""
    cache_table.update_item(
        Key={'PK': pk, 'SK': sk},
        UpdateExpression='SET #d = :true, #da = :now',
        ExpressionAttributeNames={'#d': '_deleted', '#da': '_deletedAt'},
        ExpressionAttributeValues={':true': True, ':now': datetime.now(UTC).isoformat()},
    )
