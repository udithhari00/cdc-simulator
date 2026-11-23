"""
CDC Lambda Handler (Customer Additional Address) - Processes YFS_CUSTOMER_ADDNL_ADDRESS CDC events.

This Lambda:
1. Receives YFS_CUSTOMER_ADDNL_ADDRESS CDC events via MSK
2. Caches address records to DynamoDB
3. Looks up PersonInfo from cache, then fan-out updates billTo/shipTo/soldTo
   on matching organization documents in OpenSearch

Only processes default BillTo, ShipTo, SoldTo addresses.
All other address types and non-default records are skipped.

DynamoDB Single Table Design:
- Table: enrichment_cache
- PK (Partition Key): entity_type
- SK (Sort Key): entity_id

Access Patterns:
- Get addnl address: PK=CUSTOMER_ADDNL_ADDRESS, SK=<customer_key>#<address_type>
- Get person info:   PK=PERSON_INFO,             SK=<person_info_key>
"""

import base64
import json
import logging
import os
import socket
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
ENTITY_CUSTOMER_ADDNL_ADDRESS     = 'CUSTOMER_ADDNL_ADDRESS'
ENTITY_CUSTOMER_ADDNL_ADDRESS_KEY = 'CUSTOMER_ADDNL_ADDRESS_KEY'
ENTITY_PERSON_INFO                = 'PERSON_INFO'

# Maps address role -> org doc nested field name
ADDRESS_TYPE_TO_FIELD = {'BillTo': 'billTo', 'ShipTo': 'shipTo', 'SoldTo': 'soldTo'}

ENABLE_HARD_DELETE = os.environ.get('ENABLE_HARD_DELETE', 'false').lower() == 'true'

def is_hard_delete_enabled() -> bool:
    val = os.environ.get('ENABLE_HARD_DELETE')
    if val is not None:
        return val.lower() == 'true'
    return ENABLE_HARD_DELETE


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


# ==========================================================
# Topic → Table Routing
# ==========================================================

TOPIC_TABLE_MAP = {
    'yfs_customer_addnl_address': 'YFS_CUSTOMER_ADDNL_ADDRESS',
}


def extract_table_from_topic(topic: str) -> str:
    """Extract table name from Debezium topic like 'glenraven-dev.STERLING.yfs_customer_addnl_address'."""
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
        logger.debug(
            f"Partition start | topic_partition={topic_partition} message_count={len(messages)}"
        )

        for msg in messages:
            topic  = msg.get('topic', topic_partition.rsplit('-', 1)[0])
            offset = msg.get('offset', 'unknown')

            try:
                table = extract_table_from_topic(topic)

                if msg.get('value') is None:
                    logger.debug(f'Tombstone record skipped | topic={topic} offset={offset}')
                    continue

                raw_value      = base64.b64decode(msg['value']).decode('utf-8')
                debezium_event = json.loads(raw_value)

                # Debezium emits null-payload for deletes when before-image is unavailable (online_catalog + CDB/PDB)
                # Use the message key to perform the delete
                if debezium_event.get('payload') is None and 'schema' in debezium_event:
                    logger.debug(f'Null-payload delete | topic={topic} offset={offset} - using key for delete')
                    result = process_delete_by_key(table, msg.get('key'))
                    if result.get('statusCode', 0) != 200:
                        logger.error(f"Error processing delete by key | topic={topic} offset={offset} body={result['body']}")
                        errors += 1
                    else:
                        processed += 1
                    continue

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

                logger.debug(
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
    logger.debug(f"Processing {operation} event for table {table}")

    if 'YFS_CUSTOMER_ADDNL_ADDRESS' in table:
        return process_customer_addnl_address(payload, operation)
    else:
        logger.warning(f"Unknown table: {table}")
        return {'statusCode': 400, 'body': f'Unknown table: {table}'}


# =============================================================================
# Customer Additional Address Processing
# =============================================================================

def process_customer_addnl_address(payload: dict, operation: str) -> dict:
    """
    Two-Phase Write for CustomerAddnlAddress:
    - Phase 1: Cache address records in DynamoDB (PK=CUSTOMER_ADDNL_ADDRESS, SK=customer_key#role)
    - Phase 2: Look up PersonInfo, then fan-out billTo/shipTo/soldTo to matching organization in OpenSearch

    Only processes organization-level default addresses (CUSTOMER_CONTACT_KEY is empty/null).
    Derives roles from default flags:
      - IS_BILL_TO == 'Y' and IS_DEFAULT_BILL_TO == 'Y' -> BillTo
      - IS_SHIP_TO == 'Y' and IS_DEFAULT_SHIP_TO == 'Y' -> ShipTo
      - IS_SOLD_TO == 'Y' and IS_DEFAULT_SOLD_TO == 'Y' -> SoldTo
    A single row can satisfy multiple roles (e.g. BillTo AND SoldTo).
    """
    customer_key = get_value(payload, 'CUSTOMER_KEY')
    if customer_key:
        customer_key = customer_key.strip()

    addnl_address_key = get_value(payload, 'CUSTOMER_ADDNL_ADDRESS_KEY')
    if addnl_address_key:
        addnl_address_key = addnl_address_key.strip()

    person_info_key = get_value(payload, 'PERSON_INFO_KEY')
    if person_info_key:
        person_info_key = person_info_key.strip()

    raw_contact_key = get_value(payload, 'CUSTOMER_CONTACT_KEY')
    customer_contact_key = raw_contact_key.strip() if raw_contact_key else None

    logger.info(
        f"process_customer_addnl_address | customer_key={customer_key} "
        f"addnl_address_key={addnl_address_key} operation={operation}"
    )

    if not customer_key:
        if operation == 'd' and addnl_address_key:
            old_rev = get_cached_addnl_address_by_addnl_key(addnl_address_key)
            if old_rev and old_rev.get('customerKey'):
                customer_key = old_rev['customerKey'].strip()
        if not customer_key:
            logger.warning(f"Missing CUSTOMER_KEY. Payload keys: {list(payload.keys())}")
            return {'statusCode': 400, 'body': 'Missing CUSTOMER_KEY'}

    if customer_contact_key:
        logger.info(f"Skipping contact-level address | contact_key={customer_contact_key}")
        return {'statusCode': 200, 'body': f'Skipped contact-level address: {customer_contact_key}'}

    # Determine qualifying roles based on boolean default flags
    is_bill_to         = get_value(payload, 'IS_BILL_TO')
    is_default_bill_to = get_value(payload, 'IS_DEFAULT_BILL_TO')
    is_ship_to         = get_value(payload, 'IS_SHIP_TO')
    is_default_ship_to = get_value(payload, 'IS_DEFAULT_SHIP_TO')
    is_sold_to         = get_value(payload, 'IS_SOLD_TO')
    is_default_sold_to = get_value(payload, 'IS_DEFAULT_SOLD_TO')

    qualifying_roles = []
    if (is_bill_to == 'Y' or is_bill_to is None) and is_default_bill_to == 'Y':
        qualifying_roles.append('BillTo')
    if (is_ship_to == 'Y' or is_ship_to is None) and is_default_ship_to == 'Y':
        qualifying_roles.append('ShipTo')
    if (is_sold_to == 'Y' or is_sold_to is None) and is_default_sold_to == 'Y':
        qualifying_roles.append('SoldTo')

    # Fast skip for snapshot read ('r') of non-default addresses: no DynamoDB read needed
    if operation == 'r' and not qualifying_roles:
        logger.debug(f"Skipping non-default address in snapshot | addnl_address_key={addnl_address_key}")
        return {'statusCode': 200, 'body': f'Skipped non-default address: {addnl_address_key}'}

    # Read previous reverse lookup for live events ('c', 'd', etc.) to detect revoked roles or handle deletes
    old_reverse = None
    old_roles = set()
    if operation != 'r' and addnl_address_key:
        old_reverse = get_cached_addnl_address_by_addnl_key(addnl_address_key)
        if old_reverse:
            if old_reverse.get('roles'):
                old_roles = set(old_reverse.get('roles'))
            elif old_reverse.get('addressType'):
                old_roles = {old_reverse.get('addressType')}

    # Check for hard delete or soft delete
    if operation == 'd':
        roles_to_delete = set(qualifying_roles) | old_roles
        if not roles_to_delete:
            logger.info(f"Delete event for non-default address | addnl_address_key={addnl_address_key}")
            return {'statusCode': 200, 'body': 'Skipped non-default delete'}

        for role in roles_to_delete:
            sk = f'{customer_key}#{role}'
            if is_hard_delete_enabled():
                cache_table.delete_item(Key={'PK': ENTITY_CUSTOMER_ADDNL_ADDRESS, 'SK': sk})
            else:
                soft_delete_cache_item(ENTITY_CUSTOMER_ADDNL_ADDRESS, sk)
            logger.info(f"Phase 1: Deleted addnl address | sk={sk}")

        fields_to_clear = [ADDRESS_TYPE_TO_FIELD[r] for r in roles_to_delete if r in ADDRESS_TYPE_TO_FIELD]
        if fields_to_clear:
            clear_org_address_fields(customer_key, fields_to_clear)

        if addnl_address_key:
            if is_hard_delete_enabled():
                cache_table.delete_item(Key={'PK': ENTITY_CUSTOMER_ADDNL_ADDRESS_KEY, 'SK': addnl_address_key})
            else:
                soft_delete_cache_item(ENTITY_CUSTOMER_ADDNL_ADDRESS_KEY, addnl_address_key)

        return {'statusCode': 200, 'body': f'Deleted addnl address roles: {list(roles_to_delete)}'}

    # If not a delete, check for revoked roles (e.g. was default ShipTo, now updated to 'N')
    revoked_roles = old_roles - set(qualifying_roles)
    for role in revoked_roles:
        sk = f'{customer_key}#{role}'
        logger.info(f"Phase 1: Revoking non-default role | sk={sk}")
        if is_hard_delete_enabled():
            cache_table.delete_item(Key={'PK': ENTITY_CUSTOMER_ADDNL_ADDRESS, 'SK': sk})
        else:
            soft_delete_cache_item(ENTITY_CUSTOMER_ADDNL_ADDRESS, sk)

    fields_to_clear = [ADDRESS_TYPE_TO_FIELD[r] for r in revoked_roles if r in ADDRESS_TYPE_TO_FIELD]
    if fields_to_clear:
        clear_org_address_fields(customer_key, fields_to_clear)

    if not qualifying_roles:
        logger.debug(f"Skipping non-default address | addnl_address_key={addnl_address_key}")
        if addnl_address_key and old_reverse:
            if is_hard_delete_enabled():
                cache_table.delete_item(Key={'PK': ENTITY_CUSTOMER_ADDNL_ADDRESS_KEY, 'SK': addnl_address_key})
            else:
                soft_delete_cache_item(ENTITY_CUSTOMER_ADDNL_ADDRESS_KEY, addnl_address_key)
        return {'statusCode': 200, 'body': f'Skipped non-default address: {addnl_address_key}'}

    # Fetch PersonInfo once for all qualifying roles
    address_obj = None
    if person_info_key:
        person_info = get_cached_person_info(person_info_key)
        if person_info:
            first_name  = person_info.get('firstName')
            middle_name = person_info.get('middleName')
            last_name   = person_info.get('lastName')
            address_obj = {
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
            }
        else:
            logger.warning(f"Phase 2: PersonInfo not in cache | key={person_info_key}")
    else:
        logger.warning(f"Phase 2: No PERSON_INFO_KEY in payload | customer_key={customer_key}")

    # PHASE 1: Write each qualifying role to DynamoDB
    for role in qualifying_roles:
        sk = f'{customer_key}#{role}'
        item = {
            'PK':            ENTITY_CUSTOMER_ADDNL_ADDRESS,
            'SK':            sk,
            'personInfoKey': person_info_key,
            'customerKey':   customer_key,
            'addressType':   role,
            'updatedAt':     datetime.now(UTC).isoformat()
        }
        item = {k: v for k, v in item.items() if v is not None}
        try:
            cache_table.put_item(Item=item)
            logger.debug(f"Phase 1: DynamoDB write SUCCESS | sk={sk}")
        except Exception as e:
            logger.error(f"Phase 1: DynamoDB write FAILED | sk={sk} error={e}", exc_info=True)
            raise

    # Store reverse lookup with roles list
    if addnl_address_key:
        reverse_item = {
            'PK':          ENTITY_CUSTOMER_ADDNL_ADDRESS_KEY,
            'SK':          addnl_address_key,
            'customerKey': customer_key,
            'addressType': qualifying_roles[0],
            'roles':       qualifying_roles,
            'updatedAt':   datetime.now(UTC).isoformat()
        }
        cache_table.put_item(Item=reverse_item)

    # PHASE 2: Fan-out address object for all qualifying roles to OpenSearch in a single atomic update
    if address_obj and qualifying_roles:
        field_names = [ADDRESS_TYPE_TO_FIELD[r] for r in qualifying_roles if r in ADDRESS_TYPE_TO_FIELD]
        if field_names:
            update_org_address_fields(customer_key, field_names, address_obj)

    return {'statusCode': 200, 'body': f'Processed customer addnl address: {customer_key} roles={qualifying_roles}'}


def update_org_address_fields(customer_key: str, field_names: list, address_obj: dict) -> None:
    """Atomically update multiple address fields (billTo/shipTo/soldTo) on matching organization documents."""
    if not field_names:
        return
    try:
        client = get_opensearch_client()
        result = client.update_by_query(
            index=INDEX_ORGANIZATION,
            body={
                "query": {"term": {"customer.customerKey": customer_key}},
                "script": {
                    "source": (
                        "for (def f : params.addressFields) { ctx._source[f] = params.address; }"
                        + PAINLESS_REBUILD_ORG_SEARCHABLE_TEXT
                    ),
                    "lang":   "painless",
                    "params": {"addressFields": field_names, "addressField": field_names[0], "address": address_obj}
                }
            },
            conflicts='proceed',
            refresh=True
        )
        updated = result.get('updated', 0)
        if updated > 0:
            logger.info(f"Phase 2: Updated {updated} org(s) {field_names} | key={customer_key}")
        else:
            logger.info(f"Phase 2: No org found for customerKey={customer_key}")
    except Exception as e:
        logger.warning(f"Phase 2 failed to update {field_names} | key={customer_key} error={e}")


def update_org_address_field(customer_key: str, field_name: str, address_obj: dict) -> None:
    update_org_address_fields(customer_key, [field_name], address_obj)


def clear_org_address_fields(customer_key: str, field_names: list) -> None:
    """Atomically null out multiple address fields on matching organization documents."""
    if not field_names:
        return
    try:
        client = get_opensearch_client()
        result = client.update_by_query(
            index=INDEX_ORGANIZATION,
            body={
                "query": {"term": {"customer.customerKey": customer_key}},
                "script": {
                    "source": (
                        "for (def f : params.addressFields) { ctx._source[f] = null; }"
                        + PAINLESS_REBUILD_ORG_SEARCHABLE_TEXT
                    ),
                    "lang":   "painless",
                    "params": {"addressFields": field_names, "addressField": field_names[0], "address": None}
                }
            },
            conflicts='proceed',
            refresh=True
        )
        updated = result.get('updated', 0)
        if updated > 0:
            logger.info(f"Phase 2: Cleared {field_names} on {updated} org(s) | customer_key={customer_key}")
        else:
            logger.info(f"Phase 2: No org found for customerKey={customer_key}")
    except Exception as e:
        logger.warning(f"Phase 2 failed to clear {field_names} | key={customer_key} error={e}")


def clear_org_address_field(customer_key: str, field_name: str) -> None:
    clear_org_address_fields(customer_key, [field_name])


# =============================================================================
# Key-Based Delete (null-payload deletes — before-image unavailable)
# =============================================================================

def process_delete_by_key(table: str, key_b64: Optional[str]) -> dict:
    """Handle delete events where payload is null by using the Kafka message key."""
    if not key_b64:
        logger.warning(f"process_delete_by_key | no key available | table={table}")
        return {'statusCode': 200, 'body': 'Skipped: no key'}

    try:
        key_data   = json.loads(base64.b64decode(key_b64).decode('utf-8'))
        key_payload = key_data.get('payload', {}) or {}
        addnl_address_key = get_value(
            {k.upper(): v for k, v in key_payload.items()},
            'CUSTOMER_ADDNL_ADDRESS_KEY'
        )
    except Exception as e:
        logger.warning(f"process_delete_by_key | failed to decode key | error={e}")
        return {'statusCode': 200, 'body': f'Skipped: key decode failed: {e}'}

    if not addnl_address_key:
        logger.warning(f"process_delete_by_key | CUSTOMER_ADDNL_ADDRESS_KEY missing from key")
        return {'statusCode': 200, 'body': 'Skipped: no CUSTOMER_ADDNL_ADDRESS_KEY in key'}

    addnl_address_key = addnl_address_key.strip()
    reverse = get_cached_addnl_address_by_addnl_key(addnl_address_key)
    if not reverse:
        logger.warning(f"process_delete_by_key | reverse lookup not found | addnl_address_key={addnl_address_key}")
        return {'statusCode': 200, 'body': f'Skipped: reverse lookup missing for {addnl_address_key}'}

    customer_key = reverse.get('customerKey')
    if customer_key:
        customer_key = customer_key.strip()

    if not customer_key:
        logger.warning(f"process_delete_by_key | customerKey missing in reverse lookup | addnl_address_key={addnl_address_key}")
        return {'statusCode': 200, 'body': f'Skipped: customerKey missing in reverse lookup for {addnl_address_key}'}

    roles = reverse.get('roles') or []
    if not roles and reverse.get('addressType'):
        roles = [reverse.get('addressType')]

    logger.info(f"process_delete_by_key | addnl_address_key={addnl_address_key} customer_key={customer_key} roles={roles}")

    for role in roles:
        sk = f'{customer_key}#{role}'
        if is_hard_delete_enabled():
            cache_table.delete_item(Key={'PK': ENTITY_CUSTOMER_ADDNL_ADDRESS, 'SK': sk})
        else:
            soft_delete_cache_item(ENTITY_CUSTOMER_ADDNL_ADDRESS, sk)
        logger.info(f"Phase 1: Hard-deleted addnl address via key | sk={sk}")

    fields_to_clear = [ADDRESS_TYPE_TO_FIELD[r] for r in roles if r in ADDRESS_TYPE_TO_FIELD]
    if fields_to_clear:
        clear_org_address_fields(customer_key, fields_to_clear)

    if is_hard_delete_enabled():
        cache_table.delete_item(Key={'PK': ENTITY_CUSTOMER_ADDNL_ADDRESS_KEY, 'SK': addnl_address_key})
    else:
        soft_delete_cache_item(ENTITY_CUSTOMER_ADDNL_ADDRESS_KEY, addnl_address_key)

    return {'statusCode': 200, 'body': f'Deleted addnl address by key: {addnl_address_key} roles={roles}'}


# =============================================================================
# DynamoDB Cache Lookups
# =============================================================================

def get_cached_person_info(person_info_key: str) -> Optional[dict]:
    try:
        response = cache_table.get_item(Key={'PK': ENTITY_PERSON_INFO, 'SK': person_info_key})
        item = response.get('Item')
        if item and item.get('_deleted'):
            return None
        return {k: v for k, v in item.items() if k not in ('PK', 'SK', 'updatedAt', '_deleted', '_deletedAt')} if item else None
    except Exception as e:
        logger.warning(f"Error fetching person info | key={person_info_key} error={e}")
        return None


def get_cached_addnl_address_by_addnl_key(addnl_address_key: str) -> Optional[dict]:
    try:
        response = cache_table.get_item(Key={'PK': ENTITY_CUSTOMER_ADDNL_ADDRESS_KEY, 'SK': addnl_address_key})
        item = response.get('Item')
        if item and item.get('_deleted'):
            return None
        return {k: v for k, v in item.items() if k not in ('PK', 'SK', 'updatedAt', '_deleted', '_deletedAt')} if item else None
    except Exception as e:
        logger.warning(f"Error fetching addnl address by key | addnl_address_key={addnl_address_key} error={e}")
        return None


def get_cached_customer_addnl_address(customer_key: str, address_type: str) -> Optional[dict]:
    sk = f'{customer_key}#{address_type}'
    try:
        response = cache_table.get_item(Key={'PK': ENTITY_CUSTOMER_ADDNL_ADDRESS, 'SK': sk})
        item = response.get('Item')
        if item and item.get('_deleted'):
            return None
        return {k: v for k, v in item.items() if k not in ('PK', 'SK', 'updatedAt', '_deleted', '_deletedAt')} if item else None
    except Exception as e:
        logger.warning(f"Error fetching customer addnl address | sk={sk} error={e}")
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
