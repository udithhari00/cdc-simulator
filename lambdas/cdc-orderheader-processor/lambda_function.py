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

DYNAMODB_TABLE = os.environ.get("DYNAMODB_TABLE", "enrichment_cache")
OPENSEARCH_HOST = os.environ.get("OPENSEARCH_HOST", "opensearch")
OPENSEARCH_PORT = int(os.environ.get("OPENSEARCH_PORT", "443"))
OPENSEARCH_SCHEME = os.environ.get("OPENSEARCH_SCHEME", "https")
OPENSEARCH_AUTH = os.environ.get("OPENSEARCH_AUTH", "iam")
OPENSEARCH_USERNAME = os.environ.get("OPENSEARCH_USERNAME", "admin")
OPENSEARCH_PASSWORD = os.environ.get("OPENSEARCH_PASSWORD", "admin")
OPENSEARCH_MODE = os.environ.get("OPENSEARCH_MODE", "serverless")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

INDEX_ORDER = os.environ.get("INDEX_ORDER", "orders")
ENABLE_HARD_DELETE = os.environ.get('ENABLE_HARD_DELETE', 'false').lower() == 'true'
ENTITY_PERSON_INFO = "PERSON_INFO"
ENTITY_ORGANIZATION = "ORGANIZATION"
ENTITY_CUSTOMER = "CUSTOMER"
ENTITY_ORDER_STATUS = "ORDER_STATUS"
ENTITY_ORDER_INVOICE = "ORDER_INVOICE"
ENTITY_ORDER_HOLD_TYPE = "ORDER_HOLD_TYPE"
ENTITY_SHIPMENT = "YFS_SHIPMENT"
ENTITY_ORDER_HEADER = "ORDER_HEADER"


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

DYNAMODB_ENDPOINT = os.environ.get("DYNAMODB_ENDPOINT")
dynamodb_kwargs = {
    "region_name": AWS_REGION
}

if DYNAMODB_ENDPOINT:
    # Local development override
    dynamodb_kwargs["endpoint_url"] = DYNAMODB_ENDPOINT
    dynamodb_kwargs["aws_access_key_id"] = os.environ.get("AWS_ACCESS_KEY_ID", "local")
    dynamodb_kwargs["aws_secret_access_key"] = os.environ.get("AWS_SECRET_ACCESS_KEY", "local")

dynamodb = boto3.resource("dynamodb", **dynamodb_kwargs)
cache_table = dynamodb.Table(DYNAMODB_TABLE)


# ==========================================================
# OpenSearch Client
# ==========================================================

opensearch_client = None
def get_opensearch_client():
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
            connection_class=RequestsHttpConnection,
            timeout=30,
            retry_on_timeout=True,
            max_retries=3
        )
    return opensearch_client


# ==========================================================
# Topic -> Table Mapping
# ==========================================================

TOPIC_TABLE_MAP = {
    "yfs_order_header": "YFS_ORDER_HEADER",
}

def extract_table_from_topic(topic: str) -> str:
    """
    Extract table name from Debezium topic like:
    sterling.public.yfs_order_header
    """
    table_part = topic.rsplit(".", 1)[-1].lower()
    return TOPIC_TABLE_MAP.get(table_part, table_part.upper())


# ==========================================================
# Lambda Handler
# ==========================================================

def lambda_handler(event: dict, context: Any) -> dict:

    try:
        # TODO: Remove raw event logging after initial debugging phase
        logger.info(f"Incoming event sample: {json.dumps(event, default=str)[:5000]}")

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


# ==========================================================
# MSK Batch Processor
# ==========================================================

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
                result = process_single_event(
                    table, operation, payload, source_timestamp
                )
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

# ==========================================================
# Event Router
# ==========================================================

def process_single_event(
        table: str,
        operation: str,
        payload: dict,
        source_timestamp: Optional[int]
) -> dict:

    logger.info(f"Processing {operation} event for table {table}")
    if "YFS_ORDER_HEADER" in table:
        return process_order_event(operation, payload, source_timestamp)
    else:
        logger.warning(f"Unknown table: {table}")
        return {"statusCode": 400, "body": f"Unknown table: {table}"}


# ==========================================================
# Order Processing
# ==========================================================

def process_order_event(operation: str, payload: dict, source_timestamp: Optional[int]) -> dict:

    order_header_key = get_value(payload, "ORDER_HEADER_KEY")
    if not order_header_key:
        return {"statusCode": 400, "body": "Missing ORDER_HEADER_KEY"}
    client = get_opensearch_client()

    if operation == "d":
        if ENABLE_HARD_DELETE:
            client.delete(index=INDEX_ORDER, id=order_header_key, ignore=[404])
            cache_table.delete_item(Key={"PK": ENTITY_ORDER_HEADER, "SK": order_header_key})
            logger.info(f"Order hard-deleted | orderHeaderKey={order_header_key}")
        else:
            soft_delete_os_doc(client, INDEX_ORDER, order_header_key)
            soft_delete_cache_item(ENTITY_ORDER_HEADER, order_header_key)
            logger.info(f"Order soft-deleted | orderHeaderKey={order_header_key}")
        return {"statusCode": 200, "body": f"Deleted order {order_header_key}"}

    order_doc = build_order_doc(payload, operation, source_timestamp)
    client.index(
        index=INDEX_ORDER,
        id=order_header_key,
        body=order_doc
    )

    # Cache order header fields in DynamoDB for order-line enrichment
    cache_order_header_for_lines(order_header_key, order_doc)

    logger.info(
        f"Order indexed | orderHeaderKey={order_header_key} orderNo={order_doc.get('orderNo')}"
    )
    return {"statusCode": 200, "body": f"Indexed order {order_header_key}"}


# ==========================================================
# Build Order Document
# ==========================================================

def build_order_doc(payload: dict, operation: str, ts_ms: Optional[int]) -> dict:
    order_doc = build_base_order_doc(payload, operation, ts_ms)

    apply_enrichments(order_doc, payload)

    order_doc["searchableText"] = build_searchable_text(order_doc)
    
    # Explicitly insert _deleted: False for consistency across all indices.
    # Note: client.index() implicitly clears _deletedAt by overwriting the entire document.
    order_doc["_deleted"] = False

    return {k: v for k, v in order_doc.items() if v is not None}


def build_base_order_doc(payload: dict, operation: str, ts_ms: Optional[int]) -> dict:
    return {
        "orderHeaderKey": get_value(payload, "ORDER_HEADER_KEY"),
        "orderNo": get_value(payload, "ORDER_NO"),
        "enterpriseKey": get_value(payload, "ENTERPRISE_KEY"),
        "documentType": get_value(payload, "DOCUMENT_TYPE"),
        "orderDate": get_timestamp_value(payload, "ORDER_DATE"),
        "reqDeliveryDate": get_timestamp_value(payload, "REQ_DELIVERY_DATE"),
        "reqCancelDate": get_timestamp_value(payload, "REQ_CANCEL_DATE"),
        "reqShipDate": get_timestamp_value(payload, "REQ_SHIP_DATE"),
        "orderType": get_value(payload, "ORDER_TYPE"),
        "orderPurpose": get_value(payload, "ORDER_PURPOSE"),
        "orderName": get_value(payload, "ORDER_NAME"),
        "division": get_value(payload, "DIVISION"),
        "totalAmount": get_float_value(payload, "TOTAL_AMOUNT"),
        "originalTotalAmount": get_float_value(payload, "ORIGINAL_TOTAL_AMOUNT"),
        "totalAdjustmentAmount": get_float_value(payload, "TOTAL_ADJUSTMENT_AMOUNT"),
        "tax": get_float_value(payload, "TAX"),
        "originalTax": get_float_value(payload, "ORIGINAL_TAX"),
        "currency": get_value(payload, "CURRENCY"),
        "enterpriseCurrency": get_value(payload, "ENTERPRISE_CURRENCY"),
        "paymentStatus": get_value(payload, "PAYMENT_STATUS"),
        "termsCode": get_value(payload, "TERMS_CODE"),
        "scac": get_value(payload, "SCAC"),
        "carrierServiceCode": get_value(payload, "CARRIER_SERVICE_CODE"),
        "priceProgramKey": get_value(payload, "PRICE_PROGRAM_KEY"),
        "holdFlag": get_value(payload, "HOLD_FLAG"),
        "holdReasonCode": get_value(payload, "HOLD_REASON_CODE"),
        "draftOrderFlag": get_value(payload, "DRAFT_ORDER_FLAG"),
        "chainType": get_value(payload, "CHAIN_TYPE"),
        "extnPreferredDc": get_value(payload, "EXTN_PREFERRED_DC"),
        "extnIsQuoteOrder": get_value(payload, "EXTN_IS_QUOTE_ORDER"),
        "sellerOrganizationCode": get_value(payload, "SELLER_ORGANIZATION_CODE"),
        "sellerOrgNameHash": None,
        "buyerOrganizationCode": get_value(payload, "BUYER_ORGANIZATION_CODE"),
        "buyerOrgNameHash": None,
        "billToKey": get_value(payload, "BILL_TO_KEY"),
        "billToId": get_value(payload, "BILL_TO_ID"),
        "shipToKey": get_value(payload, "SHIP_TO_KEY"),
        "soldToKey": get_value(payload, "SOLD_TO_KEY"),
        "contactKey": get_value(payload, "CONTACT_KEY"),
        "customer": build_customer_object(payload),
        "statusCode": None,
        "statusName": None,
        "cdcOperation": operation,
        "cdcTimestamp": ts_ms,
        "indexedAt": int(datetime.now(UTC).timestamp() * 1000),
        "createdTimestamp": get_timestamp_value(payload, "CREATETS"),
        "modifiedTimestamp": get_timestamp_value(payload, "MODIFYTS"),
    }


def build_customer_object(payload: dict) -> dict:
    first = get_value(payload, "CUSTOMER_FIRST_NAME")
    last = get_value(payload, "CUSTOMER_LAST_NAME")

    return {
        "customerEmailId": get_value(payload, "CUSTOMER_EMAILID"),
        "customerFirstName": first,
        "customerLastName": last,
        "customerFullName": " ".join(filter(None, [first, last])) or None,
        "customerPhoneNo": get_value(payload, "CUSTOMER_PHONE_NO"),
        "customerPoNo": get_value(payload, "CUSTOMER_PO_NO"),
        "customerZipCode": get_value(payload, "CUSTOMER_ZIP_CODE"),
        "customerRewardsNo": get_value(payload, "CUSTOMER_REWARDS_NO"),
        "customerId": get_value(payload, "BILL_TO_ID"),
    }


def apply_enrichments(order_doc: dict, payload: dict) -> None:
    enrich_person(order_doc, "billToKey", "billTo")
    enrich_person(order_doc, "shipToKey", "shipTo")
    enrich_person(order_doc, "soldToKey", "soldTo")

    enrich_organization(order_doc, "sellerOrganizationCode", "sellerOrganizationName", "sellerOrgNameHash")
    enrich_organization(order_doc, "buyerOrganizationCode", "buyerOrganizationName", "buyerOrgNameHash")

    enrich_customer(order_doc, payload)
    enrich_order_status(order_doc)
    enrich_order_invoice(order_doc)
    enrich_order_hold_types(order_doc)
    enrich_shipment_status(order_doc)


def build_searchable_text(order_doc: dict) -> str:
    parts = [
        order_doc.get("orderNo"),
        order_doc.get("orderHeaderKey"),
        order_doc.get("enterpriseKey"),
        order_doc.get("orderName"),

        # Customer
        order_doc.get("customer", {}).get("customerFirstName"),
        order_doc.get("customer", {}).get("customerLastName"),
        order_doc.get("customer", {}).get("customerEmailId"),
        order_doc.get("customer", {}).get("customerPhoneNo"),
        order_doc.get("customer", {}).get("customerPoNo"),

        order_doc.get("customer", {}).get("organizationCode"),
        order_doc.get("customer", {}).get("organizationName"),

        # Organizations
        order_doc.get("sellerOrganizationName"),
        order_doc.get("buyerOrganizationName"),
        order_doc.get("sellerOrganizationCode"),
        order_doc.get("buyerOrganizationCode"),

        # Order status
        order_doc.get("statusName"),
    ]

    parts.extend(extract_person_fields(order_doc, "billTo"))
    parts.extend(extract_person_fields(order_doc, "shipTo"))
    parts.extend(extract_person_fields(order_doc, "soldTo"))

    return " ".join(filter(None, parts))


def extract_person_fields(order_doc: dict, role: str) -> list:
    person = order_doc.get(role, {})
    return [
        person.get("fullName"),
        person.get("emailId"),
        person.get("mobilePhone"),
        person.get("dayPhone"),
        person.get("eveningPhone"),
        person.get("company"),
        person.get("city"),
        person.get("state"),
    ]

# ==========================================================
# Enrichment Helpers
# ==========================================================

def enrich_person(doc: dict, key_field: str, target_prefix: str):

    key = doc.get(key_field)
    if not key:
        return
    person = get_cached(ENTITY_PERSON_INFO, key)
    if not person:
        return

    doc[target_prefix] = {
        "personInfoKey": key,
        "firstName": person.get("firstName"),
        "middleName": person.get("middleName"),
        "lastName": person.get("lastName"),
        "fullName": build_full_name(person),
        "emailId": person.get("emailId"),
        "mobilePhone": person.get("mobilePhone"),
        "dayPhone": person.get("dayPhone"),
        "eveningPhone": person.get("eveningPhone"),
        "company": person.get("company"),
        "addressLine1": person.get("addressLine1"),
        "addressLine2": person.get("addressLine2"),
        "addressLine3": person.get("addressLine3"),
        "addressLine4": person.get("addressLine4"),
        "addressLine5": person.get("addressLine5"),
        "addressLine6": person.get("addressLine6"),
        "city": person.get("city"),
        "state": person.get("state"),
        "zipCode": person.get("zipCode"),
        "country": person.get("country"),
    }


def enrich_organization(doc: dict, key_field: str, name_field: str, hash_field: str):

    key = doc.get(key_field)
    if not key:
        return
    org = get_cached(ENTITY_ORGANIZATION, key)
    if not org:
        return
    org_name = org.get("organizationName")
    doc[name_field] = org_name
    doc[hash_field] = compute_org_name_hash(org_name)


def enrich_order_status(order_doc: dict) -> None:
    """Enrich order with statusName/statusCode from DynamoDB cache."""
    order_header_key = order_doc.get("orderHeaderKey")
    if not order_header_key:
        return
    cached = get_cached(ENTITY_ORDER_STATUS, order_header_key)
    if not cached:
        return
    order_doc["statusCode"] = cached.get("statusCode")
    order_doc["statusName"] = cached.get("statusName")


def enrich_order_invoice(order_doc: dict) -> None:
    """Enrich order with invoiceNo from DynamoDB cache."""
    order_header_key = order_doc.get("orderHeaderKey")
    if not order_header_key:
        return
    cached = get_cached(ENTITY_ORDER_INVOICE, order_header_key)
    if not cached:
        return
    order_doc["invoiceNo"] = cached.get("invoiceNo")


def enrich_order_hold_types(order_doc: dict) -> None:
    """Enrich order with holdTypeStatuses map from DynamoDB cache."""
    order_header_key = order_doc.get("orderHeaderKey")
    if not order_header_key:
        return
    cached = get_cached(ENTITY_ORDER_HOLD_TYPE, order_header_key)
    if not cached:
        return
    hold_type_statuses = cached.get("holdTypeStatuses")
    if hold_type_statuses and isinstance(hold_type_statuses, dict):
        order_doc["holdTypeStatuses"] = dict(hold_type_statuses)


def enrich_shipment_status(order_doc: dict) -> None:
    """Enrich order with shipmentStatus array from DynamoDB cache."""
    order_header_key = order_doc.get("orderHeaderKey")
    if not order_header_key:
        return
    cached = get_cached(ENTITY_SHIPMENT, order_header_key)
    if not cached:
        return
    shipment_statuses = cached.get("shipmentStatuses")
    if shipment_statuses and isinstance(shipment_statuses, dict):
        unique_statuses = sorted(list(set(s for s in shipment_statuses.values() if s is not None)))
        if unique_statuses:
            order_doc["shipmentStatus"] = unique_statuses


def enrich_customer(order_doc: dict, payload: dict):

    customer_id = get_value(payload, "BILL_TO_ID")
    if not customer_id:
        return

    customer = get_cached(ENTITY_CUSTOMER, customer_id)
    if not customer:
        return

    org_code = customer.get("organizationCode")

    organization_name = None
    if org_code:
        org = get_cached(ENTITY_ORGANIZATION, org_code)
        if org:
            organization_name = org.get("organizationName")

    customer_obj = order_doc.get("customer", {})

    order_bill_to_id = order_doc.get("billToId")
    cached_customer_id = customer.get("customerId")

    # Validation check
    if order_bill_to_id and cached_customer_id and order_bill_to_id != cached_customer_id:
        logger.warning(
            f"Customer mismatch: billToId={order_bill_to_id} "
            f"!= cached customerId={cached_customer_id}"
        )
        return

    customer_obj.update({
        "customerId": customer.get("customerId"),
        "customerKey": customer.get("customerKey"),
        "customerType": customer.get("customerType"),
        "organizationCode": org_code,
        "organizationName": organization_name,
    })

    order_doc["customer"] = customer_obj


# ==========================================================
# DynamoDB Helpers
# ==========================================================

def get_cached(entity: str, key: str) -> Optional[dict]:

    try:
        resp = cache_table.get_item(Key={"PK": entity, "SK": key})
        item = resp.get("Item")
        if item:
            return {k: v for k, v in item.items() if k not in ("PK", "SK", "updatedAt")}
        return None

    except Exception as e:
        logger.error(f"DynamoDB fetch failed | entity={entity} key={key} error={e}", exc_info=True)
        return None


def soft_delete_os_doc(client, index: str, doc_id: str) -> None:
    """Soft-delete an OpenSearch doc by setting _deleted=true instead of removing it."""
    try:
        client.update(
            index=index,
            id=doc_id,
            body={
                "doc": {
                    "_deleted": True,
                    "_deletedAt": int(datetime.now(UTC).timestamp() * 1000),
                }
            },
            refresh=False,
        )
    except Exception as e:
        if '404' not in str(e) and 'not_found' not in str(e):
            raise
        logger.info(f"Soft-delete skipped — doc not found | index={index} id={doc_id}")


def soft_delete_cache_item(pk: str, sk: str) -> None:
    """Soft-delete a DynamoDB cache row by marking it as deleted instead of removing it."""
    cache_table.update_item(
        Key={'PK': pk, 'SK': sk},
        UpdateExpression='SET #d = :true, #da = :now',
        ExpressionAttributeNames={'#d': '_deleted', '#da': '_deletedAt'},
        ExpressionAttributeValues={':true': True, ':now': int(datetime.now(UTC).timestamp() * 1000)},
    )


def cache_order_header_for_lines(order_header_key: str, order_doc: dict) -> None:
    """
    Cache a subset of order header fields in DynamoDB so the
    cdc-orderline-processor can enrich order-line documents without
    querying the OpenSearch orders index.

    Cached fields: documentType, draftOrderFlag, billToId (= customerId),
                   orderNo, orderDate.
    """
    try:
        item = {
            "PK":             ENTITY_ORDER_HEADER,
            "SK":             order_header_key,
            "documentType":   order_doc.get("documentType"),
            "draftOrderFlag": order_doc.get("draftOrderFlag"),
            "billToId":       order_doc.get("billToId"),
            "orderNo":        order_doc.get("orderNo"),
            "orderDate":      order_doc.get("orderDate"),
            "updatedAt":      int(datetime.now(UTC).timestamp() * 1000),
        }
        # Remove None values — DynamoDB doesn't accept None for non-key attributes
        item = {k: v for k, v in item.items() if v is not None}
        cache_table.put_item(Item=item)
        logger.info(
            f"DynamoDB cached order header | "
            f"PK={ENTITY_ORDER_HEADER} SK={order_header_key} "
            f"orderNo={order_doc.get('orderNo')}"
        )
    except Exception as e:
        logger.error(
            f"DynamoDB cache order header FAILED | "
            f"order_header_key={order_header_key} "
            f"error_type={type(e).__name__} error={e}",
            exc_info=True,
        )


def compute_org_name_hash(name: Optional[str]) -> str:
    """Stable 16-char hex hash of org name. Matches the hash stored by cdc-organization-processor."""
    return hashlib.sha256((name or '').encode()).hexdigest()[:16]


def build_full_name(person: dict) -> Optional[str]:

    return " ".join(filter(None, [
        person.get("firstName"),
        person.get("middleName"),
        person.get("lastName"),
    ]))

def get_value(payload: dict, key: str) -> Optional[str]:

    v = payload.get(key)
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None


def get_float_value(payload: dict, key: str) -> Optional[float]:
    """Get float value from payload."""
    value = payload.get(key)
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def get_timestamp_value(payload: dict, key: str) -> Optional[int]:
    """Get timestamp as epoch_millis from payload. Handles Debezium microsecond timestamps."""
    value = payload.get(key)
    if value is None:
        return None
    try:
        ts = int(value)
        # Debezium sends timestamps as microseconds since epoch; convert to millis
        if ts > 1e15:
            return ts // 1000
        # Already in millis
        if ts > 1e12:
            return ts
        # Seconds
        return ts * 1000
    except (ValueError, TypeError):
        return None

def get_iso_timestamp_value(payload: dict, key: str) -> Optional[str]:
    value = payload.get(key)
    if value is None:
        return None

    try:
        ts = int(value)

        # microseconds → seconds
        if ts > 1e15:
            ts = ts / 1_000_000
        # milliseconds → seconds
        elif ts > 1e12:
            ts = ts / 1000

        return datetime.fromtimestamp(ts, UTC).isoformat()

    except (ValueError, TypeError):
        return None




