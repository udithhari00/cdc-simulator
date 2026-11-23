"""
Unit tests for cdc-lambda-orderheader/lambda_function.py

Run with:
    pytest test_lambda_function.py -v
"""

import base64
import json
import os
from unittest.mock import MagicMock, patch

import pytest

os.environ.setdefault("OPENSEARCH_HOST", "localhost")
os.environ.setdefault("OPENSEARCH_AUTH", "basic")
os.environ.setdefault("AWS_REGION", "us-east-1")
os.environ.setdefault("DYNAMODB_ENDPOINT", "http://localhost:8000")

with patch("boto3.resource"), patch("boto3.Session"):
    import lambda_function as lf


# ===========================================================================
# Helpers
# ===========================================================================

def make_payload(**kwargs) -> dict:
    """Return a minimal valid YFS_ORDER_HEADER payload (uppercase keys)."""
    base = {
        "ORDER_HEADER_KEY": "OHK-001",
        "ORDER_NO":         "Y10001",
        "ENTERPRISE_KEY":   "GlenRaven",
        "SELLER_ORGANIZATION_CODE": "SELLER-KEY-001",
        "BUYER_ORGANIZATION_CODE":  "BUYER-KEY-001",
    }
    base.update(kwargs)
    return base


def make_debezium_envelope(payload: dict, op: str = "c", ts_ms: int = 1700000000000) -> dict:
    lc = {k.lower(): v for k, v in payload.items()}
    key = "before" if op == "d" else "after"
    return {"op": op, "ts_ms": ts_ms, key: lc, ("before" if op != "d" else "after"): None}


def make_msk_event(debezium_event: dict, topic: str = "glenraven-dev.STERLING.YFS_ORDER_HEADER") -> dict:
    encoded = base64.b64encode(json.dumps(debezium_event).encode()).decode()
    return {
        "eventSource": "aws:kafka",
        "records": {
            f"{topic}-0": [{"topic": topic, "offset": 0, "value": encoded}]
        }
    }


# ===========================================================================
# Fixtures
# ===========================================================================

@pytest.fixture(autouse=True)
def reset_opensearch_client():
    lf.opensearch_client = None
    yield
    lf.opensearch_client = None


@pytest.fixture
def mock_opensearch():
    client = MagicMock()
    client.index.return_value = {"result": "created"}
    client.delete.return_value = {"result": "deleted"}
    lf.opensearch_client = client
    return client


@pytest.fixture
def mock_cache_table():
    table = MagicMock()
    table.get_item.return_value = {}
    lf.cache_table = table
    return table


# ===========================================================================
# Utility functions
# ===========================================================================

class TestGetValue:
    def test_returns_string(self):
        assert lf.get_value({"K": "v"}, "K") == "v"

    def test_strips_whitespace(self):
        assert lf.get_value({"K": "val  "}, "K") == "val"

    def test_missing_returns_none(self):
        assert lf.get_value({}, "K") is None

    def test_whitespace_only_returns_none(self):
        assert lf.get_value({"K": "   "}, "K") is None

    def test_none_returns_none(self):
        assert lf.get_value({"K": None}, "K") is None


class TestGetFloatValue:
    def test_returns_float(self):
        assert lf.get_float_value({"K": "3.14"}, "K") == 3.14

    def test_integer_string(self):
        assert lf.get_float_value({"K": "100"}, "K") == 100.0

    def test_none_returns_none(self):
        assert lf.get_float_value({"K": None}, "K") is None

    def test_invalid_returns_none(self):
        assert lf.get_float_value({"K": "not-a-number"}, "K") is None


class TestGetTimestampValue:
    def test_microseconds_to_millis(self):
        ts_us = 1700000000000123
        assert lf.get_timestamp_value({"K": ts_us}, "K") == ts_us // 1000

    def test_millis_unchanged(self):
        ts_ms = 1700000000000
        assert lf.get_timestamp_value({"K": ts_ms}, "K") == ts_ms

    def test_seconds_to_millis(self):
        ts_s = 1700000000
        assert lf.get_timestamp_value({"K": ts_s}, "K") == ts_s * 1000

    def test_none_returns_none(self):
        assert lf.get_timestamp_value({"K": None}, "K") is None

    def test_invalid_returns_none(self):
        assert lf.get_timestamp_value({"K": "bad"}, "K") is None


class TestComputeOrgNameHash:
    def test_returns_16_char_hex(self):
        h = lf.compute_org_name_hash("Glen Raven Inc")
        assert len(h) == 16
        assert all(c in "0123456789abcdef" for c in h)

    def test_none_is_stable(self):
        assert lf.compute_org_name_hash(None) == lf.compute_org_name_hash(None)

    def test_different_names_differ(self):
        assert lf.compute_org_name_hash("Acme") != lf.compute_org_name_hash("Globex")

    def test_matches_org_lambda_algorithm(self):
        import hashlib
        name = "Glen Raven Inc"
        expected = hashlib.sha256(name.encode()).hexdigest()[:16]
        assert lf.compute_org_name_hash(name) == expected


class TestBuildFullName:
    def test_all_parts(self):
        assert lf.build_full_name({"firstName": "A", "middleName": "B", "lastName": "C"}) == "A B C"

    def test_first_last_only(self):
        assert lf.build_full_name({"firstName": "Jane", "lastName": "Doe"}) == "Jane Doe"

    def test_empty_dict(self):
        assert lf.build_full_name({}) == ""


# ===========================================================================
# Topic extraction
# ===========================================================================

class TestExtractTableFromTopic:
    def test_known_topic(self):
        assert lf.extract_table_from_topic("glenraven-dev.STERLING.yfs_order_header") == "YFS_ORDER_HEADER"

    def test_unknown_uppercased(self):
        assert lf.extract_table_from_topic("prefix.schema.other_table") == "OTHER_TABLE"


# ===========================================================================
# build_customer_object
# ===========================================================================

class TestBuildCustomerObject:
    def test_full_name_combined(self):
        payload = make_payload(CUSTOMER_FIRST_NAME="Jane", CUSTOMER_LAST_NAME="Doe")
        obj = lf.build_customer_object(payload)
        assert obj["customerFullName"] == "Jane Doe"

    def test_full_name_none_when_empty(self):
        obj = lf.build_customer_object(make_payload())
        assert obj["customerFullName"] is None

    def test_customer_id_from_bill_to_id(self):
        payload = make_payload(BILL_TO_ID="CUST-001")
        obj = lf.build_customer_object(payload)
        assert obj["customerId"] == "CUST-001"

    def test_missing_fields_are_none(self):
        obj = lf.build_customer_object(make_payload())
        assert obj["customerEmailId"] is None
        assert obj["customerPhoneNo"] is None


# ===========================================================================
# build_base_order_doc
# ===========================================================================

class TestBuildBaseOrderDoc:
    def test_core_fields_mapped(self):
        payload = make_payload(ORDER_NAME="Test Order", CURRENCY="USD")
        doc = lf.build_base_order_doc(payload, "c", 1700000000000)
        assert doc["orderHeaderKey"] == "OHK-001"
        assert doc["orderNo"] == "Y10001"
        assert doc["currency"] == "USD"
        assert doc["cdcOperation"] == "c"
        assert doc["cdcTimestamp"] == 1700000000000

    def test_seller_and_buyer_org_hash_initially_none(self):
        doc = lf.build_base_order_doc(make_payload(), "c", None)
        assert doc["sellerOrgNameHash"] is None
        assert doc["buyerOrgNameHash"] is None

    def test_indexed_at_is_set(self):
        doc = lf.build_base_order_doc(make_payload(), "c", None)
        assert isinstance(doc["indexedAt"], int)
        assert doc["indexedAt"] > 0

    def test_customer_object_embedded(self):
        payload = make_payload(CUSTOMER_FIRST_NAME="Jane", BILL_TO_ID="CUST-001")
        doc = lf.build_base_order_doc(payload, "c", None)
        assert isinstance(doc["customer"], dict)
        assert doc["customer"]["customerFirstName"] == "Jane"
        assert doc["customer"]["customerId"] == "CUST-001"


# ===========================================================================
# Enrichment helpers
# ===========================================================================

class TestEnrichOrganization:
    def test_sets_name_and_hash(self, mock_cache_table):
        mock_cache_table.get_item.return_value = {
            "Item": {"PK": "ORGANIZATION", "SK": "SELLER-KEY-001",
                     "organizationName": "Glen Raven Inc"}
        }
        doc = {"sellerOrganizationCode": "SELLER-KEY-001", "sellerOrgNameHash": None}
        lf.enrich_organization(doc, "sellerOrganizationCode", "sellerOrganizationName", "sellerOrgNameHash")
        assert doc["sellerOrganizationName"] == "Glen Raven Inc"
        assert doc["sellerOrgNameHash"] == lf.compute_org_name_hash("Glen Raven Inc")

    def test_no_op_when_key_missing(self, mock_cache_table):
        doc = {}
        lf.enrich_organization(doc, "sellerOrganizationCode", "sellerOrganizationName", "sellerOrgNameHash")
        mock_cache_table.get_item.assert_not_called()
        assert "sellerOrganizationName" not in doc

    def test_no_op_when_cache_miss(self, mock_cache_table):
        mock_cache_table.get_item.return_value = {}
        doc = {"sellerOrganizationCode": "SELLER-KEY-001"}
        lf.enrich_organization(doc, "sellerOrganizationCode", "sellerOrganizationName", "sellerOrgNameHash")
        assert "sellerOrganizationName" not in doc


class TestEnrichPerson:
    def test_sets_person_fields(self, mock_cache_table):
        mock_cache_table.get_item.return_value = {
            "Item": {"PK": "PERSON_INFO", "SK": "BILL-001",
                     "firstName": "Jane", "lastName": "Doe",
                     "emailId": "jane@example.com", "city": "Burlington"}
        }
        doc = {"billToKey": "BILL-001"}
        lf.enrich_person(doc, "billToKey", "billTo")
        assert doc["billTo"]["fullName"] == "Jane Doe"
        assert doc["billTo"]["emailId"] == "jane@example.com"
        assert doc["billTo"]["city"] == "Burlington"

    def test_no_op_when_key_missing(self, mock_cache_table):
        doc = {}
        lf.enrich_person(doc, "billToKey", "billTo")
        assert "billTo" not in doc

    def test_no_op_when_cache_miss(self, mock_cache_table):
        mock_cache_table.get_item.return_value = {}
        doc = {"billToKey": "BILL-001"}
        lf.enrich_person(doc, "billToKey", "billTo")
        assert "billTo" not in doc


class TestEnrichOrderStatus:
    def test_sets_status_when_cached(self, mock_cache_table):
        mock_cache_table.get_item.return_value = {
            "Item": {"PK": "ORDER_STATUS", "SK": "OHK-001",
                     "statusCode": "3700", "statusName": "Shipped"}
        }
        doc = {"orderHeaderKey": "OHK-001", "statusCode": None, "statusName": None}
        lf.enrich_order_status(doc)
        assert doc["statusCode"] == "3700"
        assert doc["statusName"] == "Shipped"

    def test_enrich_not_written_to_order_doc(self, mock_cache_table):
        """enrich data is no longer written to the orders index — it goes to order-lines instead."""
        mock_cache_table.get_item.return_value = {
            "Item": {"PK": "ORDER_STATUS", "SK": "OHK-001",
                     "statusCode": "3700", "statusName": "Shipped",
                     "enrich": {"orderlinekey": [{"OrderLineKey": "L1", "Status": "Shipped", "StatusCode": "3700"}]}}
        }
        doc = {"orderHeaderKey": "OHK-001", "statusCode": None, "statusName": None}
        lf.enrich_order_status(doc)
        assert "enrich" not in doc

    def test_no_op_when_cache_miss(self, mock_cache_table):
        mock_cache_table.get_item.return_value = {}
        doc = {"orderHeaderKey": "OHK-001", "statusCode": None, "statusName": None}
        lf.enrich_order_status(doc)
        assert doc["statusCode"] is None
        assert doc["statusName"] is None

    def test_no_op_when_order_header_key_missing(self, mock_cache_table):
        doc = {"statusCode": None, "statusName": None}
        lf.enrich_order_status(doc)
        mock_cache_table.get_item.assert_not_called()
        assert doc["statusCode"] is None

    def test_status_included_in_indexed_doc(self, mock_opensearch, mock_cache_table):
        mock_cache_table.get_item.side_effect = lambda Key: (
            {"Item": {"PK": "ORDER_STATUS", "SK": Key["SK"],
                      "statusCode": "3700", "statusName": "Shipped"}}
            if Key["PK"] == "ORDER_STATUS" else {}
        )
        lf.process_order_event("c", make_payload(), None)
        doc = mock_opensearch.index.call_args[1]["body"]
        assert doc["statusCode"] == "3700"
        assert doc["statusName"] == "Shipped"
        assert "enrich" not in doc

    def test_status_name_in_searchable_text(self, mock_cache_table):
        mock_cache_table.get_item.side_effect = lambda Key: (
            {"Item": {"PK": "ORDER_STATUS", "SK": Key["SK"],
                      "statusCode": "3700", "statusName": "Shipped"}}
            if Key["PK"] == "ORDER_STATUS" else {}
        )
        doc = lf.build_order_doc(make_payload(), "c", None)
        assert "Shipped" in doc["searchableText"]
        assert doc["statusCode"] == "3700"
        assert doc["statusName"] == "Shipped"

    def test_status_absent_from_searchable_text_when_no_cache(self, mock_cache_table):
        mock_cache_table.get_item.return_value = {}
        doc = lf.build_order_doc(make_payload(), "c", None)
        assert "None" not in doc["searchableText"]


class TestEnrichOrderInvoice:
    def test_sets_invoice_no_when_cached(self, mock_cache_table):
        mock_cache_table.get_item.return_value = {
            "Item": {"PK": "ORDER_INVOICE", "SK": "OHK-001",
                     "invoiceNo": "INV-2026-0001"}
        }
        doc = {"orderHeaderKey": "OHK-001"}
        lf.enrich_order_invoice(doc)
        assert doc["invoiceNo"] == "INV-2026-0001"

    def test_no_op_when_cache_miss(self, mock_cache_table):
        mock_cache_table.get_item.return_value = {}
        doc = {"orderHeaderKey": "OHK-001"}
        lf.enrich_order_invoice(doc)
        assert "invoiceNo" not in doc

    def test_no_op_when_order_header_key_missing(self, mock_cache_table):
        doc = {}
        lf.enrich_order_invoice(doc)
        mock_cache_table.get_item.assert_not_called()

    def test_invoice_included_in_indexed_doc(self, mock_opensearch, mock_cache_table):
        mock_cache_table.get_item.side_effect = lambda Key: (
            {"Item": {"PK": "ORDER_INVOICE", "SK": Key["SK"],
                      "invoiceNo": "INV-9999"}}
            if Key["PK"] == "ORDER_INVOICE" else {}
        )
        lf.process_order_event("c", make_payload(), None)
        doc = mock_opensearch.index.call_args[1]["body"]
        assert doc["invoiceNo"] == "INV-9999"


class TestEnrichOrderHoldTypes:
    def test_sets_hold_types_from_cache_set(self, mock_cache_table):
        mock_cache_table.get_item.return_value = {
            "Item": {"PK": "ORDER_HOLD_TYPE", "SK": "OHK-001",
                     "holdTypes": {"FRAUD_REVIEW", "CREDIT_HOLD"}}
        }
        doc = {"orderHeaderKey": "OHK-001"}
        lf.enrich_order_hold_types(doc)
        assert isinstance(doc["holdTypes"], list)
        assert sorted(doc["holdTypes"]) == ["CREDIT_HOLD", "FRAUD_REVIEW"]

    def test_hold_types_sorted(self, mock_cache_table):
        mock_cache_table.get_item.return_value = {
            "Item": {"PK": "ORDER_HOLD_TYPE", "SK": "OHK-001",
                     "holdTypes": {"Z_HOLD", "A_HOLD", "M_HOLD"}}
        }
        doc = {"orderHeaderKey": "OHK-001"}
        lf.enrich_order_hold_types(doc)
        assert doc["holdTypes"] == ["A_HOLD", "M_HOLD", "Z_HOLD"]

    def test_no_op_when_cache_miss(self, mock_cache_table):
        mock_cache_table.get_item.return_value = {}
        doc = {"orderHeaderKey": "OHK-001"}
        lf.enrich_order_hold_types(doc)
        assert "holdTypes" not in doc

    def test_no_op_when_order_header_key_missing(self, mock_cache_table):
        doc = {}
        lf.enrich_order_hold_types(doc)
        mock_cache_table.get_item.assert_not_called()

    def test_no_op_when_hold_types_empty(self, mock_cache_table):
        """After all hold types removed, DynamoDB item exists but holdTypes attr is gone."""
        mock_cache_table.get_item.return_value = {
            "Item": {"PK": "ORDER_HOLD_TYPE", "SK": "OHK-001"}
        }
        doc = {"orderHeaderKey": "OHK-001"}
        lf.enrich_order_hold_types(doc)
        assert "holdTypes" not in doc

    def test_hold_types_included_in_indexed_doc(self, mock_opensearch, mock_cache_table):
        mock_cache_table.get_item.side_effect = lambda Key: (
            {"Item": {"PK": "ORDER_HOLD_TYPE", "SK": Key["SK"],
                      "holdTypes": {"PAYMENT_HOLD"}}}
            if Key["PK"] == "ORDER_HOLD_TYPE" else {}
        )
        lf.process_order_event("c", make_payload(), None)
        doc = mock_opensearch.index.call_args[1]["body"]
        assert doc["holdTypes"] == ["PAYMENT_HOLD"]
class TestEnrichCustomer:
    def test_enriches_customer_fields(self, mock_cache_table):
        mock_cache_table.get_item.return_value = {
            "Item": {"PK": "CUSTOMER", "SK": "CUST-001",
                     "customerId": "CUST-001", "customerKey": "CKEY-001",
                     "customerType": "B", "organizationCode": "ORG-CODE"}
        }
        payload = make_payload(BILL_TO_ID="CUST-001")
        order_doc = lf.build_base_order_doc(payload, "c", None)
        lf.enrich_customer(order_doc, payload)
        assert order_doc["customer"]["customerId"] == "CUST-001"
        assert order_doc["customer"]["customerType"] == "B"
        assert order_doc["customer"]["organizationCode"] == "ORG-CODE"

    def test_no_op_when_no_bill_to_id(self, mock_cache_table):
        payload = make_payload()
        order_doc = lf.build_base_order_doc(payload, "c", None)
        lf.enrich_customer(order_doc, payload)
        mock_cache_table.get_item.assert_not_called()

    def test_skips_on_customer_id_mismatch(self, mock_cache_table):
        mock_cache_table.get_item.return_value = {
            "Item": {"PK": "CUSTOMER", "SK": "CUST-001",
                     "customerId": "CUST-999"}  # mismatch
        }
        payload = make_payload(BILL_TO_ID="CUST-001")
        order_doc = lf.build_base_order_doc(payload, "c", None)
        order_doc["billToId"] = "CUST-001"
        lf.enrich_customer(order_doc, payload)
        # customerId should not have been updated from cache
        assert order_doc["customer"].get("customerKey") is None


# ===========================================================================
# build_searchable_text
# ===========================================================================

class TestBuildSearchableText:
    def _build_doc(self, mock_cache_table, payload_overrides=None):
        payload = make_payload(**(payload_overrides or {}))
        mock_cache_table.get_item.return_value = {}
        return lf.build_order_doc(payload, "c", None)

    def test_contains_order_no_and_key(self, mock_cache_table):
        doc = self._build_doc(mock_cache_table)
        assert "Y10001" in doc["searchableText"]
        assert "OHK-001" in doc["searchableText"]

    def test_contains_customer_name(self, mock_cache_table):
        doc = self._build_doc(mock_cache_table, {"CUSTOMER_FIRST_NAME": "Jane", "CUSTOMER_LAST_NAME": "Doe"})
        assert "Jane" in doc["searchableText"]
        assert "Doe" in doc["searchableText"]

    def test_contains_org_codes(self, mock_cache_table):
        doc = self._build_doc(mock_cache_table)
        assert "SELLER-KEY-001" in doc["searchableText"]
        assert "BUYER-KEY-001" in doc["searchableText"]

    def test_no_none_tokens(self, mock_cache_table):
        doc = self._build_doc(mock_cache_table)
        assert "None" not in doc["searchableText"]

    def test_includes_bill_to_person_fields(self, mock_cache_table):
        mock_cache_table.get_item.side_effect = lambda Key: (
            {"Item": {"PK": "PERSON_INFO", "SK": Key["SK"],
                      "firstName": "Bill", "lastName": "Person",
                      "city": "Burlington", "emailId": "bill@example.com"}}
            if Key["PK"] == "PERSON_INFO" else {}
        )
        payload = make_payload(BILL_TO_KEY="BILL-001")
        doc = lf.build_order_doc(payload, "c", None)
        assert "Burlington" in doc["searchableText"]
        assert "bill@example.com" in doc["searchableText"]

    def test_includes_seller_org_name_when_enriched(self, mock_cache_table):
        mock_cache_table.get_item.side_effect = lambda Key: (
            {"Item": {"PK": "ORGANIZATION", "SK": Key["SK"],
                      "organizationName": "Glen Raven Inc"}}
            if Key["PK"] == "ORGANIZATION" else {}
        )
        payload = make_payload()
        doc = lf.build_order_doc(payload, "c", None)
        assert "Glen Raven Inc" in doc["searchableText"]


# ===========================================================================
# process_order_event
# ===========================================================================

class TestProcessOrderEvent:
    def test_missing_order_header_key_returns_400(self, mock_opensearch, mock_cache_table):
        result = lf.process_order_event("c", {}, None)
        assert result["statusCode"] == 400

    def test_create_indexes_with_order_header_key(self, mock_opensearch, mock_cache_table):
        result = lf.process_order_event("c", make_payload(), 1700000000000)
        assert result["statusCode"] == 200
        mock_opensearch.index.assert_called_once()
        assert mock_opensearch.index.call_args[1]["id"] == "OHK-001"
        assert mock_opensearch.index.call_args[1]["index"] == lf.INDEX_ORDER

    def test_delete_calls_opensearch_delete(self, mock_opensearch, mock_cache_table):
        result = lf.process_order_event("d", make_payload(), None)
        assert result["statusCode"] == 200
        mock_opensearch.delete.assert_called_once_with(
            index=lf.INDEX_ORDER, id="OHK-001", ignore=[404]
        )
        mock_opensearch.index.assert_not_called()

    def test_doc_has_seller_org_hash_after_enrichment(self, mock_opensearch, mock_cache_table):
        mock_cache_table.get_item.side_effect = lambda Key: (
            {"Item": {"PK": "ORGANIZATION", "SK": Key["SK"],
                      "organizationName": "Glen Raven Inc"}}
            if Key["PK"] == "ORGANIZATION" else {}
        )
        lf.process_order_event("c", make_payload(), None)
        doc = mock_opensearch.index.call_args[1]["body"]
        assert doc.get("sellerOrgNameHash") == lf.compute_org_name_hash("Glen Raven Inc")
        assert doc.get("buyerOrgNameHash") == lf.compute_org_name_hash("Glen Raven Inc")

    def test_none_fields_excluded_from_doc(self, mock_opensearch, mock_cache_table):
        lf.process_order_event("c", make_payload(), None)
        doc = mock_opensearch.index.call_args[1]["body"]
        assert all(v is not None for v in doc.values())


# ===========================================================================
# MSK batch processor
# ===========================================================================

class TestProcessMskEvent:
    def _make_event(self, op="c", extra=None):
        env = make_debezium_envelope(make_payload(**(extra or {})), op=op)
        return make_msk_event(env)

    def test_successful_batch_returns_200(self, mock_opensearch, mock_cache_table):
        result = lf.process_msk_event(self._make_event())
        assert result["statusCode"] == 200
        assert "Processed 1" in result["body"]

    def test_delete_decoded_from_before(self, mock_opensearch, mock_cache_table):
        result = lf.process_msk_event(self._make_event(op="d"))
        assert result["statusCode"] == 200
        mock_opensearch.delete.assert_called_once()

    def test_payload_keys_uppercased(self, mock_opensearch, mock_cache_table):
        env = make_debezium_envelope(make_payload())
        # Verify envelope has lowercase keys as Debezium sends
        assert "order_header_key" in env["after"]
        lf.process_msk_event(make_msk_event(env))
        mock_opensearch.index.assert_called_once()

    def test_malformed_value_counted_as_error(self, mock_opensearch, mock_cache_table):
        event = {
            "eventSource": "aws:kafka",
            "records": {"topic-0": [{"topic": "topic", "offset": 0, "value": "!!!bad!!!"}]}
        }
        result = lf.process_msk_event(event)
        assert "errors 1" in result["body"]

    def test_multiple_messages_processed(self, mock_opensearch, mock_cache_table):
        topic = "glenraven-dev.STERLING.yfs_order_header"

        def make_msg(key):
            env = make_debezium_envelope(make_payload(ORDER_HEADER_KEY=key))
            return {"topic": topic, "offset": 0,
                    "value": base64.b64encode(json.dumps(env).encode()).decode()}

        event = {
            "eventSource": "aws:kafka",
            "records": {f"{topic}-0": [make_msg("K1"), make_msg("K2"), make_msg("K3")]}
        }
        result = lf.process_msk_event(event)
        assert result["statusCode"] == 200
        assert "Processed 3" in result["body"]


# ===========================================================================
# lambda_handler
# ===========================================================================

class TestLambdaHandler:
    def test_msk_event_routes_correctly(self, mock_opensearch, mock_cache_table):
        event = make_msk_event(make_debezium_envelope(make_payload()))
        result = lf.lambda_handler(event, None)
        assert result["statusCode"] == 200

    def test_direct_invocation_routes_correctly(self, mock_opensearch, mock_cache_table):
        event = {"table": "YFS_ORDER_HEADER", "operation": "c", "payload": make_payload()}
        result = lf.lambda_handler(event, None)
        assert result["statusCode"] == 200

    def test_unhandled_exception_returns_500(self, mock_opensearch, mock_cache_table):
        with patch.object(lf, "process_msk_event", side_effect=RuntimeError("boom")):
            result = lf.lambda_handler({"eventSource": "aws:kafka", "records": {}}, None)
        assert result["statusCode"] == 500
        assert "boom" in result["body"]

    def test_unknown_table_returns_400(self, mock_opensearch, mock_cache_table):
        event = {"table": "UNKNOWN_TABLE", "operation": "c", "payload": make_payload()}
        result = lf.lambda_handler(event, None)
        assert result["statusCode"] == 400


# ===========================================================================
# _parse_debezium_message
# ===========================================================================

class TestParseDebeziumMessage:
    def test_flattened_smt_format_detected(self):
        event = {
            "schema": {"type": "struct", "fields": []},
            "payload": {
                "ORDER_HEADER_KEY": "OHK-001",
                "ORDER_NO": "Y10001",
                "__op": "u",
                "__source_ts_ms": 1773154201000
            }
        }
        op, ts, payload = lf._parse_debezium_message(event)
        assert op == "u"
        assert ts == 1773154201000
        assert "ORDER_HEADER_KEY" in payload
        assert "__op" not in payload
        assert "__source_ts_ms" not in payload

    def test_standard_envelope_format_create(self):
        event = {
            "op": "c",
            "ts_ms": 1700000000000,
            "before": None,
            "after": {"order_header_key": "OHK-001"}
        }
        op, ts, payload = lf._parse_debezium_message(event)
        assert op == "c"
        assert ts == 1700000000000
        assert "order_header_key" in payload

    def test_standard_envelope_delete_uses_before(self):
        event = {
            "op": "d",
            "ts_ms": 1700000000000,
            "before": {"order_header_key": "OHK-001"},
            "after": None
        }
        op, ts, payload = lf._parse_debezium_message(event)
        assert op == "d"
        assert "order_header_key" in payload

    def test_null_op_in_smt_defaults_to_c(self):
        event = {
            "schema": {},
            "payload": {"ORDER_HEADER_KEY": "OHK-001", "__op": None, "__source_ts_ms": 0}
        }
        op, ts, payload = lf._parse_debezium_message(event)
        assert op == "c"

    def test_dunder_keys_stripped_from_smt_payload(self):
        event = {
            "schema": {},
            "payload": {
                "ORDER_NO": "Y10001",
                "__op": "r",
                "__source_ts_ms": 1773000000000,
                "__deleted": "false"
            }
        }
        _, _, payload = lf._parse_debezium_message(event)
        for k in payload:
            assert not k.startswith("__")


# ===========================================================================
# Real YFS_ORDER_HEADER CDC event (flattened SMT format)
# ===========================================================================

REAL_ORDER_HEADER_CDC_EVENT = {
    "schema": {
        "type": "struct",
        "fields": [],
        "optional": False,
        "name": "glenraven.STERLING.YFS_ORDER_HEADER.Value"
    },
    "payload": {
        "ORDER_HEADER_KEY": "202406111239006158392735",
        "ORDER_NO": "WC_4481801",
        "ENTERPRISE_KEY": "TV          ",
        "DOCUMENT_TYPE": "0001",
        "SELLER_ORGANIZATION_CODE": "TV          ",
        "BUYER_ORGANIZATION_CODE": "T100137127_762          ",
        "BILL_TO_KEY": None,
        "BILL_TO_ID": "10309501",
        "SHIP_TO_KEY": None,
        "SOLD_TO_KEY": None,
        "CONTACT_KEY": None,
        "PRICE_PROGRAM_KEY": None,
        "SCAC": "UPSN",
        "CARRIER_SERVICE_CODE": "Ground",
        "TERMS_CODE": None,
        "DIVISION": None,
        "DRAFT_ORDER_FLAG": "N",
        "HOLD_FLAG": "N",
        "HOLD_REASON_CODE": None,
        "ORDER_PURPOSE": None,
        "PAYMENT_STATUS": "AUTHORIZED",
        "ORDER_TYPE": "W",
        "CURRENCY": "USD",
        "ENTERPRISE_CURRENCY": "USD",
        "ORDER_DATE": 1718105674000,
        "REQ_DELIVERY_DATE": None,
        "REQ_CANCEL_DATE": 16725225600000,
        "REQ_SHIP_DATE": None,
        "ORDER_NAME": None,
        "TOTAL_AMOUNT": 1564.05,
        "ORIGINAL_TOTAL_AMOUNT": 1564.05,
        "TOTAL_ADJUSTMENT_AMOUNT": None,
        "TAX": 6.62,
        "ORIGINAL_TAX": 6.62,
        "CUSTOMER_EMAILID": "MATT@EXCLUSIVERETRACTABLEAWNINGS.COM",
        "CUSTOMER_FIRST_NAME": "MATTHEW",
        "CUSTOMER_LAST_NAME": "SHORT",
        "CUSTOMER_PHONE_NO": None,
        "CUSTOMER_PO_NO": None,
        "CUSTOMER_ZIP_CODE": "60805-1026",
        "CUSTOMER_REWARDS_NO": None,
        "CREATETS": 1718109540000,
        "MODIFYTS": 1773139800000,
        "__op": "u",
        "__source_ts_ms": 1773154201000,
        "__deleted": "false"
    }
}


def _make_msk_event_from_raw(raw_event: dict, topic: str = "glenraven-prod.STERLING.YFS_ORDER_HEADER") -> dict:
    """Wrap a raw CDC event dict as an MSK trigger (base64-encoded JSON value)."""
    encoded = base64.b64encode(json.dumps(raw_event).encode()).decode()
    return {
        "eventSource": "aws:kafka",
        "records": {
            f"{topic}-0": [{"topic": topic, "offset": 0, "value": encoded}]
        }
    }


class TestRealOrderHeaderCdcEvent:
    """End-to-end tests using a real YFS_ORDER_HEADER CDC event (flattened SMT format)."""

    @pytest.fixture(autouse=True)
    def setup(self, mock_opensearch, mock_cache_table):
        mock_cache_table.get_item.return_value = {}
        self.mock_os = mock_opensearch
        self.msk_event = _make_msk_event_from_raw(REAL_ORDER_HEADER_CDC_EVENT)

    def _get_indexed_doc(self) -> dict:
        lf.process_msk_event(self.msk_event)
        return self.mock_os.index.call_args[1]["body"]

    def test_processes_successfully(self):
        result = lf.process_msk_event(self.msk_event)
        assert result["statusCode"] == 200
        assert "Processed 1" in result["body"]
        assert "errors 0" in result["body"]

    def test_order_header_key_as_document_id(self):
        lf.process_msk_event(self.msk_event)
        assert self.mock_os.index.call_args[1]["id"] == "202406111239006158392735"

    def test_indexed_to_correct_index(self):
        lf.process_msk_event(self.msk_event)
        assert self.mock_os.index.call_args[1]["index"] == lf.INDEX_ORDER

    def test_order_no(self):
        doc = self._get_indexed_doc()
        assert doc["orderNo"] == "WC_4481801"

    def test_enterprise_key_stripped(self):
        doc = self._get_indexed_doc()
        assert doc["enterpriseKey"] == "TV"

    def test_seller_org_code_stripped(self):
        doc = self._get_indexed_doc()
        assert doc["sellerOrganizationCode"] == "TV"

    def test_buyer_org_code_stripped(self):
        doc = self._get_indexed_doc()
        assert doc["buyerOrganizationCode"] == "T100137127_762"

    def test_order_date_epoch_millis(self):
        doc = self._get_indexed_doc()
        assert doc["orderDate"] == 1718105674000

    def test_req_cancel_date_epoch_millis(self):
        doc = self._get_indexed_doc()
        assert doc["reqCancelDate"] == 16725225600000

    def test_tax_as_float(self):
        doc = self._get_indexed_doc()
        assert doc["tax"] == 6.62

    def test_original_tax(self):
        doc = self._get_indexed_doc()
        assert doc["originalTax"] == 6.62

    def test_total_amount_as_float(self):
        doc = self._get_indexed_doc()
        assert doc["totalAmount"] == 1564.05

    def test_original_total_amount(self):
        doc = self._get_indexed_doc()
        assert doc["originalTotalAmount"] == 1564.05

    def test_currency(self):
        doc = self._get_indexed_doc()
        assert doc["currency"] == "USD"

    def test_enterprise_currency(self):
        doc = self._get_indexed_doc()
        assert doc["enterpriseCurrency"] == "USD"

    def test_scac(self):
        doc = self._get_indexed_doc()
        assert doc["scac"] == "UPSN"

    def test_carrier_service_code(self):
        doc = self._get_indexed_doc()
        assert doc["carrierServiceCode"] == "Ground"

    def test_payment_status(self):
        doc = self._get_indexed_doc()
        assert doc["paymentStatus"] == "AUTHORIZED"

    def test_cdc_operation_is_update(self):
        doc = self._get_indexed_doc()
        assert doc["cdcOperation"] == "u"

    def test_cdc_timestamp_from_source_ts_ms(self):
        doc = self._get_indexed_doc()
        assert doc["cdcTimestamp"] == 1773154201000

    def test_created_timestamp(self):
        doc = self._get_indexed_doc()
        assert doc["createdTimestamp"] == 1718109540000

    def test_modified_timestamp(self):
        doc = self._get_indexed_doc()
        assert doc["modifiedTimestamp"] == 1773139800000

    def test_customer_email(self):
        doc = self._get_indexed_doc()
        assert doc["customer"]["customerEmailId"] == "MATT@EXCLUSIVERETRACTABLEAWNINGS.COM"

    def test_customer_first_name(self):
        doc = self._get_indexed_doc()
        assert doc["customer"]["customerFirstName"] == "MATTHEW"

    def test_customer_last_name(self):
        doc = self._get_indexed_doc()
        assert doc["customer"]["customerLastName"] == "SHORT"

    def test_customer_full_name(self):
        doc = self._get_indexed_doc()
        assert doc["customer"]["customerFullName"] == "MATTHEW SHORT"

    def test_customer_zip_code(self):
        doc = self._get_indexed_doc()
        assert doc["customer"]["customerZipCode"] == "60805-1026"

    def test_customer_id_from_bill_to_id(self):
        doc = self._get_indexed_doc()
        assert doc["customer"]["customerId"] == "10309501"

    def test_null_fields_excluded(self):
        doc = self._get_indexed_doc()
        for k, v in doc.items():
            assert v is not None, f"Field '{k}' should be excluded when None"

    def test_searchable_text_contains_order_no(self):
        doc = self._get_indexed_doc()
        assert "WC_4481801" in doc["searchableText"]

    def test_searchable_text_contains_customer_name(self):
        doc = self._get_indexed_doc()
        assert "MATTHEW" in doc["searchableText"]
        assert "SHORT" in doc["searchableText"]

    def test_searchable_text_contains_org_codes(self):
        doc = self._get_indexed_doc()
        assert "TV" in doc["searchableText"]
        assert "T100137127_762" in doc["searchableText"]

    def test_no_none_tokens_in_searchable_text(self):
        doc = self._get_indexed_doc()
        assert "None" not in doc["searchableText"]

    def test_indexed_at_is_recent_epoch_millis(self):
        doc = self._get_indexed_doc()
        assert isinstance(doc["indexedAt"], int)
        assert doc["indexedAt"] > 1_700_000_000_000


# ===========================================================================
# Soft Delete — cdc-orderheader-processor
# ===========================================================================

class TestSoftDeleteOrderHeader:
    """
    Tests for the ENABLE_HARD_DELETE toggle on the order header lambda.

    Soft delete (default, ENABLE_HARD_DELETE=false):
      - OpenSearch document is updated with _deleted=True and _deletedAt (epoch ms)
      - client.delete() is NOT called

    Hard delete (ENABLE_HARD_DELETE=true):
      - client.delete() is called with the order header key and ignore=[404]
      - client.update() is NOT called for the delete
    """

    @pytest.fixture(autouse=True)
    def setup(self, mock_opensearch, mock_cache_table):
        self.os = mock_opensearch
        self.table = mock_cache_table

    # ------------------------------------------------------------------
    # Soft delete (default)
    # ------------------------------------------------------------------

    def test_soft_delete_calls_update_not_delete(self):
        with patch.dict("os.environ", {"ENABLE_HARD_DELETE": "false"}):
            result = lf.process_order_event("d", make_payload(), None)
        assert result["statusCode"] == 200
        self.os.update.assert_called_once()
        self.os.delete.assert_not_called()

    def test_soft_delete_sets_deleted_true(self):
        with patch.dict("os.environ", {"ENABLE_HARD_DELETE": "false"}):
            lf.process_order_event("d", make_payload(), None)
        doc = self.os.update.call_args[1]["body"]["doc"]
        assert doc["_deleted"] is True

    def test_soft_delete_sets_deleted_at_epoch_millis(self):
        with patch.dict("os.environ", {"ENABLE_HARD_DELETE": "false"}):
            lf.process_order_event("d", make_payload(), None)
        doc = self.os.update.call_args[1]["body"]["doc"]
        assert isinstance(doc["_deletedAt"], int)
        assert doc["_deletedAt"] > 1_700_000_000_000

    def test_soft_delete_targets_correct_id(self):
        with patch.dict("os.environ", {"ENABLE_HARD_DELETE": "false"}):
            lf.process_order_event("d", make_payload(), None)
        call_kwargs = self.os.update.call_args[1]
        assert call_kwargs["id"] == "OHK-001"
        assert call_kwargs["index"] == lf.INDEX_ORDER

    # ------------------------------------------------------------------
    # Hard delete
    # ------------------------------------------------------------------

    def test_hard_delete_calls_client_delete(self):
        with patch.dict("os.environ", {"ENABLE_HARD_DELETE": "true"}):
            result = lf.process_order_event("d", make_payload(), None)
        assert result["statusCode"] == 200
        self.os.delete.assert_called_once_with(
            index=lf.INDEX_ORDER, id="OHK-001", ignore=[404]
        )
        self.os.update.assert_not_called()

    def test_hard_delete_ignores_404(self):
        self.os.delete.return_value = {"result": "not_found"}
        with patch.dict("os.environ", {"ENABLE_HARD_DELETE": "true"}):
            result = lf.process_order_event("d", make_payload(), None)
        assert result["statusCode"] == 200

    # ------------------------------------------------------------------
    # Toggle boundary — verify env var is respected exactly
    # ------------------------------------------------------------------

    def test_default_behaviour_is_soft_delete(self):
        """Without explicitly setting ENABLE_HARD_DELETE, soft delete must be active."""
        import os as _os
        _os.environ.pop("ENABLE_HARD_DELETE", None)
        try:
            lf.process_order_event("d", make_payload(), None)
        finally:
            pass  # restore not needed; fixture resets client
        self.os.update.assert_called_once()
        self.os.delete.assert_not_called()
