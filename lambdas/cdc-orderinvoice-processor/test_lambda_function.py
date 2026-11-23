"""
Unit tests for cdc-orderinvoice-processor/lambda_function.py

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
    """Return a minimal valid YFS_ORDER_INVOICE payload (uppercase keys)."""
    base = {
        "ORDER_HEADER_KEY": "OHK-001",
        "INVOICE_NO":       "INV-2026-0001",
    }
    base.update(kwargs)
    return base


def make_debezium_envelope(payload: dict, op: str = "c", ts_ms: int = 1700000000000) -> dict:
    lc = {k.lower(): v for k, v in payload.items()}
    key = "before" if op == "d" else "after"
    return {"op": op, "ts_ms": ts_ms, key: lc, ("before" if op != "d" else "after"): None}


def make_msk_event(debezium_event: dict, topic: str = "sterling.public.yfs_order_invoice") -> dict:
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
    client.update.return_value = {"result": "updated"}
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


class TestNowMs:
    def test_returns_int(self):
        assert isinstance(lf.now_ms(), int)

    def test_returns_recent_timestamp(self):
        assert lf.now_ms() > 1_700_000_000_000


# ===========================================================================
# Topic extraction
# ===========================================================================

class TestExtractTableFromTopic:
    def test_known_topic(self):
        assert lf.extract_table_from_topic("sterling.public.yfs_order_invoice") == "YFS_ORDER_INVOICE"

    def test_unknown_uppercased(self):
        assert lf.extract_table_from_topic("prefix.schema.other_table") == "OTHER_TABLE"


# ===========================================================================
# _parse_debezium_message
# ===========================================================================

class TestParseDebeziumMessage:
    def test_flattened_smt_format_detected(self):
        event = {
            "schema": {"type": "struct", "fields": []},
            "payload": {
                "ORDER_HEADER_KEY": "OHK-001",
                "INVOICE_NO": "INV-001",
                "__op": "u",
                "__source_ts_ms": 1773154201000
            }
        }
        op, ts, payload = lf._parse_debezium_message(event)
        assert op == "u"
        assert ts == 1773154201000
        assert "ORDER_HEADER_KEY" in payload
        assert "__op" not in payload

    def test_standard_envelope_create(self):
        event = {
            "op": "c", "ts_ms": 1700000000000,
            "before": None, "after": {"ORDER_HEADER_KEY": "OHK-001", "INVOICE_NO": "INV-001"}
        }
        op, ts, payload = lf._parse_debezium_message(event)
        assert op == "c"
        assert "ORDER_HEADER_KEY" in payload

    def test_standard_envelope_delete_uses_before(self):
        event = {
            "op": "d", "ts_ms": 1700000000000,
            "before": {"ORDER_HEADER_KEY": "OHK-001", "INVOICE_NO": "INV-001"}, "after": None
        }
        op, ts, payload = lf._parse_debezium_message(event)
        assert op == "d"
        assert "ORDER_HEADER_KEY" in payload


# ===========================================================================
# DynamoDB cache — save_invoice_cache
# ===========================================================================

class TestSaveInvoiceCache:
    def test_puts_item_with_correct_keys(self, mock_cache_table):
        lf.save_invoice_cache("OHK-001", "INV-001")
        mock_cache_table.put_item.assert_called_once()
        item = mock_cache_table.put_item.call_args[1]["Item"]
        assert item["PK"] == "ORDER_INVOICE"
        assert item["SK"] == "OHK-001"
        assert item["invoiceNo"] == "INV-001"

    def test_includes_updated_at_timestamp(self, mock_cache_table):
        lf.save_invoice_cache("OHK-001", "INV-001")
        item = mock_cache_table.put_item.call_args[1]["Item"]
        assert isinstance(item["updatedAt"], int)
        assert item["updatedAt"] > 1_700_000_000_000


# ===========================================================================
# DynamoDB cache — delete_invoice_cache (phantom delete prevention)
# ===========================================================================

class TestDeleteInvoiceCache:
    def test_soft_delete_uses_condition_expression(self, mock_cache_table):
        """Soft delete should guard with attribute_exists to prevent phantom records."""
        lf.delete_invoice_cache("OHK-001")
        mock_cache_table.update_item.assert_called_once()
        kwargs = mock_cache_table.update_item.call_args[1]
        assert "ConditionExpression" in kwargs
        assert "attribute_exists" in kwargs["ConditionExpression"]

    def test_soft_delete_sets_deleted_flag(self, mock_cache_table):
        lf.delete_invoice_cache("OHK-001")
        kwargs = mock_cache_table.update_item.call_args[1]
        assert "#d" in kwargs["ExpressionAttributeNames"]
        assert kwargs["ExpressionAttributeValues"][":true"] is True

    def test_phantom_delete_caught_gracefully(self, mock_cache_table):
        """If item doesn't exist, ConditionalCheckFailedException should be caught."""
        from botocore.exceptions import ClientError
        mock_cache_table.update_item.side_effect = ClientError(
            {"Error": {"Code": "ConditionalCheckFailedException", "Message": "condition"}},
            "UpdateItem"
        )
        # Should not raise
        lf.delete_invoice_cache("OHK-PHANTOM")

    def test_non_conditional_error_re_raised(self, mock_cache_table):
        """Other ClientError codes should be re-raised."""
        from botocore.exceptions import ClientError
        mock_cache_table.update_item.side_effect = ClientError(
            {"Error": {"Code": "InternalServerError", "Message": "boom"}},
            "UpdateItem"
        )
        with pytest.raises(ClientError):
            lf.delete_invoice_cache("OHK-001")

    def test_hard_delete_calls_delete_item(self, mock_cache_table):
        with patch.object(lf, "ENABLE_HARD_DELETE", True):
            lf.delete_invoice_cache("OHK-001")
        mock_cache_table.delete_item.assert_called_once()
        mock_cache_table.update_item.assert_not_called()


# ===========================================================================
# OpenSearch — update_invoice_in_index
# ===========================================================================

class TestUpdateInvoiceInIndex:
    def test_sets_invoice_no_on_order(self, mock_opensearch):
        lf.update_invoice_in_index(mock_opensearch, "OHK-001", "INV-001")
        mock_opensearch.update.assert_called_once()
        kwargs = mock_opensearch.update.call_args[1]
        assert kwargs["index"] == lf.INDEX_ORDER
        assert kwargs["id"] == "OHK-001"
        assert kwargs["body"]["doc"]["invoiceNo"] == "INV-001"

    def test_404_error_logged_not_raised(self, mock_opensearch):
        mock_opensearch.update.side_effect = Exception("404 not_found")
        # Should not raise — logs a warning instead
        lf.update_invoice_in_index(mock_opensearch, "OHK-MISSING", "INV-001")


# ===========================================================================
# OpenSearch — clear_invoice_in_index
# ===========================================================================

class TestClearInvoiceInIndex:
    def test_sets_invoice_no_to_none(self, mock_opensearch):
        lf.clear_invoice_in_index(mock_opensearch, "OHK-001")
        kwargs = mock_opensearch.update.call_args[1]
        assert kwargs["body"]["doc"]["invoiceNo"] is None

    def test_404_error_logged_not_raised(self, mock_opensearch):
        mock_opensearch.update.side_effect = Exception("404 not_found")
        lf.clear_invoice_in_index(mock_opensearch, "OHK-MISSING")


# ===========================================================================
# process_order_invoice_event
# ===========================================================================

class TestProcessOrderInvoiceEvent:
    def test_missing_order_header_key_returns_400(self, mock_opensearch, mock_cache_table):
        result = lf.process_order_invoice_event("c", {"INVOICE_NO": "INV-001"})
        assert result["statusCode"] == 400
        assert "ORDER_HEADER_KEY" in result["body"]

    def test_missing_invoice_no_on_create_returns_400(self, mock_opensearch, mock_cache_table):
        result = lf.process_order_invoice_event("c", {"ORDER_HEADER_KEY": "OHK-001"})
        assert result["statusCode"] == 400
        assert "INVOICE_NO" in result["body"]

    def test_create_saves_cache_and_updates_index(self, mock_opensearch, mock_cache_table):
        result = lf.process_order_invoice_event("c", make_payload())
        assert result["statusCode"] == 200
        mock_cache_table.put_item.assert_called_once()
        mock_opensearch.update.assert_called_once()

    def test_delete_clears_cache_and_index(self, mock_opensearch, mock_cache_table):
        result = lf.process_order_invoice_event("d", make_payload())
        assert result["statusCode"] == 200
        # Soft delete calls update_item, not put_item
        mock_cache_table.update_item.assert_called_once()
        # Index clear
        mock_opensearch.update.assert_called_once()

    def test_delete_allows_missing_invoice_no(self, mock_opensearch, mock_cache_table):
        """Delete events from Debezium may have the INVOICE_NO; it's not required."""
        result = lf.process_order_invoice_event("d", {"ORDER_HEADER_KEY": "OHK-001"})
        assert result["statusCode"] == 200


# ===========================================================================
# MSK Batch Processor
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

    def test_malformed_value_counted_as_error(self, mock_opensearch, mock_cache_table):
        event = {
            "eventSource": "aws:kafka",
            "records": {"topic-0": [{"topic": "topic", "offset": 0, "value": "!!!bad!!!"}]}
        }
        result = lf.process_msk_event(event)
        assert "errors 1" in result["body"]

    def test_tombstone_record_skipped(self, mock_opensearch, mock_cache_table):
        event = {
            "eventSource": "aws:kafka",
            "records": {"topic-0": [{"topic": "topic", "offset": 0, "value": None}]}
        }
        result = lf.process_msk_event(event)
        assert result["statusCode"] == 200
        assert "errors 0" in result["body"]

    def test_multiple_messages_processed(self, mock_opensearch, mock_cache_table):
        topic = "sterling.public.yfs_order_invoice"
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
        event = {"table": "YFS_ORDER_INVOICE", "operation": "c", "payload": make_payload()}
        result = lf.lambda_handler(event, None)
        assert result["statusCode"] == 200

    def test_unknown_table_returns_400(self, mock_opensearch, mock_cache_table):
        event = {"table": "UNKNOWN_TABLE", "operation": "c", "payload": make_payload()}
        result = lf.lambda_handler(event, None)
        assert result["statusCode"] == 400

    def test_unhandled_exception_returns_500(self, mock_opensearch, mock_cache_table):
        with patch.object(lf, "process_msk_event", side_effect=RuntimeError("boom")):
            result = lf.lambda_handler({"eventSource": "aws:kafka", "records": {}}, None)
        assert result["statusCode"] == 500
        assert "boom" in result["body"]


# ===========================================================================
# OpenSearch Client
# ===========================================================================

class TestGetOpensearchClient:
    def setup_method(self):
        lf.opensearch_client = None

    @patch("lambda_function.OpenSearch")
    @patch("lambda_function.OPENSEARCH_AUTH", "basic")
    def test_basic_auth_creates_client(self, mock_os_cls):
        mock_os_cls.return_value = MagicMock()
        client = lf.get_opensearch_client()
        assert client is not None

    @patch("lambda_function.OpenSearch")
    @patch("lambda_function.OPENSEARCH_AUTH", "basic")
    def test_singleton_returns_cached_client(self, mock_os_cls):
        mock_os_cls.return_value = MagicMock()
        first = lf.get_opensearch_client()
        second = lf.get_opensearch_client()
        assert first is second
        assert mock_os_cls.call_count == 1

    @patch("lambda_function.OpenSearch")
    @patch("lambda_function.AWSV4SignerAuth")
    @patch("lambda_function.boto3.Session")
    @patch("lambda_function.OPENSEARCH_AUTH", "iam")
    @patch("lambda_function.OPENSEARCH_MODE", "serverless")
    def test_iam_serverless_uses_aoss(self, mock_session, mock_auth_cls, mock_os_cls):
        mock_session.return_value.get_credentials.return_value = MagicMock()
        mock_auth_cls.return_value = MagicMock()
        mock_os_cls.return_value = MagicMock()
        lf.get_opensearch_client()
        mock_auth_cls.assert_called_once_with(mock_session.return_value.get_credentials.return_value, lf.AWS_REGION, "aoss")

    @patch("lambda_function.boto3.Session")
    @patch("lambda_function.OPENSEARCH_AUTH", "iam")
    def test_iam_no_credentials_raises(self, mock_session):
        mock_session.return_value.get_credentials.return_value = None
        with pytest.raises(RuntimeError, match="AWS credentials not found"):
            lf.get_opensearch_client()
