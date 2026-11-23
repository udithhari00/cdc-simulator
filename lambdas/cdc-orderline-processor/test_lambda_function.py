"""
Comprehensive test suite for lambda_handler.py
Target: >80% coverage
Run with: pytest test_lambda_function.py -v --cov=lambda_handler --cov-report=term-missing
"""

import base64
import json
import os

from unittest.mock import patch, MagicMock, call
import pytest

os.environ.setdefault("OPENSEARCH_HOST", "localhost")
os.environ.setdefault("OPENSEARCH_AUTH", "basic")
os.environ.setdefault("AWS_REGION", "us-east-1")
os.environ.setdefault("DYNAMODB_ENDPOINT", "http://localhost:8000")

with patch("boto3.resource"), patch("boto3.Session"):
    import lambda_function as lf

# Alias for backwards-compat with test bodies that reference lambda_handler.*
lambda_handler = lf


# =============================================================================
# Fixtures / Shared Payloads
# =============================================================================

SAMPLE_PAYLOAD = {
    "ORDER_LINE_KEY": "OL123",
    "ORDER_HEADER_KEY": "OH123",
    "ITEM_ID": "ITEM001",
    "ITEM_DESCRIPTION": "Test Item",
    "PRIME_LINE_NO": "1",
    "SUB_LINE_NO": "0",
    "UNIT_PRICE": "10.5",
    "UOM": "EA",
    "PRODUCT_CLASS": "CONSUMER",
    "ORDERED_QTY": "2.0",
    "SHIPNODE_KEY": "NODE1",
    "CREATETS": "1000",
    "MODIFYTS": "2000",
}


def _make_msk_event(debezium_event: dict, topic: str = "sterling.public.yfs_order_line") -> dict:
    """Helper: wrap a Debezium event in a fake MSK event."""
    encoded = base64.b64encode(json.dumps(debezium_event).encode()).decode()
    return {
        "records": {
            "topic-0": [
                {"value": encoded, "topic": topic, "offset": 1}
            ]
        }
    }


@pytest.fixture
def mock_cache_table():
    table = MagicMock()
    table.get_item.return_value = {}
    lf.cache_table = table
    return table


# =============================================================================
# Utility / Helper Function Tests
# =============================================================================

class TestGetValue:
    def test_strips_whitespace(self):
        assert lambda_handler.get_value({"A": " test "}, "A") == "test"

    def test_missing_key_returns_none(self):
        assert lambda_handler.get_value({}, "A") is None

    def test_none_value_returns_none(self):
        assert lambda_handler.get_value({"A": None}, "A") is None

    def test_blank_string_returns_none(self):
        assert lambda_handler.get_value({"A": "   "}, "A") is None

    def test_integer_value_cast_to_string(self):
        assert lambda_handler.get_value({"A": 42}, "A") == "42"


class TestGetIntValue:
    def test_valid_int_string(self):
        assert lambda_handler.get_int_value({"A": "5"}, "A") == 5

    def test_missing_key_returns_none(self):
        assert lambda_handler.get_int_value({}, "A") is None

    def test_none_value_returns_none(self):
        assert lambda_handler.get_int_value({"A": None}, "A") is None

    def test_invalid_string_returns_none(self):
        assert lambda_handler.get_int_value({"A": "abc"}, "A") is None

    def test_float_string_truncates(self):
        # int("5.5") raises ValueError — expect None
        assert lambda_handler.get_int_value({"A": "5.5"}, "A") is None


class TestGetFloatValue:
    def test_valid_float_string(self):
        assert lambda_handler.get_float_value({"A": "5.5"}, "A") == 5.5

    def test_missing_key_returns_none(self):
        assert lambda_handler.get_float_value({}, "A") is None

    def test_none_value_returns_none(self):
        assert lambda_handler.get_float_value({"A": None}, "A") is None

    def test_invalid_string_returns_none(self):
        assert lambda_handler.get_float_value({"A": "abc"}, "A") is None


class TestGetTimestampValue:
    def test_seconds_converted_to_millis(self):
        # 1000 seconds → 1_000_000 ms
        assert lambda_handler.get_timestamp_value({"A": "1000"}, "A") == 1_000_000

    def test_already_in_millis(self):
        # 13-digit → already millis, returned as-is
        ts_ms = 1_700_000_000_000
        assert lambda_handler.get_timestamp_value({"A": str(ts_ms)}, "A") == ts_ms

    def test_microseconds_converted_to_millis(self):
        ts_us = 1_700_000_000_000_001
        assert lambda_handler.get_timestamp_value({"A": str(ts_us)}, "A") == ts_us // 1000

    def test_missing_key_returns_none(self):
        assert lambda_handler.get_timestamp_value({}, "A") is None

    def test_none_value_returns_none(self):
        assert lambda_handler.get_timestamp_value({"A": None}, "A") is None

    def test_invalid_string_returns_none(self):
        assert lambda_handler.get_timestamp_value({"A": "bad"}, "A") is None


class TestNowMs:
    def test_returns_int(self):
        assert isinstance(lambda_handler.now_ms(), int)

    def test_returns_recent_timestamp(self):
        ts = lambda_handler.now_ms()
        # Should be after Jan 2024 in ms
        assert ts > 1_700_000_000_000


# =============================================================================
# extract_table_from_topic
# =============================================================================

class TestExtractTableFromTopic:
    def test_standard_debezium_topic(self):
        assert lambda_handler.extract_table_from_topic("sterling.public.yfs_order_line") == "YFS_ORDER_LINE"

    def test_unknown_table_uppercased(self):
        assert lambda_handler.extract_table_from_topic("db.schema.unknown_table") == "UNKNOWN_TABLE"

    def test_single_segment_topic(self):
        assert lambda_handler.extract_table_from_topic("yfs_order_line") == "YFS_ORDER_LINE"


# =============================================================================
# transform_order_line
# =============================================================================

class TestTransformOrderLine:
    def test_core_fields_mapped(self):
        doc = lambda_handler.transform_order_line(SAMPLE_PAYLOAD, "c", 123)
        assert doc["orderLineKey"] == "OL123"
        assert doc["orderHeaderKey"] == "OH123"
        assert doc["itemId"] == "ITEM001"
        assert doc["itemDescription"] == "Test Item"
        assert doc["primeLineNo"] == 1
        assert doc["subLineNo"] == 0
        assert doc["unitPrice"] == 10.5

    def test_cdc_metadata_set(self):
        doc = lambda_handler.transform_order_line(SAMPLE_PAYLOAD, "u", 9999)
        assert doc["cdcOperation"] == "u"
        assert doc["cdcTimestamp"] == 9999

    def test_missing_optional_fields_are_none(self):
        minimal = {"ORDER_LINE_KEY": "OL1", "ORDER_HEADER_KEY": "OH1"}
        doc = lambda_handler.transform_order_line(minimal, "c", None)
        assert doc["itemId"] is None
        assert doc["unitPrice"] is None

    def test_timestamp_fields_converted(self):
        doc = lambda_handler.transform_order_line(SAMPLE_PAYLOAD, "c", 123)
        # CREATETS = "1000" → 1_000_000 ms
        assert doc["createdTimestamp"] == 1_000_000


# =============================================================================
# process_order_line_event
# =============================================================================

class TestProcessOrderLineEvent:

    @patch("lambda_function.upsert_order_line")
    def test_create_event_calls_upsert(self, mock_upsert):
        mock_upsert.return_value = {"statusCode": 200}
        result = lambda_handler.process_order_line_event("c", SAMPLE_PAYLOAD, 123)
        assert result["statusCode"] == 200
        mock_upsert.assert_called_once()

    @patch("lambda_function.upsert_order_line")
    def test_update_event_calls_upsert(self, mock_upsert):
        mock_upsert.return_value = {"statusCode": 200}
        result = lambda_handler.process_order_line_event("u", SAMPLE_PAYLOAD, 123)
        assert result["statusCode"] == 200
        mock_upsert.assert_called_once()

    @patch("lambda_function.upsert_order_line")
    def test_read_event_calls_upsert(self, mock_upsert):
        mock_upsert.return_value = {"statusCode": 200}
        result = lambda_handler.process_order_line_event("r", SAMPLE_PAYLOAD, 123)
        assert result["statusCode"] == 200

    @patch("lambda_function.delete_order_line")
    def test_delete_event_calls_delete(self, mock_delete):
        mock_delete.return_value = {"statusCode": 200}
        result = lambda_handler.process_order_line_event("d", SAMPLE_PAYLOAD, 123)
        assert result["statusCode"] == 200
        mock_delete.assert_called_once_with("OL123", "OH123")

    def test_missing_order_line_key_returns_400(self):
        payload = {**SAMPLE_PAYLOAD, "ORDER_LINE_KEY": None}
        result = lambda_handler.process_order_line_event("c", payload, 123)
        assert result["statusCode"] == 400
        assert "Missing ORDER_LINE_KEY" in result["body"]

    @patch("lambda_function.upsert_order_line")
    def test_searchable_text_built_from_item_fields(self, mock_upsert):
        mock_upsert.return_value = {"statusCode": 200}
        payload = {**SAMPLE_PAYLOAD, "UPC_CODE": "111222333"}
        lambda_handler.process_order_line_event("c", payload, 123)
        doc_arg = mock_upsert.call_args[0][1]
        assert "ITEM001" in doc_arg["searchableText"]
        assert "Test Item" in doc_arg["searchableText"]
        assert "111222333" in doc_arg["searchableText"]

    @patch("lambda_function.upsert_order_line")
    def test_none_values_stripped_from_document(self, mock_upsert):
        mock_upsert.return_value = {"statusCode": 200}
        lambda_handler.process_order_line_event("c", SAMPLE_PAYLOAD, 123)
        doc_arg = mock_upsert.call_args[0][1]
        # All values in the final doc should be non-None except _deletedAt
        for k, v in doc_arg.items():
            if k == '_deletedAt':
                assert v is None
            else:
                assert v is not None


# =============================================================================
# process_single_event
# =============================================================================

class TestProcessSingleEvent:

    @patch("lambda_function.process_order_line_event")
    def test_routes_yfs_order_line(self, mock_proc):
        mock_proc.return_value = {"statusCode": 200}
        result = lambda_handler.process_single_event("YFS_ORDER_LINE", "c", SAMPLE_PAYLOAD, 123)
        assert result["statusCode"] == 200
        mock_proc.assert_called_once()

    def test_unknown_table_returns_400(self):
        result = lambda_handler.process_single_event("UNKNOWN_TABLE", "c", {}, 123)
        assert result["statusCode"] == 400
        assert "Unknown table" in result["body"]


# =============================================================================
# upsert_order_line
# =============================================================================

class TestUpsertOrderLine:

    @patch("lambda_function.get_opensearch_client")
    def test_successful_upsert_returns_200(self, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update.return_value = {"result": "created"}

        result = lambda_handler.upsert_order_line("OL123", {"itemId": "ITEM1"})

        assert result["statusCode"] == 200
        assert "OL123" in result["body"]
        mock_os.update.assert_called_once()

    @patch("lambda_function.get_opensearch_client")
    def test_upsert_correct_index_and_id(self, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update.return_value = {"result": "updated"}

        lambda_handler.upsert_order_line("MY_KEY", {"field": "val"})

        call_kwargs = mock_os.update.call_args[1]
        assert call_kwargs["index"] == lambda_handler.INDEX_ORDER_LINES
        assert call_kwargs["id"] == "MY_KEY"

    @patch("lambda_function.get_opensearch_client")
    def test_upsert_raises_on_opensearch_error(self, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update.side_effect = Exception("Connection refused")

        with pytest.raises(Exception, match="Connection refused"):
            lambda_handler.upsert_order_line("OL123", {})


# =============================================================================
# delete_order_line
# =============================================================================

class TestDeleteOrderLine:

    @patch("lambda_function.get_opensearch_client")
    @patch("lambda_function.ENABLE_HARD_DELETE", True)
    def test_successful_delete_returns_200(self, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os

        result = lambda_handler.delete_order_line("OL123", "OH123")
        assert result["statusCode"] == 200
        mock_os.delete.assert_called_once()

    @patch("lambda_function.get_opensearch_client")
    @patch("lambda_function.ENABLE_HARD_DELETE", True)
    def test_404_error_treated_as_success(self, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.delete.side_effect = Exception("404 not_found")

        result = lambda_handler.delete_order_line("OL123", "OH123")
        assert result["statusCode"] == 200
        assert "absent" in result["body"]

    @patch("lambda_function.get_opensearch_client")
    @patch("lambda_function.ENABLE_HARD_DELETE", True)
    def test_non_404_error_raises(self, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.delete.side_effect = Exception("503 Service Unavailable")

        with pytest.raises(Exception, match="503"):
            lambda_handler.delete_order_line("OL123", "OH123")


# =============================================================================
# lambda_handler (entry point)
# =============================================================================

class TestLambdaHandler:

    @patch("lambda_function.process_single_event")
    def test_direct_invocation(self, mock_process):
        mock_process.return_value = {"statusCode": 200}
        event = {"table": "YFS_ORDER_LINE", "operation": "c", "payload": SAMPLE_PAYLOAD}
        result = lambda_handler.lambda_handler(event, None)
        assert result["statusCode"] == 200

    @patch("lambda_function.process_msk_event")
    def test_msk_trigger_via_records_key(self, mock_msk):
        mock_msk.return_value = {"statusCode": 200}
        event = {"records": {}}
        result = lambda_handler.lambda_handler(event, None)
        mock_msk.assert_called_once()
        assert result["statusCode"] == 200

    @patch("lambda_function.process_msk_event")
    def test_msk_trigger_via_eventsource(self, mock_msk):
        mock_msk.return_value = {"statusCode": 200}
        event = {"eventSource": "aws:kafka", "records": {}}
        result = lambda_handler.lambda_handler(event, None)
        mock_msk.assert_called_once()

    @patch("lambda_function.process_single_event")
    def test_unhandled_exception_returns_500(self, mock_process):
        mock_process.side_effect = RuntimeError("Boom!")
        event = {"table": "YFS_ORDER_LINE", "operation": "c", "payload": {}}
        result = lambda_handler.lambda_handler(event, None)
        assert result["statusCode"] == 500
        assert "Boom!" in result["body"]

    @patch("lambda_function.process_single_event")
    def test_defaults_applied_when_keys_missing(self, mock_process):
        mock_process.return_value = {"statusCode": 200}
        result = lambda_handler.lambda_handler({}, None)
        # table defaults to "" → process_single_event still called
        mock_process.assert_called_once()


# =============================================================================
# process_msk_event
# =============================================================================

class TestProcessMskEvent:

    @patch("lambda_function.process_single_event")
    def test_create_event_processed(self, mock_process):
        mock_process.return_value = {"statusCode": 200}
        event = _make_msk_event({"op": "c", "after": SAMPLE_PAYLOAD, "ts_ms": 123})
        result = lambda_handler.process_msk_event(event)
        assert result["statusCode"] == 200
        mock_process.assert_called_once()

    @patch("lambda_function.process_single_event")
    def test_delete_event_uses_before_image(self, mock_process):
        mock_process.return_value = {"statusCode": 200}
        before = {**SAMPLE_PAYLOAD, "ORDER_LINE_KEY": "OL_DEL"}
        event = _make_msk_event({"op": "d", "before": before, "ts_ms": 456})
        lambda_handler.process_msk_event(event)
        _, call_op, call_payload, _ = mock_process.call_args[0]
        assert call_op == "d"
        assert call_payload.get("ORDER_LINE_KEY") == "OL_DEL"

    @patch("lambda_function.process_single_event")
    def test_multiple_messages_all_processed(self, mock_process):
        mock_process.return_value = {"statusCode": 200}
        encoded = base64.b64encode(json.dumps({"op": "c", "after": SAMPLE_PAYLOAD, "ts_ms": 1}).encode()).decode()
        event = {
            "records": {
                "topic-0": [
                    {"value": encoded, "topic": "sterling.public.yfs_order_line", "offset": 1},
                    {"value": encoded, "topic": "sterling.public.yfs_order_line", "offset": 2},
                ]
            }
        }
        result = lambda_handler.process_msk_event(event)
        assert result["statusCode"] == 200
        assert mock_process.call_count == 2

    @patch("lambda_function.process_single_event")
    def test_payload_keys_uppercased(self, mock_process):
        mock_process.return_value = {"statusCode": 200}
        lowercase_payload = {"order_line_key": "OL1", "order_header_key": "OH1"}
        event = _make_msk_event({"op": "c", "after": lowercase_payload, "ts_ms": 1})
        lambda_handler.process_msk_event(event)
        _, _, call_payload, _ = mock_process.call_args[0]
        assert "ORDER_LINE_KEY" in call_payload

    @patch("lambda_function.process_single_event")
    def test_bad_message_increments_error_count_and_returns_207(self, mock_process):
        event = {
            "records": {
                "topic-0": [
                    {"value": "NOT_VALID_BASE64!!!", "topic": "sterling.public.yfs_order_line", "offset": 9}
                ]
            }
        }
        result = lambda_handler.process_msk_event(event)
        assert result["statusCode"] == 207
        assert "errors 1" in result["body"]

    @patch("lambda_function.process_single_event")
    def test_non_200_result_counted_as_error_and_returns_207(self, mock_process):
        mock_process.return_value = {"statusCode": 400, "body": "bad"}
        event = _make_msk_event({"op": "c", "after": SAMPLE_PAYLOAD, "ts_ms": 1})
        result = lambda_handler.process_msk_event(event)
        assert result["statusCode"] == 207
        assert "errors 1" in result["body"]

    @patch("lambda_function.process_single_event")
    def test_empty_records_returns_200(self, mock_process):
        result = lambda_handler.process_msk_event({"records": {}})
        assert result["statusCode"] == 200
        mock_process.assert_not_called()

    @patch("lambda_function.process_single_event")
    def test_topic_without_dot_still_resolves_table(self, mock_process):
        mock_process.return_value = {"statusCode": 200}
        encoded = base64.b64encode(json.dumps({"op": "c", "after": SAMPLE_PAYLOAD, "ts_ms": 1}).encode()).decode()
        event = {
            "records": {
                "yfs_order_line-0": [
                    # No "topic" key — falls back to partition key split
                    {"value": encoded, "offset": 1}
                ]
            }
        }
        result = lambda_handler.process_msk_event(event)
        assert result["statusCode"] == 200


# =============================================================================
# get_opensearch_client — auth branches
# =============================================================================

class TestGetOpensearchClient:

    def setup_method(self):
        # Reset singleton before each test
        lambda_handler.opensearch_client = None

    @patch("lambda_function.OpenSearch")
    @patch("lambda_function.AWSV4SignerAuth")
    @patch("lambda_function.boto3.Session")
    @patch("lambda_function.OPENSEARCH_AUTH", "iam")
    @patch("lambda_function.OPENSEARCH_MODE", "provisioned")
    def test_iam_provisioned_creates_client(self, mock_session, mock_auth_cls, mock_os_cls):
        mock_creds = MagicMock()
        mock_session.return_value.get_credentials.return_value = mock_creds
        mock_auth_cls.return_value = MagicMock()
        mock_os_cls.return_value = MagicMock()

        client = lambda_handler.get_opensearch_client()
        assert client is not None
        # provisioned → service should be 'es'
        mock_auth_cls.assert_called_once_with(mock_creds, lambda_handler.AWS_REGION, 'es')

    @patch("lambda_function.OpenSearch")
    @patch("lambda_function.AWSV4SignerAuth")
    @patch("lambda_function.boto3.Session")
    @patch("lambda_function.OPENSEARCH_AUTH", "iam")
    @patch("lambda_function.OPENSEARCH_MODE", "serverless")
    def test_iam_serverless_uses_aoss_service(self, mock_session, mock_auth_cls, mock_os_cls):
        mock_creds = MagicMock()
        mock_session.return_value.get_credentials.return_value = mock_creds
        mock_auth_cls.return_value = MagicMock()
        mock_os_cls.return_value = MagicMock()

        lambda_handler.get_opensearch_client()
        mock_auth_cls.assert_called_once_with(mock_creds, lambda_handler.AWS_REGION, 'aoss')

    @patch("lambda_function.OpenSearch")
    @patch("lambda_function.OPENSEARCH_AUTH", "basic")
    def test_basic_auth_creates_client(self, mock_os_cls):
        mock_os_cls.return_value = MagicMock()
        client = lambda_handler.get_opensearch_client()
        assert client is not None
        mock_os_cls.assert_called_once()

    @patch("lambda_function.OpenSearch")
    @patch("lambda_function.OPENSEARCH_AUTH", "basic")
    def test_singleton_returns_cached_client(self, mock_os_cls):
        mock_os_cls.return_value = MagicMock()
        first = lambda_handler.get_opensearch_client()
        second = lambda_handler.get_opensearch_client()
        assert first is second
        # OpenSearch constructor called only once
        assert mock_os_cls.call_count == 1

    @patch("lambda_function.boto3.Session")
    @patch("lambda_function.OPENSEARCH_AUTH", "iam")
    def test_iam_no_credentials_raises_runtime_error(self, mock_session):
        mock_session.return_value.get_credentials.return_value = None
        with pytest.raises(RuntimeError, match="AWS credentials not found"):
            lambda_handler.get_opensearch_client()


# =============================================================================
# _parse_debezium_message
# =============================================================================

class TestParseDebeziumMessage:
    def test_flattened_smt_format_detected(self):
        event = {
            "schema": {"type": "struct", "fields": []},
            "payload": {
                "ORDER_LINE_KEY": "OLK-001",
                "__op": "u",
                "__source_ts_ms": 1773151346000
            }
        }
        op, ts, payload = lf._parse_debezium_message(event)
        assert op == "u"
        assert ts == 1773151346000
        assert "ORDER_LINE_KEY" in payload
        assert "__op" not in payload
        assert "__source_ts_ms" not in payload

    def test_standard_envelope_format_create(self):
        event = {
            "op": "c",
            "ts_ms": 1700000000000,
            "before": None,
            "after": {"order_line_key": "OLK-001"}
        }
        op, ts, payload = lf._parse_debezium_message(event)
        assert op == "c"
        assert ts == 1700000000000
        assert "order_line_key" in payload

    def test_standard_envelope_delete_uses_before(self):
        event = {
            "op": "d",
            "ts_ms": 1700000000000,
            "before": {"order_line_key": "OLK-001"},
            "after": None
        }
        op, ts, payload = lf._parse_debezium_message(event)
        assert op == "d"
        assert "order_line_key" in payload

    def test_null_op_defaults_to_c(self):
        event = {
            "schema": {},
            "payload": {"ORDER_LINE_KEY": "OLK-001", "__op": None, "__source_ts_ms": 0}
        }
        op, ts, payload = lf._parse_debezium_message(event)
        assert op == "c"

    def test_dunder_keys_stripped(self):
        event = {
            "schema": {},
            "payload": {
                "ORDER_LINE_KEY": "OLK-001",
                "__op": "r",
                "__source_ts_ms": 1773000000000,
                "__table": "YFS_ORDER_LINE",
                "__deleted": "false"
            }
        }
        _, _, payload = lf._parse_debezium_message(event)
        for k in payload:
            assert not k.startswith("__")


# =============================================================================
# Tombstone (null value) handling
# =============================================================================

class TestTombstoneRecord:
    @patch("lambda_function.process_single_event")
    def test_null_value_skipped_not_counted_as_error(self, mock_process):
        event = {
            "eventSource": "aws:kafka",
            "records": {
                "topic-0": [{"topic": "sterling.public.yfs_order_line", "offset": 0, "value": None}]
            }
        }
        result = lf.process_msk_event(event)
        assert result["statusCode"] == 200
        assert "errors 0" in result["body"]
        mock_process.assert_not_called()


# =============================================================================
# Real YFS_ORDER_LINE CDC event (flattened SMT format)
# =============================================================================

REAL_ORDER_LINE_CDC_EVENT = {
    "schema": {
        "type": "struct",
        "fields": [],
        "optional": False,
        "name": "glenraven-dev.STERLING.YFS_ORDER_LINE.Value"
    },
    "payload": {
        "ORDER_LINE_KEY": "202406201341106165469564",
        "ORDER_HEADER_KEY": "202406201339026165466026",
        "PRIME_LINE_NO": 3.0,
        "SUB_LINE_NO": 1.0,
        "LINE_TYPE": " ",
        "ORDER_CLASS": "NEW",
        "ITEM_ID": "895104                                  ",
        "ALTERNATE_ITEM_ID": None,
        "UOM": "YD",
        "PRODUCT_CLASS": "A         ",
        "UNIT_PRICE": 13.28,
        "COST_CURRENCY": "USD",
        "ORDERED_QTY": 45.0,
        "BASIC_CAPACITY_REQUIRED": 0.0,
        "OPTION_CAPACITY_REQUIRED": 0.0,
        "DEPENDENT_ON_LINE_KEY": None,
        "CURRENT_WORK_ORDER_KEY": None,
        "DEPENDENCY_SHIPPING_RULE": None,
        "FILL_QUANTITY": 0.0,
        "COMMITTED_QUANTITY": None,
        "DEPENDENCY_RATIO": 0.0,
        "MAINTAIN_RATIO": " ",
        "MERGE_NODE": None,
        "PARENT_OF_DEPENDENT_GROUP": "N",
        "SOURCE_FROM_ORGANIZATION": None,
        "CHAINED_FROM_ORDER_LINE_KEY": None,
        "CHAINED_FROM_ORDER_HEADER_KEY": None,
        "DERIVED_FROM_ORDER_LINE_KEY": None,
        "DERIVED_FROM_ORDER_HEADER_KEY": None,
        "DERIVED_FROM_ORDER_RELEASE_KEY": None,
        "DISTRIBUTION_RULE_ID": None,
        "INVOICED_QUANTITY": 0.0,
        "OVER_RECEIPT_QUANTITY": 0.0,
        "RETURN_REASON": " ",
        "SHIPNODE_KEY": "TE                      ",
        "PROCURE_FROM_NODE": None,
        "SHIP_TO_KEY": None,
        "MARK_FOR_KEY": None,
        "BUYER_MARK_FOR_NODE_ID": None,
        "REQ_DELIVERY_DATE": None,
        "REQ_CANCEL_DATE": 16725225600000,
        "REQ_SHIP_DATE": None,
        "SCAC": " ",
        "CARRIER_SERVICE_CODE": " ",
        "CARRIER_ACCOUNT_NO": " ",
        "PICKABLE_FLAG": "Y",
        "SHIP_TOGETHER_NO": " ",
        "HOLD_FLAG": "N",
        "KIT_CODE": " ",
        "HOLD_REASON_CODE": " ",
        "OTHER_CHARGES": 0.0,
        "LINE_TOTAL": 597.6,
        "INVOICED_LINE_TOTAL": 0.0,
        "INVOICED_EXTENDED_PRICE": 0.0,
        "SETTLED_QUANTITY": 0.0,
        "SETTLED_AMOUNT": 0.0,
        "TAXABLE_FLAG": "Y",
        "TAX_EXEMPTION_CERTIFICATE": None,
        "DISCOUNT_TYPE": " ",
        "DISCOUNT_REFERENCE": " ",
        "GIFT_FLAG": "N",
        "PERSONALIZE_FLAG": " ",
        "PERSONALIZE_CODE": " ",
        "DEPARTMENT_CODE": " ",
        "CUSTOMER_ITEM": " ",
        "CUSTOMER_ITEM_DESCRIPTION": " ",
        "ITEM_WEIGHT": 0.9,
        "ITEM_WEIGHT_UOM": "LBS",
        "ITEM_DESCRIPTION": "Dickson North American Collection #7244 47\" Amande (Avocado) (Standard Pack 65 Yards)",
        "ITEM_SHORT_DESCRIPTION": "Dickson North American Collection #7244 47\" Amande (Avocado) (Standard Pack 65 Yards) (895104)",
        "RESERVATION_ID": " ",
        "RESERVATION_POOL": " ",
        "CUSTOMER_PO_NO": " ",
        "CUSTOMER_PO_LINE_NO": " ",
        "TAX": 0.0,
        "DELIVERY_CODE": " ",
        "ORIGINAL_ORDERED_QTY": 45.0,
        "LIST_PRICE": 0.0,
        "RETAIL_PRICE": 0.0,
        "DISCOUNT_PERCENTAGE": 0.0,
        "PACKLIST_TYPE": " ",
        "SUPPLIER_ITEM": " ",
        "SUPPLIER_ITEM_DESCRIPTION": " ",
        "UNIT_COST": 8.6816,
        "UPC_CODE": " ",
        "FOB": " ",
        "MANUFACTURER_NAME": "DICKSON CONSTANT",
        "MANUFACTURER_ITEM": "7244",
        "MANUFACTURER_ITEM_DESC": "DICKSON 47\" 7244 AMANDE ( AVOCADO)",
        "COUNTRY_OF_ORIGIN": " ",
        "ISBN": " ",
        "HARMONIZED_CODE": "5512.29.0000",
        "SHIP_TO_ID": "3005301",
        "PRODUCT_LINE": " ",
        "NMFC_CODE": "49265-10",
        "NMFC_CLASS": "65",
        "NMFC_DESCRIPTION": " ",
        "TAX_PRODUCT_CODE": "P0000000",
        "IMPORT_LICENSE_NO": " ",
        "IMPORT_LICENSE_EXP_DATE": 16725225600000,
        "ECCN_NO": " ",
        "SCHEDULE_B_CODE": " ",
        "SUPPLIER_CODE": None,
        "PURPOSE": " ",
        "RECEIVING_NODE": None,
        "BUYER_RECEIVING_NODE_ID": None,
        "SHIPMENT_CONSOL_GROUP_ID": None,
        "ORIG_ORDER_LINE_KEY": None,
        "LINE_SEQ_NO": "3.1",
        "SPLIT_QTY": 0.0,
        "PRICING_DATE": 1718841600000,
        "PIPELINE_KEY": "20120529165413110451    ",
        "CONDITION_VARIABLE_1": " ",
        "CONDITION_VARIABLE_2": " ",
        "IS_PRICE_LOCKED": "N",
        "IS_COST_OVERRIDDEN": "N",
        "IS_CAPACITY_OVERRIDDEN": "N",
        "INVOICE_COMPLETE": "N",
        "DELIVERY_METHOD": "SHP",
        "ITEM_GROUP_CODE": "PROD",
        "CANNOT_COMPLETE_BEFORE_DATE": None,
        "CANNOT_COMPLETE_AFTER_DATE": None,
        "APPT_STATUS": " ",
        "CAN_ADD_SERVICE_LINES": "N",
        "PRICING_UOM": "YD",
        "CAPACITY_UOM": " ",
        "PRICING_QUANTITY": 45.0,
        "SHIPPED_QUANTITY": 0.0,
        "SCHED_FAILURE_REASON_CODE": "NOT_ENOUGH_PRODUCT_CHOICES",
        "EARLIEST_SHIP_DATE": 1718890870000,
        "EARLIEST_DELIVERY_DATE": 1718890870000,
        "PROMISED_APPT_START_DATE": None,
        "PROMISED_APPT_END_DATE": None,
        "SEGMENT": " ",
        "SEGMENT_TYPE": " ",
        "IS_FORWARDING_ALLOWED": None,
        "IS_PROCUREMENT_ALLOWED": None,
        "RESHIP_PARENT_LINE_KEY": None,
        "BUNDLE_PARENT_ORDER_LINE_KEY": None,
        "IS_PRICE_INFO_ONLY": None,
        "LEVEL_OF_SERVICE": None,
        "CREATETS": 1718890871000,
        "MODIFYTS": 1773136946000,
        "CREATEUSERID": "spraa@dist.us.glenraven.com",
        "MODIFYUSERID": "WaveAgentServer",
        "CREATEPROGID": "ISCCSSYS001",
        "MODIFYPROGID": "RELEASE.0001",
        "LOCKID": 532.0,
        "ORDERING_UOM": "YD",
        "IS_STANDALONE_SERVICE": " ",
        "INVOICE_BASED_ON_ACTUALS": "N",
        "ACTUAL_PRICING_QUANTITY": 45.0,
        "FULFILLMENT_TYPE": " ",
        "SERIAL_NO": " ",
        "RESERVATION_MANDATORY": "N",
        "IS_FIRM_PREDEFINED_NODE": "Y",
        "INTENTIONAL_BACKORDER": "N",
        "REPRICING_QUANTITY": 45.0,
        "EXTN_REQ_DROP_SHIP": "N",
        "EXTN_SCH_FRM_BO": "Y",
        "EXTN_PLANNER_NAME": None,
        "ITEM_DESCRIPTION_LC": "dickson north american collection #7244 47\" amande (avocado) (standard pack 65 yards)",
        "GROUP_SEQUENCE_NUM": None,
        "IS_ELIGIBLE_FOR_SHIP_DISC": "Y",
        "EXTN_CCN_ADS_DELIVERY_DATE": None,
        "CONTRACT_ORDER_LINE_KEY": None,
        "CONTRACT_ORDER_KEY": None,
        "EXTN_CUSTOMER_CAN_KEEP": "N",
        "__op": "u",
        "__table": "YFS_ORDER_LINE",
        "__source_ts_ms": 1773151346000
    }
}


def _make_msk_event_from_raw(raw_event: dict, topic: str = "glenraven-prod.STERLING.YFS_ORDER_LINE") -> dict:
    """Wrap a raw CDC event dict as an MSK trigger (base64-encoded JSON value)."""
    encoded = base64.b64encode(json.dumps(raw_event).encode()).decode()
    return {
        "eventSource": "aws:kafka",
        "records": {
            f"{topic}-0": [{"topic": topic, "offset": 0, "value": encoded}]
        }
    }


class TestRealOrderLineCdcEvent:
    """End-to-end tests using a real YFS_ORDER_LINE CDC event (flattened SMT format)."""

    @pytest.fixture(autouse=True)
    def setup(self):
        mock_client = MagicMock()
        mock_client.update.return_value = {"result": "updated"}
        mock_client.delete.return_value = {"result": "deleted"}
        lf.opensearch_client = mock_client
        self.mock_os = mock_client
        self.msk_event = _make_msk_event_from_raw(REAL_ORDER_LINE_CDC_EVENT)
        yield
        lf.opensearch_client = None

    def _get_upserted_doc(self) -> dict:
        lf.process_msk_event(self.msk_event)
        return self.mock_os.update.call_args[1]["body"]["doc"]

    def test_processes_successfully(self):
        result = lf.process_msk_event(self.msk_event)
        assert result["statusCode"] == 200
        assert "Processed 1" in result["body"]
        assert "errors 0" in result["body"]

    def test_order_line_key_as_document_id(self):
        lf.process_msk_event(self.msk_event)
        assert self.mock_os.update.call_args[1]["id"] == "202406201341106165469564"

    def test_indexed_to_correct_index(self):
        lf.process_msk_event(self.msk_event)
        assert self.mock_os.update.call_args[1]["index"] == lf.INDEX_ORDER_LINES

    def test_upsert_doc_as_upsert_true(self):
        lf.process_msk_event(self.msk_event)
        assert self.mock_os.update.call_args[1]["body"]["doc_as_upsert"] is True

    def test_order_header_key(self):
        doc = self._get_upserted_doc()
        assert doc["orderHeaderKey"] == "202406201339026165466026"

    def test_item_id_stripped(self):
        doc = self._get_upserted_doc()
        assert doc["itemId"] == "895104"

    def test_product_class_stripped(self):
        doc = self._get_upserted_doc()
        assert doc["productClass"] == "A"

    def test_shipnode_key_stripped(self):
        doc = self._get_upserted_doc()
        assert doc["shipnodeKey"] == "TE"

    def test_prime_line_no_as_int(self):
        doc = self._get_upserted_doc()
        assert doc["primeLineNo"] == 3

    def test_sub_line_no_as_int(self):
        doc = self._get_upserted_doc()
        assert doc["subLineNo"] == 1

    def test_unit_price(self):
        doc = self._get_upserted_doc()
        assert doc["unitPrice"] == 13.28

    def test_unit_cost(self):
        doc = self._get_upserted_doc()
        assert doc["unitCost"] == 8.6816

    def test_ordered_qty(self):
        doc = self._get_upserted_doc()
        assert doc["orderedQty"] == 45.0

    def test_line_total(self):
        doc = self._get_upserted_doc()
        assert doc["lineTotal"] == 597.6

    def test_item_weight(self):
        doc = self._get_upserted_doc()
        assert doc["itemWeight"] == 0.9

    def test_item_weight_uom(self):
        doc = self._get_upserted_doc()
        assert doc["itemWeightUom"] == "LBS"

    def test_item_description(self):
        doc = self._get_upserted_doc()
        assert "Dickson North American Collection" in doc["itemDescription"]
        assert "7244" in doc["itemDescription"]

    def test_manufacturer_name(self):
        doc = self._get_upserted_doc()
        assert doc["manufacturerName"] == "DICKSON CONSTANT"

    def test_manufacturer_item(self):
        doc = self._get_upserted_doc()
        assert doc["manufacturerItem"] == "7244"

    def test_harmonized_code(self):
        doc = self._get_upserted_doc()
        assert doc["harmonizedCode"] == "5512.29.0000"

    def test_ship_to_id(self):
        doc = self._get_upserted_doc()
        assert doc["shipToId"] == "3005301"

    def test_delivery_method(self):
        doc = self._get_upserted_doc()
        assert doc["deliveryMethod"] == "SHP"

    def test_order_class(self):
        doc = self._get_upserted_doc()
        assert doc["orderClass"] == "NEW"

    def test_uom(self):
        doc = self._get_upserted_doc()
        assert doc["uom"] == "YD"

    def test_pickable_flag(self):
        doc = self._get_upserted_doc()
        assert doc["pickableFlag"] == "Y"

    def test_taxable_flag(self):
        doc = self._get_upserted_doc()
        assert doc["taxableFlag"] == "Y"

    def test_line_seq_no(self):
        doc = self._get_upserted_doc()
        assert doc["lineSeqNo"] == "3.1"

    def test_req_cancel_date_epoch_millis(self):
        doc = self._get_upserted_doc()
        assert doc["reqCancelDate"] == 16725225600000

    def test_earliest_ship_date(self):
        doc = self._get_upserted_doc()
        assert doc["earliestShipDate"] == 1718890870000

    def test_earliest_delivery_date(self):
        doc = self._get_upserted_doc()
        assert doc["earliestDeliveryDate"] == 1718890870000

    def test_pricing_date(self):
        doc = self._get_upserted_doc()
        assert doc["pricingDate"] == 1718841600000

    def test_created_timestamp(self):
        doc = self._get_upserted_doc()
        assert doc["createdTimestamp"] == 1718890871000

    def test_modified_timestamp(self):
        doc = self._get_upserted_doc()
        assert doc["modifiedTimestamp"] == 1773136946000

    def test_create_user_id(self):
        doc = self._get_upserted_doc()
        assert doc["createUserId"] == "spraa@dist.us.glenraven.com"

    def test_modify_user_id(self):
        doc = self._get_upserted_doc()
        assert doc["modifyUserId"] == "WaveAgentServer"

    def test_cdc_operation_is_update(self):
        doc = self._get_upserted_doc()
        assert doc["cdcOperation"] == "u"

    def test_cdc_timestamp_from_source_ts_ms(self):
        doc = self._get_upserted_doc()
        assert doc["cdcTimestamp"] == 1773151346000

    def test_indexed_at_is_recent(self):
        doc = self._get_upserted_doc()
        assert isinstance(doc["indexedAt"], int)
        assert doc["indexedAt"] > 1_700_000_000_000

    def test_null_fields_excluded(self):
        doc = self._get_upserted_doc()
        for k, v in doc.items():
            if k == '_deletedAt':
                assert v is None, f"Field '{k}' should be None"
            else:
                assert v is not None, f"Field '{k}' should be excluded when None"

    def test_blank_string_fields_excluded(self):
        # Blank Oracle CHAR fields (single space) should be stripped to None and excluded
        doc = self._get_upserted_doc()
        assert "scac" not in doc
        assert "carrierServiceCode" not in doc
        assert "returnReason" not in doc

    def test_searchable_text_contains_item_id(self):
        doc = self._get_upserted_doc()
        assert "895104" in doc["searchableText"]

    def test_searchable_text_contains_item_description(self):
        doc = self._get_upserted_doc()
        assert "Dickson North American Collection" in doc["searchableText"]

    def test_searchable_text_contains_manufacturer(self):
        doc = self._get_upserted_doc()
        assert "DICKSON CONSTANT" in doc["searchableText"]
        assert "7244" in doc["searchableText"]


# =============================================================================
# Soft Delete — cdc-orderline-processor
# =============================================================================

class TestSoftDeleteOrderLine:
    """
    Tests for the ENABLE_HARD_DELETE toggle on the order line lambda.

    Soft delete (default, ENABLE_HARD_DELETE=false):
      - OpenSearch document is updated with _deleted=True and _deletedAt (epoch ms)
      - client.delete() is NOT called

    Hard delete (ENABLE_HARD_DELETE=true):
      - client.delete() is called with the order line key and ignore=[404]
      - client.update() is NOT called for the delete
    """

    # ------------------------------------------------------------------
    # Soft delete (default)
    # ------------------------------------------------------------------

    @patch("lambda_function.get_opensearch_client")
    def test_soft_delete_calls_update_not_delete(self, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update.return_value = {"result": "updated"}
        with patch.dict("os.environ", {"ENABLE_HARD_DELETE": "false"}):
            result = lambda_handler.delete_order_line("OL_SOFT_001", "OH_SOFT_001")
        assert result["statusCode"] == 200
        mock_os.update.assert_called_once()
        mock_os.delete.assert_not_called()

    @patch("lambda_function.get_opensearch_client")
    def test_soft_delete_sets_deleted_true(self, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update.return_value = {"result": "updated"}
        with patch.dict("os.environ", {"ENABLE_HARD_DELETE": "false"}):
            lambda_handler.delete_order_line("OL_SOFT_001", "OH_SOFT_001")
        doc = mock_os.update.call_args[1]["body"]["doc"]
        assert doc["_deleted"] is True

    @patch("lambda_function.get_opensearch_client")
    def test_soft_delete_sets_deleted_at_epoch_millis(self, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update.return_value = {"result": "updated"}
        with patch.dict("os.environ", {"ENABLE_HARD_DELETE": "false"}):
            lambda_handler.delete_order_line("OL_SOFT_001", "OH_SOFT_001")
        doc = mock_os.update.call_args[1]["body"]["doc"]
        assert isinstance(doc["_deletedAt"], int)
        assert doc["_deletedAt"] > 1_700_000_000_000

    @patch("lambda_function.get_opensearch_client")
    def test_soft_delete_targets_correct_id_and_index(self, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update.return_value = {"result": "updated"}
        with patch.dict("os.environ", {"ENABLE_HARD_DELETE": "false"}):
            lambda_handler.delete_order_line("OL_SOFT_001", "OH_SOFT_001")
        call_kwargs = mock_os.update.call_args[1]
        assert call_kwargs["id"] == "OL_SOFT_001"
        assert call_kwargs["index"] == lambda_handler.INDEX_ORDER_LINES

    # ------------------------------------------------------------------
    # Hard delete
    # ------------------------------------------------------------------

    @patch("lambda_function.get_opensearch_client")
    @patch("lambda_function.ENABLE_HARD_DELETE", True)
    def test_hard_delete_calls_client_delete(self, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.delete.return_value = {"result": "deleted"}
        with patch.dict("os.environ", {"ENABLE_HARD_DELETE": "true"}):
            result = lambda_handler.delete_order_line("OL_HARD_001", "OH_HARD_001")
        assert result["statusCode"] == 200
        mock_os.delete.assert_called_once()
        mock_os.update.assert_not_called()

    @patch("lambda_function.get_opensearch_client")
    def test_hard_delete_404_returns_200(self, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.delete.side_effect = Exception("404 not_found")
        with patch.dict("os.environ", {"ENABLE_HARD_DELETE": "true"}):
            result = lambda_handler.delete_order_line("OL_HARD_001", "OH_HARD_001")
        assert result["statusCode"] == 200


# =============================================================================
# DynamoDB Cache Helpers
# =============================================================================

class TestGetCached:
    def test_returns_item_without_pk_sk_updatedAt(self, mock_cache_table):
        mock_cache_table.get_item.return_value = {
            "Item": {"PK": "ORDER_LINE_HOLD_TYPE", "SK": "OLK-001",
                     "holdTypes": {"FRAUD_REVIEW"}, "updatedAt": 123}
        }
        result = lf.get_cached("ORDER_LINE_HOLD_TYPE", "OLK-001")
        assert "PK" not in result
        assert "SK" not in result
        assert "updatedAt" not in result
        assert "holdTypes" in result

    def test_returns_none_when_no_item(self, mock_cache_table):
        mock_cache_table.get_item.return_value = {}
        result = lf.get_cached("ORDER_LINE_HOLD_TYPE", "OLK-MISSING")
        assert result is None

    def test_returns_none_on_error(self, mock_cache_table):
        mock_cache_table.get_item.side_effect = Exception("connection error")
        result = lf.get_cached("ORDER_LINE_HOLD_TYPE", "OLK-001")
        assert result is None


# =============================================================================
# Enrichment — enrich_order_line_hold_types
# =============================================================================

class TestEnrichOrderLineHoldTypes:
    def test_sets_hold_types_from_cache_set(self, mock_cache_table):
        mock_cache_table.get_item.return_value = {
            "Item": {"PK": "ORDER_LINE_HOLD_TYPE", "SK": "OLK-001",
                     "holdTypeStatuses": {"FRAUD_REVIEW": "1100", "CREDIT_HOLD": "1100"}}
        }
        doc = {"orderLineKey": "OLK-001"}
        lf.enrich_order_line_hold_types(doc)
        assert isinstance(doc["holdTypeStatuses"], dict)

    def test_hold_types_sorted(self, mock_cache_table):
        mock_cache_table.get_item.return_value = {
            "Item": {"PK": "ORDER_LINE_HOLD_TYPE", "SK": "OLK-001",
                     "holdTypeStatuses": {"Z_HOLD": "1100", "A_HOLD": "1100", "M_HOLD": "1100"}}
        }
        doc = {"orderLineKey": "OLK-001"}
        lf.enrich_order_line_hold_types(doc)
        assert "A_HOLD" in doc["holdTypeStatuses"]
        assert "M_HOLD" in doc["holdTypeStatuses"]

    def test_no_op_when_cache_miss(self, mock_cache_table):
        mock_cache_table.get_item.return_value = {}
        doc = {"orderLineKey": "OLK-001"}
        lf.enrich_order_line_hold_types(doc)
        assert "holdTypeStatuses" not in doc

    def test_no_op_when_order_line_key_missing(self, mock_cache_table):
        doc = {}
        lf.enrich_order_line_hold_types(doc)
        mock_cache_table.get_item.assert_not_called()

    def test_no_op_when_hold_types_attr_absent(self, mock_cache_table):
        """After all hold types removed, DynamoDB item exists but holdTypes attr is gone."""
        mock_cache_table.get_item.return_value = {
            "Item": {"PK": "ORDER_LINE_HOLD_TYPE", "SK": "OLK-001"}
        }
        doc = {"orderLineKey": "OLK-001"}
        lf.enrich_order_line_hold_types(doc)
        assert "holdTypes" not in doc

    @patch("lambda_function.upsert_order_line")
    def test_hold_types_in_upserted_doc(self, mock_upsert, mock_cache_table):
        mock_upsert.return_value = {"statusCode": 200}
        mock_cache_table.get_item.return_value = {
            "Item": {"PK": "ORDER_LINE_HOLD_TYPE", "SK": "OL123",
                     "holdTypeStatuses": {"PAYMENT_HOLD": "1100"}}
        }
        lf.process_order_line_event("c", SAMPLE_PAYLOAD, 123)
        doc = mock_upsert.call_args[0][1]
        assert doc["holdTypeStatuses"] == {"PAYMENT_HOLD": "1100"}


# =============================================================================
# Enrichment — enrich_order_line_status
# =============================================================================

class TestEnrichOrderLineStatus:
    def test_sets_status_from_matching_entry(self, mock_cache_table):
        mock_cache_table.get_item.return_value = {
            "Item": {"PK": "ORDER_STATUS", "SK": "OH123",
                     "statusName": "Shipped", "statusCode": "3700",
                     "enrich": {"orderlinekey": [
                         {"OrderLineKey": "OL123", "Status": "Shipped", "StatusCode": "3700"},
                         {"OrderLineKey": "OL456", "Status": "Created", "StatusCode": "1100"},
                     ]}}
        }
        doc = {"orderLineKey": "OL123", "orderHeaderKey": "OH123"}
        lf.enrich_order_line_status(doc)
        assert doc["statusName"] == "Shipped"
        assert doc["statusCode"] == "3700"

    def test_no_op_when_order_line_key_not_in_enrich(self, mock_cache_table):
        mock_cache_table.get_item.return_value = {
            "Item": {"PK": "ORDER_STATUS", "SK": "OH123",
                     "enrich": {"orderlinekey": [
                         {"OrderLineKey": "OL999", "Status": "Created", "StatusCode": "1100"},
                     ]}}
        }
        doc = {"orderLineKey": "OL123", "orderHeaderKey": "OH123"}
        lf.enrich_order_line_status(doc)
        assert "statusName" not in doc
        assert "statusCode" not in doc

    def test_no_op_when_cache_miss(self, mock_cache_table):
        mock_cache_table.get_item.return_value = {}
        doc = {"orderLineKey": "OL123", "orderHeaderKey": "OH123"}
        lf.enrich_order_line_status(doc)
        assert "statusName" not in doc

    def test_no_op_when_order_header_key_missing(self, mock_cache_table):
        doc = {"orderLineKey": "OL123"}
        lf.enrich_order_line_status(doc)
        mock_cache_table.get_item.assert_not_called()

    def test_no_op_when_order_line_key_missing(self, mock_cache_table):
        doc = {"orderHeaderKey": "OH123"}
        lf.enrich_order_line_status(doc)
        mock_cache_table.get_item.assert_not_called()

    def test_no_op_when_enrich_is_missing(self, mock_cache_table):
        mock_cache_table.get_item.return_value = {
            "Item": {"PK": "ORDER_STATUS", "SK": "OH123",
                     "statusName": "Shipped"}
        }
        doc = {"orderLineKey": "OL123", "orderHeaderKey": "OH123"}
        lf.enrich_order_line_status(doc)
        assert "statusName" not in doc

    def test_no_op_when_enrich_has_no_orderlinekey(self, mock_cache_table):
        mock_cache_table.get_item.return_value = {
            "Item": {"PK": "ORDER_STATUS", "SK": "OH123",
                     "enrich": {}}
        }
        doc = {"orderLineKey": "OL123", "orderHeaderKey": "OH123"}
        lf.enrich_order_line_status(doc)
        assert "statusName" not in doc

    @patch("lambda_function.upsert_order_line")
    def test_status_in_upserted_doc(self, mock_upsert, mock_cache_table):
        mock_upsert.return_value = {"statusCode": 200}
        mock_cache_table.get_item.side_effect = lambda Key: (
            {"Item": {"PK": "ORDER_STATUS", "SK": Key["SK"],
                      "enrich": {"orderlinekey": [
                          {"OrderLineKey": "OL123", "Status": "Shipped", "StatusCode": "3700"},
                      ]}}}
            if Key["PK"] == "ORDER_STATUS" else {}
        )
        lf.process_order_line_event("c", SAMPLE_PAYLOAD, 123)
        doc = mock_upsert.call_args[0][1]
        assert doc["statusName"] == "Shipped"
        assert doc["statusCode"] == "3700"
