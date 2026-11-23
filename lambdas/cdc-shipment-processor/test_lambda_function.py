"""
Unit tests for cdc-shipment-processor/lambda_function.py

Run with:
    pytest test_lambda_function.py -v
"""

import base64
import json
import os
from unittest.mock import MagicMock, patch, call

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
    """Return a minimal valid YFS_SHIPMENT payload (uppercase keys)."""
    base = {
        "ORDER_HEADER_KEY": "OHK-001",
        "SHIPMENT_KEY":     "SHP-001",
        "STATUS":           "1100.0500",
    }
    base.update(kwargs)
    return base


def make_debezium_envelope(payload: dict, op: str = "c", ts_ms: int = 1700000000000) -> dict:
    lc = {k.lower(): v for k, v in payload.items()}
    key = "before" if op == "d" else "after"
    return {"op": op, "ts_ms": ts_ms, key: lc, ("before" if op != "d" else "after"): None}


def make_smt_envelope(payload: dict, op: str = "c", ts_ms: int = 1700000000000) -> dict:
    """Create a flattened SMT format Debezium message."""
    inner = dict(payload)
    inner["__op"] = op
    inner["__source_ts_ms"] = ts_ms
    return {"schema": {"type": "struct", "fields": []}, "payload": inner}


def make_msk_event(debezium_event: dict, topic: str = "glenraven-dev.STERLING.yfs_shipment") -> dict:
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

    def test_none_returns_none(self):
        assert lf.get_value({"K": None}, "K") is None

    def test_whitespace_only_returns_none(self):
        assert lf.get_value({"K": "   "}, "K") is None


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
        assert lf.extract_table_from_topic("glenraven-dev.STERLING.yfs_shipment") == "YFS_SHIPMENT"

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
                "SHIPMENT_KEY": "SHP-001",
                "STATUS": "1100.0500",
                "__op": "c",
                "__source_ts_ms": 1773154201000
            }
        }
        op, ts, payload = lf._parse_debezium_message(event)
        assert op == "c"
        assert ts == 1773154201000
        assert "ORDER_HEADER_KEY" in payload
        assert "SHIPMENT_KEY" in payload
        assert "__op" not in payload

    def test_standard_envelope_create(self):
        event = {
            "op": "c", "ts_ms": 1700000000000,
            "before": None,
            "after": {"ORDER_HEADER_KEY": "OHK-001", "SHIPMENT_KEY": "SHP-001", "STATUS": "1100.0500"}
        }
        op, _, payload = lf._parse_debezium_message(event)
        assert op == "c"
        assert "ORDER_HEADER_KEY" in payload

    def test_standard_envelope_delete_uses_before(self):
        event = {
            "op": "d", "ts_ms": 1700000000000,
            "before": {"ORDER_HEADER_KEY": "OHK-001", "SHIPMENT_KEY": "SHP-001", "STATUS": "1100.0500"}, "after": None
        }
        op, _, payload = lf._parse_debezium_message(event)
        assert op == "d"
        assert "ORDER_HEADER_KEY" in payload

    def test_smt_op_defaults_to_create_when_empty(self):
        event = {
            "schema": {"type": "struct"},
            "payload": {
                "ORDER_HEADER_KEY": "OHK-001",
                "SHIPMENT_KEY": "SHP-001",
                "__op": "",
                "__source_ts_ms": 1700000000000
            }
        }
        op, _, _ = lf._parse_debezium_message(event)
        assert op == "c"


# ===========================================================================
# DynamoDB — upsert_shipment_status_in_cache
# ===========================================================================

class TestUpsertShipmentStatusInCache:
    def test_update_item_called_with_correct_keys(self, mock_cache_table):
        mock_cache_table.update_item.return_value = {
            "Attributes": {"shipmentStatuses": {"SHP-001": "1100.0500"}}
        }
        statuses = lf.upsert_shipment_status_in_cache("OHK-001", "SHP-001", "1100.0500")
        mock_cache_table.update_item.assert_called_once()
        kwargs = mock_cache_table.update_item.call_args[1]
        assert kwargs["Key"]["PK"] == "YFS_SHIPMENT"
        assert kwargs["Key"]["SK"] == "OHK-001"
        assert kwargs["ExpressionAttributeNames"]["#sk"] == "SHP-001"
        assert kwargs["ExpressionAttributeValues"][":st"] == "1100.0500"
        assert statuses == ["1100.0500"]

    def test_returns_unique_sorted_list(self, mock_cache_table):
        mock_cache_table.update_item.return_value = {
            "Attributes": {"shipmentStatuses": {"SHP-1": "1100", "SHP-2": "1400", "SHP-3": "1100", "SHP-4": None}}
        }
        statuses = lf.upsert_shipment_status_in_cache("OHK-001", "SHP-1", "1100")
        assert statuses == ["1100", "1400"]

    def test_initializes_map_on_validation_exception(self, mock_cache_table):
        from botocore.exceptions import ClientError
        mock_cache_table.update_item.side_effect = [
            ClientError({"Error": {"Code": "ValidationException", "Message": "msg"}}, "UpdateItem"),
            {"Attributes": {"shipmentStatuses": {"SHP-001": "1100.0500"}}}
        ]
        statuses = lf.upsert_shipment_status_in_cache("OHK-001", "SHP-001", "1100.0500")
        assert mock_cache_table.update_item.call_count == 2
        kwargs = mock_cache_table.update_item.call_args_list[1][1]
        assert ":map" in kwargs["ExpressionAttributeValues"]
        assert statuses == ["1100.0500"]

    def test_put_item_failure_raises(self, mock_cache_table):
        mock_cache_table.update_item.side_effect = Exception("connection error")
        with pytest.raises(Exception, match="connection error"):
            lf.upsert_shipment_status_in_cache("OHK-001", "SHP-001", "1100.0500")


# ===========================================================================
# DynamoDB — remove_shipment_status_from_cache
# ===========================================================================

class TestRemoveShipmentStatusFromCache:
    def test_remove_expression_used(self, mock_cache_table):
        mock_cache_table.update_item.return_value = {
            "Attributes": {"shipmentStatuses": {"SHP-002": "3700"}}
        }
        statuses = lf.remove_shipment_status_from_cache("OHK-001", "SHP-001")
        mock_cache_table.update_item.assert_called_once()
        kwargs = mock_cache_table.update_item.call_args[1]
        assert "REMOVE shipmentStatuses.#sk" in kwargs["UpdateExpression"]
        assert statuses == ["3700"]

    def test_condition_check_failure_returns_empty_list(self, mock_cache_table):
        from botocore.exceptions import ClientError
        mock_cache_table.update_item.side_effect = ClientError(
            {"Error": {"Code": "ConditionalCheckFailedException", "Message": "condition"}},
            "UpdateItem"
        )
        statuses = lf.remove_shipment_status_from_cache("OHK-001", "SHP-001")
        assert statuses == []


# ===========================================================================
# DynamoDB — soft_delete_cache_item
# ===========================================================================

class TestSoftDeleteCacheItem:
    def test_phantom_delete_caught_gracefully(self, mock_cache_table):
        from botocore.exceptions import ClientError
        mock_cache_table.update_item.side_effect = ClientError(
            {"Error": {"Code": "ConditionalCheckFailedException", "Message": "condition"}},
            "UpdateItem"
        )
        # Should not raise
        lf.soft_delete_cache_item("YFS_SHIPMENT", "OHK-PHANTOM")

    def test_non_conditional_error_re_raised(self, mock_cache_table):
        from botocore.exceptions import ClientError
        mock_cache_table.update_item.side_effect = ClientError(
            {"Error": {"Code": "InternalServerError", "Message": "boom"}},
            "UpdateItem"
        )
        with pytest.raises(ClientError):
            lf.soft_delete_cache_item("YFS_SHIPMENT", "OHK-001")


# ===========================================================================
# OpenSearch — update_shipment_status_in_index
# ===========================================================================

class TestUpdateShipmentStatusInIndex:
    def test_partial_doc_update_with_status_list(self, mock_opensearch):
        lf.update_shipment_status_in_index(mock_opensearch, "OHK-001", ["1100.0500"])
        kwargs = mock_opensearch.update.call_args[1]
        assert kwargs["index"] == lf.INDEX_ORDERS
        assert kwargs["id"] == "OHK-001"
        assert kwargs["body"] == {"doc": {"shipmentStatus": ["1100.0500"]}}

    def test_empty_list_sets_null(self, mock_opensearch):
        lf.update_shipment_status_in_index(mock_opensearch, "OHK-001", [])
        kwargs = mock_opensearch.update.call_args[1]
        assert kwargs["body"] == {"doc": {"shipmentStatus": None}}

    def test_404_logged_not_raised(self, mock_opensearch):
        mock_opensearch.update.side_effect = Exception("404 not_found")
        # Should not raise
        lf.update_shipment_status_in_index(mock_opensearch, "OHK-MISSING", ["1100.0500"])


# ===========================================================================
# process_shipment_event
# ===========================================================================

class TestProcessShipmentEvent:
    def test_missing_order_header_key_returns_200_skip(self, mock_opensearch, mock_cache_table):
        result = lf.process_shipment_event("c", {"STATUS": "1100.0500", "SHIPMENT_KEY": "SHP-1"})
        assert result["statusCode"] == 200
        assert "Missing ORDER_HEADER_KEY" in result["body"]

    def test_missing_shipment_key_returns_200_skip(self, mock_opensearch, mock_cache_table):
        result = lf.process_shipment_event("c", {"STATUS": "1100.0500", "ORDER_HEADER_KEY": "OHK-1"})
        assert result["statusCode"] == 200
        assert "Missing SHIPMENT_KEY" in result["body"]

    def test_create_saves_and_updates(self, mock_opensearch, mock_cache_table):
        mock_cache_table.update_item.return_value = {"Attributes": {"shipmentStatuses": {"SHP-1": "1100"}}}
        result = lf.process_shipment_event("c", make_payload())
        assert result["statusCode"] == 200
        mock_cache_table.update_item.assert_called_once()
        mock_opensearch.update.assert_called_once()
        assert mock_opensearch.update.call_args[1]["body"]["doc"]["shipmentStatus"] == ["1100"]

    def test_update_saves_and_updates(self, mock_opensearch, mock_cache_table):
        mock_cache_table.update_item.return_value = {"Attributes": {"shipmentStatuses": {"SHP-1": "3700"}}}
        result = lf.process_shipment_event("u", make_payload())
        assert result["statusCode"] == 200
        mock_cache_table.update_item.assert_called_once()
        mock_opensearch.update.assert_called_once()

    def test_delete_clears_status(self, mock_opensearch, mock_cache_table):
        # mock update_item returns empty map
        mock_cache_table.update_item.return_value = {"Attributes": {"shipmentStatuses": {}}}
        result = lf.process_shipment_event("d", make_payload())
        assert result["statusCode"] == 200
        # DynamoDB remove
        mock_cache_table.update_item.assert_called_once()
        # OpenSearch update with empty list
        mock_opensearch.update.assert_called_once()
        assert mock_opensearch.update.call_args[1]["body"]["doc"]["shipmentStatus"] is None

    def test_create_with_null_status(self, mock_opensearch, mock_cache_table):
        mock_cache_table.update_item.return_value = {"Attributes": {"shipmentStatuses": {"SHP-1": None}}}
        result = lf.process_shipment_event("c", make_payload(STATUS=None))
        assert result["statusCode"] == 200
        mock_cache_table.update_item.assert_called_once()
        mock_opensearch.update.assert_called_once()


# ===========================================================================
# process_single_event
# ===========================================================================

class TestProcessSingleEvent:
    def test_routes_yfs_shipment(self, mock_opensearch, mock_cache_table):
        result = lf.process_single_event("YFS_SHIPMENT", "c", make_payload())
        assert result["statusCode"] == 200

    def test_unknown_table_returns_400(self, mock_opensearch, mock_cache_table):
        result = lf.process_single_event("UNKNOWN_TABLE", "c", make_payload())
        assert result["statusCode"] == 400
        assert "Unknown table" in result["body"]


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
        assert "errors 0" in result["body"]

    def test_multiple_messages_processed(self, mock_opensearch, mock_cache_table):
        topic = "glenraven-dev.STERLING.yfs_shipment"
        def make_msg(status):
            env = make_debezium_envelope(make_payload(STATUS=status))
            return {"topic": topic, "offset": 0,
                    "value": base64.b64encode(json.dumps(env).encode()).decode()}
        event = {
            "eventSource": "aws:kafka",
            "records": {f"{topic}-0": [make_msg("1100"), make_msg("3700")]}
        }
        result = lf.process_msk_event(event)
        assert "Processed 2" in result["body"]

    def test_smt_format_processed(self, mock_opensearch, mock_cache_table):
        smt = make_smt_envelope(make_payload(), op="c")
        event = make_msk_event(smt)
        result = lf.process_msk_event(event)
        assert result["statusCode"] == 200
        assert "Processed 1" in result["body"]


# ===========================================================================
# lambda_handler
# ===========================================================================

class TestLambdaHandler:
    def test_msk_event_routes_correctly(self, mock_opensearch, mock_cache_table):
        event = make_msk_event(make_debezium_envelope(make_payload()))
        result = lf.lambda_handler(event, None)
        assert result["statusCode"] == 200

    def test_direct_invocation_routes_correctly(self, mock_opensearch, mock_cache_table):
        event = {"table": "YFS_SHIPMENT", "operation": "c", "payload": make_payload()}
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

    @patch("lambda_function.boto3.Session")
    @patch("lambda_function.OPENSEARCH_AUTH", "iam")
    def test_iam_no_credentials_raises(self, mock_session):
        mock_session.return_value.get_credentials.return_value = None
        with pytest.raises(RuntimeError, match="AWS credentials not found"):
            lf.get_opensearch_client()
