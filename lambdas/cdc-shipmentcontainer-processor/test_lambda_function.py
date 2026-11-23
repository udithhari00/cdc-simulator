"""
Unit tests for cdc-shipmentcontainer-processor/lambda_function.py

Run with:
    pytest test_lambda_function.py -v
"""

import base64
import json
import os
from unittest.mock import MagicMock, patch, call

import pytest

os.environ.setdefault("AWS_REGION", "us-east-1")
os.environ.setdefault("DYNAMODB_ENDPOINT", "http://localhost:8000")

with patch("boto3.resource"):
    import lambda_function as lf


# ===========================================================================
# Helpers
# ===========================================================================

def make_payload(**kwargs) -> dict:
    """Return a minimal valid YFS_SHIPMENT_CONTAINER payload (uppercase keys)."""
    base = {
        "SHIPMENT_CONTAINER_KEY": "SC-001",
        "SHIPMENT_KEY": "SHP-001",
        "TRACKING_NO": "1Z999AA10123456784",
        "SCAC": "UPSS",
        "CARRIER_SERVICE_CODE": "UPS_GROUND",
        "CONTAINER_NO": "CTN-001",
        "CONTAINER_TYPE": "CASE",
    }
    base.update(kwargs)
    return base


def make_debezium_envelope(payload: dict, op: str = "c", ts_ms: int = 1700000000000) -> dict:
    lc = {k.lower(): v for k, v in payload.items()}
    key = "before" if op == "d" else "after"
    return {"op": op, "ts_ms": ts_ms, key: lc, ("before" if op != "d" else "after"): None}


def make_smt_event(payload: dict, op: str = "u", ts_ms: int = 1773154201000) -> dict:
    return {
        "schema": {"type": "struct", "fields": []},
        "payload": {**payload, "__op": op, "__source_ts_ms": ts_ms}
    }


def make_msk_event(debezium_event: dict, topic: str = "glenraven-dev.STERLING.YFS_SHIPMENT_CONTAINER") -> dict:
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
def mock_cache_table():
    table = MagicMock()
    table.get_item.return_value = {}
    table.query.return_value = {'Items': []}
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


class TestExtractTableFromTopic:
    def test_known_topic(self):
        assert lf.extract_table_from_topic("glenraven-dev.STERLING.yfs_shipment_container") == "YFS_SHIPMENT_CONTAINER"

    def test_unknown_uppercased(self):
        assert lf.extract_table_from_topic("prefix.schema.other_table") == "OTHER_TABLE"


# ===========================================================================
# _parse_debezium_message
# ===========================================================================

class TestParseDebeziumMessage:
    def test_flattened_smt_format(self):
        event = make_smt_event(make_payload())
        op, ts, payload = lf._parse_debezium_message(event)
        assert op == "u"
        assert "SHIPMENT_CONTAINER_KEY" in payload
        assert "__op" not in payload

    def test_standard_envelope_create(self):
        event = make_debezium_envelope(make_payload())
        op, ts, payload = lf._parse_debezium_message(event)
        assert op == "c"
        assert "shipment_container_key" in payload

    def test_standard_envelope_delete(self):
        event = make_debezium_envelope(make_payload(), op="d")
        op, _, payload = lf._parse_debezium_message(event)
        assert op == "d"
        assert "shipment_container_key" in payload


# ===========================================================================
# process_shipment_container_event — Phase 1 (DynamoDB cache + reverse row)
# ===========================================================================

class TestProcessShipmentContainerEvent:
    def test_missing_key_returns_400(self, mock_cache_table):
        result = lf.process_shipment_container_event({}, "c")
        assert result["statusCode"] == 400

    def test_create_writes_forward_row(self, mock_cache_table):
        result = lf.process_shipment_container_event(make_payload(), "c")
        assert result["statusCode"] == 200

        put_calls = mock_cache_table.put_item.call_args_list
        forward = [c for c in put_calls if c[1]["Item"]["PK"] == "YFS_SHIPMENT_CONTAINER"]
        assert len(forward) == 1
        item = forward[0][1]["Item"]
        assert item["SK"] == "SC-001"
        assert item["shipmentKey"] == "SHP-001"
        assert item["trackingNo"] == "1Z999AA10123456784"
        assert item["scac"] == "UPSS"
        assert item["carrierServiceCode"] == "UPS_GROUND"
        assert item["containerNo"] == "CTN-001"

    def test_create_writes_by_shipment_reverse_row(self, mock_cache_table):
        lf.process_shipment_container_event(make_payload(), "c")
        put_calls = mock_cache_table.put_item.call_args_list
        by_shipment = [c for c in put_calls if c[1]["Item"]["PK"] == "YFS_SHIPMENT_CONTAINER_BY_SHIPMENT"]
        assert len(by_shipment) == 1
        item = by_shipment[0][1]["Item"]
        assert item["SK"] == "SHP-001#SC-001"
        assert item["trackingNo"] == "1Z999AA10123456784"

    def test_delete_soft_deletes_both_rows(self, mock_cache_table):
        result = lf.process_shipment_container_event(make_payload(), "d")
        assert result["statusCode"] == 200
        # forward + reverse = 2 soft deletes
        assert mock_cache_table.update_item.call_count == 2

    def test_delete_hard_deletes_when_enabled(self, mock_cache_table):
        lf.ENABLE_HARD_DELETE = True
        try:
            result = lf.process_shipment_container_event(make_payload(), "d")
            assert result["statusCode"] == 200
            assert mock_cache_table.delete_item.call_count == 2
        finally:
            lf.ENABLE_HARD_DELETE = False

    def test_none_fields_excluded(self, mock_cache_table):
        payload = make_payload(CONTAINER_TYPE=None, CONTAINER_SCM=None)
        lf.process_shipment_container_event(payload, "c")
        put_calls = mock_cache_table.put_item.call_args_list
        forward = [c for c in put_calls if c[1]["Item"]["PK"] == "YFS_SHIPMENT_CONTAINER"]
        item = forward[0][1]["Item"]
        assert "containerType" not in item
        assert "containerScm" not in item


# ===========================================================================
# process_shipment_container_event — Phase 2 (tracking cache rebuild)
# ===========================================================================

class TestProcessShipmentContainerEventPhase2:
    def test_rebuild_called_for_affected_order_lines(self, mock_cache_table):
        mock_cache_table.query.side_effect = [
            # get_order_line_keys_by_shipment
            {'Items': [
                {'orderLineKey': 'OLK-001', 'orderHeaderKey': 'OHK-001'},
                {'orderLineKey': 'OLK-002', 'orderHeaderKey': 'OHK-001'},
            ]},
            # rebuild for OLK-001: find shipment lines by order line
            {'Items': [{'shipmentKey': 'SHP-001', 'orderHeaderKey': 'OHK-001'}]},
            # rebuild for OLK-001: find containers
            {'Items': [{'trackingNo': '1Z999AA10123456784', 'scac': 'UPSS', 'carrierServiceCode': 'UPS_GROUND'}]},
            # rebuild for OLK-002: find shipment lines by order line
            {'Items': [{'shipmentKey': 'SHP-001', 'orderHeaderKey': 'OHK-001'}]},
            # rebuild for OLK-002: find containers
            {'Items': [{'trackingNo': '1Z999AA10123456784', 'scac': 'UPSS', 'carrierServiceCode': 'UPS_GROUND'}]},
        ]

        result = lf.process_shipment_container_event(make_payload(), "c")
        assert result["statusCode"] == 200

        put_calls = mock_cache_table.put_item.call_args_list
        tracking = [c for c in put_calls if c[1]["Item"]["PK"] == "TRACKING_NO"]
        assert len(tracking) == 2

        sks = {c[1]["Item"]["SK"] for c in tracking}
        assert sks == {"OHK-001#OLK-001", "OHK-001#OLK-002"}

    def test_no_shipment_key_skips_rebuild(self, mock_cache_table):
        payload = make_payload(SHIPMENT_KEY=None)
        result = lf.process_shipment_container_event(payload, "c")
        assert result["statusCode"] == 200
        # Should only write forward row (no reverse, no rebuild)
        put_calls = mock_cache_table.put_item.call_args_list
        assert len(put_calls) == 1  # Just the forward row

    def test_rebuild_error_does_not_fail_event(self, mock_cache_table):
        mock_cache_table.query.side_effect = Exception("DynamoDB down")
        result = lf.process_shipment_container_event(make_payload(), "c")
        assert result["statusCode"] == 200


# ===========================================================================
# rebuild_tracking_cache
# ===========================================================================

class TestRebuildTrackingCache:
    def test_multiple_shipments_aggregated(self, mock_cache_table):
        mock_cache_table.query.side_effect = [
            # Find shipment lines by order line
            {'Items': [
                {'shipmentKey': 'SHP-001', 'orderHeaderKey': 'OHK-001'},
                {'shipmentKey': 'SHP-002', 'orderHeaderKey': 'OHK-001'},
            ]},
            # Containers for SHP-001
            {'Items': [{'trackingNo': 'TRK-A', 'scac': 'FEDX', 'carrierServiceCode': 'FG'}]},
            # Containers for SHP-002
            {'Items': [{'trackingNo': 'TRK-B', 'scac': 'UPSS', 'carrierServiceCode': 'UG'}]},
        ]

        lf.rebuild_tracking_cache('OLK-001', 'OHK-001')

        put_calls = [c for c in mock_cache_table.put_item.call_args_list
                     if c[1]["Item"]["PK"] == "TRACKING_NO"]
        assert len(put_calls) == 1
        item = put_calls[0][1]["Item"]
        assert "TRK-A" in item["trackingNumbers"]
        assert "TRK-B" in item["trackingNumbers"]

    def test_empty_tracking_no_skipped(self, mock_cache_table):
        mock_cache_table.query.side_effect = [
            {'Items': [{'shipmentKey': 'SHP-001', 'orderHeaderKey': 'OHK-001'}]},
            {'Items': [{'trackingNo': ''}]},  # empty tracking no
        ]

        lf.rebuild_tracking_cache('OLK-001', 'OHK-001')

        # Should soft-delete (no valid tracking numbers)
        mock_cache_table.update_item.assert_called()

    def test_missing_shipment_cache_uses_empty_scac(self, mock_cache_table):
        mock_cache_table.query.side_effect = [
            {'Items': [{'shipmentKey': 'SHP-001', 'orderHeaderKey': 'OHK-001'}]},
            {'Items': [{'trackingNo': 'TRK-X'}]},
        ]

        lf.rebuild_tracking_cache('OLK-001', 'OHK-001')

        put_calls = [c for c in mock_cache_table.put_item.call_args_list
                     if c[1]["Item"]["PK"] == "TRACKING_NO"]
        assert len(put_calls) == 1
        item = put_calls[0][1]["Item"]
        assert item["scac"] == [""]
        assert item["carrierServiceCode"] == [""]


# ===========================================================================
# MSK batch processor
# ===========================================================================

class TestProcessMskEvent:
    def _make_event(self, op="c", extra=None):
        env = make_debezium_envelope(make_payload(**(extra or {})), op=op)
        return make_msk_event(env)

    def test_successful_batch_returns_200(self, mock_cache_table):
        result = lf.process_msk_event(self._make_event())
        assert result["statusCode"] == 200
        assert "Processed 1" in result["body"]

    def test_delete_batch(self, mock_cache_table):
        result = lf.process_msk_event(self._make_event(op="d"))
        assert result["statusCode"] == 200

    def test_malformed_value_counted_as_error(self, mock_cache_table):
        event = {
            "eventSource": "aws:kafka",
            "records": {"topic-0": [{"topic": "topic", "offset": 0, "value": "!!!bad!!!"}]}
        }
        result = lf.process_msk_event(event)
        assert "errors 1" in result["body"]

    def test_multiple_messages_processed(self, mock_cache_table):
        topic = "glenraven-dev.STERLING.yfs_shipment_container"

        def make_msg(key):
            env = make_debezium_envelope(make_payload(SHIPMENT_CONTAINER_KEY=key))
            return {"topic": topic, "offset": 0,
                    "value": base64.b64encode(json.dumps(env).encode()).decode()}

        event = {
            "eventSource": "aws:kafka",
            "records": {f"{topic}-0": [make_msg("K1"), make_msg("K2")]}
        }
        result = lf.process_msk_event(event)
        assert result["statusCode"] == 200
        assert "Processed 2" in result["body"]


# ===========================================================================
# lambda_handler
# ===========================================================================

class TestLambdaHandler:
    def test_msk_event_routes_correctly(self, mock_cache_table):
        event = make_msk_event(make_debezium_envelope(make_payload()))
        result = lf.lambda_handler(event, None)
        assert result["statusCode"] == 200

    def test_direct_invocation_routes_correctly(self, mock_cache_table):
        event = {"table": "YFS_SHIPMENT_CONTAINER", "operation": "c", "payload": make_payload()}
        result = lf.lambda_handler(event, None)
        assert result["statusCode"] == 200

    def test_unhandled_exception_returns_500(self, mock_cache_table):
        with patch.object(lf, "process_msk_event", side_effect=RuntimeError("boom")):
            result = lf.lambda_handler({"eventSource": "aws:kafka", "records": {}}, None)
        assert result["statusCode"] == 500

    def test_unknown_table_returns_400(self, mock_cache_table):
        event = {"table": "UNKNOWN_TABLE", "operation": "c", "payload": make_payload()}
        result = lf.lambda_handler(event, None)
        assert result["statusCode"] == 400


# ===========================================================================
# SMT format end-to-end
# ===========================================================================

class TestSmtFormatEndToEnd:
    def test_smt_create_event(self, mock_cache_table):
        smt_event = make_smt_event(make_payload(), op="c")
        msk_event = make_msk_event(smt_event)
        result = lf.process_msk_event(msk_event)
        assert result["statusCode"] == 200
        put_calls = mock_cache_table.put_item.call_args_list
        forward = [c for c in put_calls if c[1]["Item"]["PK"] == "YFS_SHIPMENT_CONTAINER"]
        assert len(forward) == 1
        assert forward[0][1]["Item"]["trackingNo"] == "1Z999AA10123456784"

    def test_smt_update_changes_tracking_no(self, mock_cache_table):
        smt_event = make_smt_event(make_payload(TRACKING_NO="NEW-TRK-999"), op="u")
        msk_event = make_msk_event(smt_event)
        result = lf.process_msk_event(msk_event)
        assert result["statusCode"] == 200
        put_calls = mock_cache_table.put_item.call_args_list
        forward = [c for c in put_calls if c[1]["Item"]["PK"] == "YFS_SHIPMENT_CONTAINER"]
        assert forward[0][1]["Item"]["trackingNo"] == "NEW-TRK-999"
