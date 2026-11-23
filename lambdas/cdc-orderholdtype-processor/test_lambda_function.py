"""
Unit tests for cdc-orderholdtype-processor/lambda_function.py

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
    """Return a minimal valid YFS_ORDER_HOLD_TYPE payload (uppercase keys)."""
    base = {
        "ORDER_HEADER_KEY": "OHK-001",
        "ORDER_LINE_KEY":   "OLK-001",
        "HOLD_TYPE":        "FRAUD_REVIEW",
        "STATUS":           "1300",
    }
    base.update(kwargs)
    return base


def make_debezium_envelope(payload: dict, op: str = "c", ts_ms: int = 1700000000000) -> dict:
    lc = {k.lower(): v for k, v in payload.items()}
    key = "before" if op == "d" else "after"
    return {"op": op, "ts_ms": ts_ms, key: lc, ("before" if op != "d" else "after"): None}


def make_msk_event(debezium_event: dict, topic: str = "sterling.public.yfs_order_hold_type") -> dict:
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


class TestGetIntValue:
    def test_returns_integer(self):
        assert lf.get_int_value({"K": "1300"}, "K") == 1300

    def test_returns_integer_from_int(self):
        assert lf.get_int_value({"K": 1200}, "K") == 1200

    def test_strips_whitespace(self):
        assert lf.get_int_value({"K": " 1100 "}, "K") == 1100

    def test_missing_returns_none(self):
        assert lf.get_int_value({}, "K") is None

    def test_none_returns_none(self):
        assert lf.get_int_value({"K": None}, "K") is None

    def test_whitespace_only_returns_none(self):
        assert lf.get_int_value({"K": "   "}, "K") is None

    def test_non_numeric_returns_none(self):
        assert lf.get_int_value({"K": "abc"}, "K") is None


# ===========================================================================
# Topic extraction
# ===========================================================================

class TestExtractTableFromTopic:
    def test_known_topic(self):
        assert lf.extract_table_from_topic("sterling.public.yfs_order_hold_type") == "YFS_ORDER_HOLD_TYPE"

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
                "HOLD_TYPE": "FRAUD_REVIEW",
                "__op": "c",
                "__source_ts_ms": 1773154201000
            }
        }
        op, ts, payload = lf._parse_debezium_message(event)
        assert op == "c"
        assert ts == 1773154201000
        assert "ORDER_HEADER_KEY" in payload
        assert "__op" not in payload

    def test_standard_envelope_create(self):
        event = {
            "op": "c", "ts_ms": 1700000000000,
            "before": None,
            "after": {"ORDER_HEADER_KEY": "OHK-001", "HOLD_TYPE": "CREDIT_HOLD"}
        }
        op, _, payload = lf._parse_debezium_message(event)
        assert op == "c"
        assert "ORDER_HEADER_KEY" in payload

    def test_standard_envelope_delete_uses_before(self):
        event = {
            "op": "d", "ts_ms": 1700000000000,
            "before": {"ORDER_HEADER_KEY": "OHK-001", "HOLD_TYPE": "CREDIT_HOLD"}, "after": None
        }
        op, _, payload = lf._parse_debezium_message(event)
        assert op == "d"
        assert "ORDER_HEADER_KEY" in payload


# ===========================================================================
# _status_priority
# ===========================================================================

class TestStatusPriority:
    def test_none_returns_minus_one(self):
        assert lf._status_priority(None) == -1

    def test_valid_status_returns_int(self):
        assert lf._status_priority("1300") == 1300

    def test_1200(self):
        assert lf._status_priority("1200") == 1200

    def test_1100(self):
        assert lf._status_priority("1100") == 1100

    def test_invalid_string_returns_minus_one(self):
        assert lf._status_priority("abc") == -1

    def test_priority_ordering(self):
        assert lf._status_priority("1300") > lf._status_priority("1200")
        assert lf._status_priority("1200") > lf._status_priority("1100")
        assert lf._status_priority("1100") > lf._status_priority(None)


# ===========================================================================
# DynamoDB cache — upsert_hold_type_status_in_cache
# ===========================================================================

class TestUpsertHoldTypeStatusInCache:
    def test_uses_set_expression_for_map_key(self, mock_cache_table):
        # get_item returns empty (no existing entry) so upsert proceeds
        mock_cache_table.get_item.return_value = {}
        lf.upsert_hold_type_status_in_cache("ORDER_HOLD_TYPE", "OHK-001", "FRAUD_REVIEW", "1300")
        mock_cache_table.update_item.assert_called_once()
        kwargs = mock_cache_table.update_item.call_args[1]
        assert "SET holdTypeStatuses.#ht" in kwargs["UpdateExpression"]
        assert kwargs["ExpressionAttributeNames"]["#ht"] == "FRAUD_REVIEW"
        assert kwargs["ExpressionAttributeValues"][":st"] == "1300"

    def test_correct_pk_and_sk(self, mock_cache_table):
        mock_cache_table.get_item.return_value = {}
        lf.upsert_hold_type_status_in_cache("ORDER_HOLD_TYPE", "OHK-001", "FRAUD_REVIEW", "1300")
        key = mock_cache_table.update_item.call_args[1]["Key"]
        assert key["PK"] == "ORDER_HOLD_TYPE"
        assert key["SK"] == "OHK-001"

    def test_order_line_hold_type_entity(self, mock_cache_table):
        mock_cache_table.get_item.return_value = {}
        lf.upsert_hold_type_status_in_cache("ORDER_LINE_HOLD_TYPE", "OLK-001", "PAYMENT_HOLD", "1200")
        key = mock_cache_table.update_item.call_args[1]["Key"]
        assert key["PK"] == "ORDER_LINE_HOLD_TYPE"
        assert key["SK"] == "OLK-001"

    def test_null_status_stored_as_none_when_no_existing(self, mock_cache_table):
        mock_cache_table.get_item.return_value = {}
        lf.upsert_hold_type_status_in_cache("ORDER_HOLD_TYPE", "OHK-001", "FRAUD_REVIEW", None)
        kwargs = mock_cache_table.update_item.call_args[1]
        assert kwargs["ExpressionAttributeValues"][":st"] is None

    def test_validation_error_initializes_map(self, mock_cache_table):
        """When holdTypeStatuses attribute doesn't exist, DynamoDB raises ValidationException.
        The function should catch it and initialize the map."""
        from botocore.exceptions import ClientError
        mock_cache_table.get_item.return_value = {}
        mock_cache_table.update_item.side_effect = [
            ClientError(
                {"Error": {"Code": "ValidationException", "Message": "path does not exist"}},
                "UpdateItem"
            ),
            None,  # second call succeeds
        ]
        lf.upsert_hold_type_status_in_cache("ORDER_HOLD_TYPE", "OHK-001", "FRAUD_REVIEW", "1300")
        assert mock_cache_table.update_item.call_count == 2
        # Second call should initialize the map
        second_kwargs = mock_cache_table.update_item.call_args_list[1][1]
        assert ":map" in second_kwargs["ExpressionAttributeValues"]
        assert second_kwargs["ExpressionAttributeValues"][":map"] == {"FRAUD_REVIEW": "1300"}

    def test_generic_exception_logged_not_raised(self, mock_cache_table):
        mock_cache_table.get_item.return_value = {}
        mock_cache_table.update_item.side_effect = Exception("connection error")
        # Should not raise — logs error internally
        lf.upsert_hold_type_status_in_cache("ORDER_HOLD_TYPE", "OHK-001", "FRAUD_REVIEW", "1300")

    # --- Max-wins priority tests ---

    def test_higher_status_overwrites_lower(self, mock_cache_table):
        """1300 should overwrite existing 1200."""
        mock_cache_table.get_item.return_value = {
            "Item": {"holdTypeStatuses": {"FRAUD_REVIEW": "1200"}}
        }
        lf.upsert_hold_type_status_in_cache("ORDER_HOLD_TYPE", "OHK-001", "FRAUD_REVIEW", "1300")
        mock_cache_table.update_item.assert_called_once()

    def test_lower_status_skipped_when_existing_higher(self, mock_cache_table):
        """1200 should NOT overwrite existing 1300 — max-wins."""
        mock_cache_table.get_item.return_value = {
            "Item": {"holdTypeStatuses": {"FRAUD_REVIEW": "1300"}}
        }
        lf.upsert_hold_type_status_in_cache("ORDER_HOLD_TYPE", "OHK-001", "FRAUD_REVIEW", "1200")
        mock_cache_table.update_item.assert_not_called()

    def test_equal_status_overwrites(self, mock_cache_table):
        """Same status (1300 == 1300) should still write (idempotent)."""
        mock_cache_table.get_item.return_value = {
            "Item": {"holdTypeStatuses": {"FRAUD_REVIEW": "1300"}}
        }
        lf.upsert_hold_type_status_in_cache("ORDER_HOLD_TYPE", "OHK-001", "FRAUD_REVIEW", "1300")
        mock_cache_table.update_item.assert_called_once()

    def test_null_does_not_overwrite_existing_status(self, mock_cache_table):
        """null should NOT overwrite existing 1300 — 1300 > null."""
        mock_cache_table.get_item.return_value = {
            "Item": {"holdTypeStatuses": {"FRAUD_REVIEW": "1300"}}
        }
        lf.upsert_hold_type_status_in_cache("ORDER_HOLD_TYPE", "OHK-001", "FRAUD_REVIEW", None)
        mock_cache_table.update_item.assert_not_called()

    def test_status_overwrites_null(self, mock_cache_table):
        """1100 should overwrite existing null — 1100 > null."""
        mock_cache_table.get_item.return_value = {
            "Item": {"holdTypeStatuses": {"FRAUD_REVIEW": None}}
        }
        lf.upsert_hold_type_status_in_cache("ORDER_HOLD_TYPE", "OHK-001", "FRAUD_REVIEW", "1100")
        mock_cache_table.update_item.assert_called_once()

    def test_new_hold_type_writes_when_others_exist(self, mock_cache_table):
        """A new hold type should be added even when other hold types exist with higher status."""
        mock_cache_table.get_item.return_value = {
            "Item": {"holdTypeStatuses": {"CREDIT_HOLD": "1300"}}
        }
        lf.upsert_hold_type_status_in_cache("ORDER_HOLD_TYPE", "OHK-001", "FRAUD_REVIEW", "1100")
        mock_cache_table.update_item.assert_called_once()


# ===========================================================================
# DynamoDB cache — remove_hold_type_status_from_cache
# ===========================================================================

class TestRemoveHoldTypeStatusFromCache:
    def test_uses_remove_expression_for_map_key(self, mock_cache_table):
        lf.remove_hold_type_status_from_cache("ORDER_HOLD_TYPE", "OHK-001", "FRAUD_REVIEW")
        kwargs = mock_cache_table.update_item.call_args[1]
        assert "REMOVE holdTypeStatuses.#ht" in kwargs["UpdateExpression"]
        assert kwargs["ExpressionAttributeNames"]["#ht"] == "FRAUD_REVIEW"

    def test_has_condition_expression_to_prevent_phantom(self, mock_cache_table):
        lf.remove_hold_type_status_from_cache("ORDER_HOLD_TYPE", "OHK-001", "FRAUD_REVIEW")
        kwargs = mock_cache_table.update_item.call_args[1]
        assert "ConditionExpression" in kwargs
        assert "attribute_exists" in kwargs["ConditionExpression"]

    def test_phantom_delete_caught_gracefully(self, mock_cache_table):
        from botocore.exceptions import ClientError
        mock_cache_table.update_item.side_effect = ClientError(
            {"Error": {"Code": "ConditionalCheckFailedException", "Message": "condition"}},
            "UpdateItem"
        )
        # Should not raise
        lf.remove_hold_type_status_from_cache("ORDER_HOLD_TYPE", "OHK-PHANTOM", "HOLD_X")

    def test_non_conditional_error_re_raised(self, mock_cache_table):
        from botocore.exceptions import ClientError
        mock_cache_table.update_item.side_effect = ClientError(
            {"Error": {"Code": "InternalServerError", "Message": "boom"}},
            "UpdateItem"
        )
        with pytest.raises(ClientError):
            lf.remove_hold_type_status_from_cache("ORDER_HOLD_TYPE", "OHK-001", "HOLD_X")

    def test_generic_exception_logged_not_raised(self, mock_cache_table):
        mock_cache_table.update_item.side_effect = Exception("timeout")
        # Generic exceptions are caught and logged
        lf.remove_hold_type_status_from_cache("ORDER_HOLD_TYPE", "OHK-001", "HOLD_X")


# ===========================================================================
# DynamoDB cache — get_hold_type_statuses_from_cache
# ===========================================================================

class TestGetHoldTypeStatusesFromCache:
    def test_returns_map_from_item(self, mock_cache_table):
        mock_cache_table.get_item.return_value = {
            "Item": {"PK": "ORDER_HOLD_TYPE", "SK": "OHK-001",
                     "holdTypeStatuses": {"FRAUD_REVIEW": "1300", "CREDIT_HOLD": "1200"}}
        }
        result = lf.get_hold_type_statuses_from_cache("ORDER_HOLD_TYPE", "OHK-001")
        assert result == {"FRAUD_REVIEW": "1300", "CREDIT_HOLD": "1200"}

    def test_returns_empty_dict_when_no_item(self, mock_cache_table):
        mock_cache_table.get_item.return_value = {}
        result = lf.get_hold_type_statuses_from_cache("ORDER_HOLD_TYPE", "OHK-001")
        assert result == {}

    def test_returns_empty_dict_when_no_statuses_attribute(self, mock_cache_table):
        mock_cache_table.get_item.return_value = {
            "Item": {"PK": "ORDER_HOLD_TYPE", "SK": "OHK-001"}
        }
        result = lf.get_hold_type_statuses_from_cache("ORDER_HOLD_TYPE", "OHK-001")
        assert result == {}

    def test_returns_empty_dict_on_exception(self, mock_cache_table):
        mock_cache_table.get_item.side_effect = Exception("timeout")
        result = lf.get_hold_type_statuses_from_cache("ORDER_HOLD_TYPE", "OHK-001")
        assert result == {}

    def test_handles_null_status_values(self, mock_cache_table):
        mock_cache_table.get_item.return_value = {
            "Item": {"PK": "ORDER_HOLD_TYPE", "SK": "OHK-001",
                     "holdTypeStatuses": {"FRAUD_REVIEW": None, "CREDIT_HOLD": "1200"}}
        }
        result = lf.get_hold_type_statuses_from_cache("ORDER_HOLD_TYPE", "OHK-001")
        assert result == {"FRAUD_REVIEW": None, "CREDIT_HOLD": "1200"}


# ===========================================================================
# OpenSearch — upsert_hold_type_status_in_index
# ===========================================================================

class TestUpsertHoldTypeStatusInIndex:
    def test_uses_painless_script_with_holdtype_and_status(self, mock_opensearch):
        lf.upsert_hold_type_status_in_index(mock_opensearch, "orders", "OHK-001", "FRAUD_REVIEW", "1300")
        kwargs = mock_opensearch.update.call_args[1]
        assert "script" in kwargs["body"]
        assert kwargs["body"]["script"]["lang"] == "painless"
        assert kwargs["body"]["script"]["params"]["holdType"] == "FRAUD_REVIEW"
        assert kwargs["body"]["script"]["params"]["status"] == "1300"

    def test_targets_correct_index_and_id(self, mock_opensearch):
        lf.upsert_hold_type_status_in_index(mock_opensearch, "orders", "OHK-001", "HOLD_X", "1200")
        kwargs = mock_opensearch.update.call_args[1]
        assert kwargs["index"] == "orders"
        assert kwargs["id"] == "OHK-001"

    def test_null_status_passed_to_script(self, mock_opensearch):
        lf.upsert_hold_type_status_in_index(mock_opensearch, "orders", "OHK-001", "FRAUD_REVIEW", None)
        kwargs = mock_opensearch.update.call_args[1]
        assert kwargs["body"]["script"]["params"]["status"] is None

    def test_404_logged_not_raised(self, mock_opensearch):
        mock_opensearch.update.side_effect = Exception("404 not_found")
        lf.upsert_hold_type_status_in_index(mock_opensearch, "orders", "OHK-MISSING", "HOLD_X", "1300")


# ===========================================================================
# OpenSearch — remove_hold_type_status_from_index
# ===========================================================================

class TestRemoveHoldTypeStatusFromIndex:
    def test_uses_remove_painless_script(self, mock_opensearch):
        lf.remove_hold_type_status_from_index(mock_opensearch, "orders", "OHK-001", "FRAUD_REVIEW")
        kwargs = mock_opensearch.update.call_args[1]
        assert "remove" in kwargs["body"]["script"]["source"]
        assert kwargs["body"]["script"]["params"]["holdType"] == "FRAUD_REVIEW"

    def test_404_logged_not_raised(self, mock_opensearch):
        mock_opensearch.update.side_effect = Exception("404 not_found")
        lf.remove_hold_type_status_from_index(mock_opensearch, "orders", "OHK-MISSING", "HOLD_X")


# ===========================================================================
# OpenSearch — replace_hold_type_statuses_in_index
# ===========================================================================

class TestReplaceHoldTypeStatusesInIndex:
    def test_uses_replace_script_with_statuses(self, mock_opensearch):
        statuses = {"FRAUD_REVIEW": "1300", "CREDIT_HOLD": "1200"}
        lf.replace_hold_type_statuses_in_index(mock_opensearch, "orders", "OHK-001", statuses)
        kwargs = mock_opensearch.update.call_args[1]
        assert "script" in kwargs["body"]
        assert kwargs["body"]["script"]["params"]["statuses"] == statuses

    def test_empty_statuses(self, mock_opensearch):
        lf.replace_hold_type_statuses_in_index(mock_opensearch, "orders", "OHK-001", {})
        kwargs = mock_opensearch.update.call_args[1]
        assert kwargs["body"]["script"]["params"]["statuses"] == {}

    def test_404_logged_not_raised(self, mock_opensearch):
        mock_opensearch.update.side_effect = Exception("404 not_found")
        lf.replace_hold_type_statuses_in_index(mock_opensearch, "orders", "OHK-MISSING", {"A": "1100"})


# ===========================================================================
# process_order_hold_type_event
# ===========================================================================

class TestProcessOrderHoldTypeEvent:
    def test_missing_order_header_key_returns_400(self, mock_opensearch, mock_cache_table):
        result = lf.process_order_hold_type_event("c", {"HOLD_TYPE": "HOLD_X"})
        assert result["statusCode"] == 400
        assert "ORDER_HEADER_KEY" in result["body"]

    def test_missing_hold_type_on_create_returns_400(self, mock_opensearch, mock_cache_table):
        result = lf.process_order_hold_type_event("c", {"ORDER_HEADER_KEY": "OHK-001"})
        assert result["statusCode"] == 400
        assert "HOLD_TYPE" in result["body"]

    def test_create_with_order_line_key_and_status(self, mock_opensearch, mock_cache_table):
        result = lf.process_order_hold_type_event("c", make_payload())
        assert result["statusCode"] == 200
        # DynamoDB: 2 upsert_hold_type_status_in_cache calls (order + order-line)
        assert mock_cache_table.update_item.call_count == 2
        # OpenSearch: 2 upsert_hold_type_status_in_index calls (order + order-line)
        assert mock_opensearch.update.call_count == 2

    def test_create_without_order_line_key(self, mock_opensearch, mock_cache_table):
        payload = make_payload(ORDER_LINE_KEY="")
        result = lf.process_order_hold_type_event("c", payload)
        assert result["statusCode"] == 200
        # DynamoDB: 1 upsert call (order only)
        assert mock_cache_table.update_item.call_count == 1
        # OpenSearch: 1 upsert call (order only)
        assert mock_opensearch.update.call_count == 1

    def test_create_with_null_status_stores_null(self, mock_opensearch, mock_cache_table):
        """When STATUS is null, the hold type should be stored with None status."""
        payload = make_payload(STATUS=None)
        result = lf.process_order_hold_type_event("c", payload)
        assert result["statusCode"] == 200
        # Both DynamoDB and OpenSearch should still be called (not skipped)
        assert mock_cache_table.update_item.call_count == 2
        assert mock_opensearch.update.call_count == 2

    def test_create_stores_status_as_string(self, mock_opensearch, mock_cache_table):
        """Status value should be converted to string before storage."""
        result = lf.process_order_hold_type_event("c", make_payload(STATUS="1300"))
        assert result["statusCode"] == 200
        # Verify DynamoDB received string status
        kwargs = mock_cache_table.update_item.call_args_list[0][1]
        assert kwargs["ExpressionAttributeValues"][":st"] == "1300"

    def test_delete_with_hold_type(self, mock_opensearch, mock_cache_table):
        # get_hold_type_statuses_from_cache returns remaining statuses after removal
        mock_cache_table.get_item.return_value = {
            "Item": {"holdTypeStatuses": {"CREDIT_HOLD": "1200"}}
        }
        result = lf.process_order_hold_type_event("d", make_payload())
        assert result["statusCode"] == 200
        # DynamoDB: 2 remove calls (order + order-line)
        assert mock_cache_table.update_item.call_count == 2
        # DynamoDB: 2 get_item calls (get remaining for order + order-line)
        assert mock_cache_table.get_item.call_count == 2
        # OpenSearch: 2 replace calls (sync remaining for order + order-line)
        assert mock_opensearch.update.call_count == 2

    def test_delete_without_hold_type_no_ops(self, mock_opensearch, mock_cache_table):
        """Delete with no hold_type (edge case) — nothing to remove."""
        result = lf.process_order_hold_type_event("d", {"ORDER_HEADER_KEY": "OHK-001"})
        assert result["statusCode"] == 200
        mock_cache_table.update_item.assert_not_called()
        mock_opensearch.update.assert_not_called()


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
        topic = "sterling.public.yfs_order_hold_type"
        def make_msg(ht):
            env = make_debezium_envelope(make_payload(HOLD_TYPE=ht))
            return {"topic": topic, "offset": 0,
                    "value": base64.b64encode(json.dumps(env).encode()).decode()}
        event = {
            "eventSource": "aws:kafka",
            "records": {f"{topic}-0": [make_msg("HOLD_A"), make_msg("HOLD_B")]}
        }
        result = lf.process_msk_event(event)
        assert "Processed 2" in result["body"]


# ===========================================================================
# lambda_handler
# ===========================================================================

class TestLambdaHandler:
    def test_msk_event_routes_correctly(self, mock_opensearch, mock_cache_table):
        event = make_msk_event(make_debezium_envelope(make_payload()))
        result = lf.lambda_handler(event, None)
        assert result["statusCode"] == 200

    def test_direct_invocation_routes_correctly(self, mock_opensearch, mock_cache_table):
        event = {"table": "YFS_ORDER_HOLD_TYPE", "operation": "c", "payload": make_payload()}
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
