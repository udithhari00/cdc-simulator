"""
Unit tests for cdc-lambda-customeraddnladdress/lambda_function.py

Run with:
    pytest test_lambda_function.py -v
"""

import base64
import json

from unittest.mock import patch, MagicMock
import pytest
import lambda_function as lf

# =============================================================================
# Sample data
# =============================================================================

SAMPLE_ADDNL_ADDR_KEY = "ADDNL_ADDR_KEY_001"

SAMPLE_ADDNL_ADDR_PAYLOAD = {
    "CUSTOMER_KEY":       "CKEY001",
    "ADDRESS_TYPE":       "BillTo",
    "PERSON_INFO_KEY":    "PIK001",
    "IS_DEFAULT_BILL_TO": "Y",
    "IS_DEFAULT_SHIP_TO": "N",
    "IS_DEFAULT_SOLD_TO": "N",
}

SAMPLE_ADDNL_ADDR_PAYLOAD_WITH_KEY = {
    **SAMPLE_ADDNL_ADDR_PAYLOAD,
    "CUSTOMER_ADDNL_ADDRESS_KEY": SAMPLE_ADDNL_ADDR_KEY,
}

SAMPLE_PERSON_INFO = {
    "firstName":    "John",
    "middleName":   "B",
    "lastName":     "Smith",
    "emailId":      "john@example.com",
    "dayPhone":     "555-0001",
    "mobilePhone":  "555-0002",
    "company":      "Acme Corp",
    "addressLine1": "123 Main St",
    "city":         "Chicago",
    "state":        "IL",
    "zipCode":      "60601",
    "country":      "US",
}


def _make_msk_event(debezium_event: dict, topic: str = "glenraven-dev.STERLING.yfs_customer_addnl_address", key: str = None) -> dict:
    encoded = base64.b64encode(json.dumps(debezium_event).encode()).decode()
    msg = {"value": encoded, "topic": topic, "offset": 1}
    if key is not None:
        msg["key"] = key
    return {"records": {"topic-0": [msg]}}


def _make_key_b64(addnl_address_key: str) -> str:
    """Encode a Debezium key payload the same way MSK delivers it."""
    key_data = {"schema": {}, "payload": {"CUSTOMER_ADDNL_ADDRESS_KEY": addnl_address_key}}
    return base64.b64encode(json.dumps(key_data).encode()).decode()


def _make_null_payload_msk_event(addnl_address_key: str, topic: str = "glenraven-dev.STERLING.yfs_customer_addnl_address") -> dict:
    """MSK event with null Debezium payload (delete with no before-image) and encoded key."""
    debezium_event = {"schema": {"type": "struct", "optional": True}, "payload": None}
    encoded = base64.b64encode(json.dumps(debezium_event).encode()).decode()
    return {
        "records": {
            "topic-0": [{
                "value": encoded,
                "topic": topic,
                "offset": 1,
                "key":   _make_key_b64(addnl_address_key),
            }]
        }
    }


# =============================================================================
# Utility helpers
# =============================================================================

class TestGetValue:
    def test_strips_whitespace(self):
        assert lf.get_value({"A": " test "}, "A") == "test"

    def test_missing_key_returns_none(self):
        assert lf.get_value({}, "A") is None

    def test_none_value_returns_none(self):
        assert lf.get_value({"A": None}, "A") is None

    def test_blank_string_returns_none(self):
        assert lf.get_value({"A": "   "}, "A") is None


class TestNowMs:
    def test_returns_int(self):
        assert isinstance(lf.now_ms(), int)

    def test_recent(self):
        assert lf.now_ms() > 1_700_000_000_000


# =============================================================================
# extract_table_from_topic
# =============================================================================

class TestExtractTableFromTopic:
    def test_yfs_customer_addnl_address(self):
        assert lf.extract_table_from_topic("glenraven-dev.STERLING.yfs_customer_addnl_address") == "YFS_CUSTOMER_ADDNL_ADDRESS"

    def test_unknown_uppercased(self):
        assert lf.extract_table_from_topic("db.schema.some_table") == "SOME_TABLE"


# =============================================================================
# process_customer_addnl_address
# =============================================================================

class TestProcessCustomerAddnlAddress:

    @patch("lambda_function.get_opensearch_client")
    @patch("lambda_function.get_cached_person_info")
    @patch("lambda_function.cache_table")
    def test_billto_default_processed(self, mock_table, mock_pi, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_pi.return_value = SAMPLE_PERSON_INFO
        mock_os.update_by_query.return_value = {"updated": 1}
        result = lf.process_customer_addnl_address(SAMPLE_ADDNL_ADDR_PAYLOAD, "c")
        assert result["statusCode"] == 200
        # No CUSTOMER_ADDNL_ADDRESS_KEY in payload → only main item written
        mock_table.put_item.assert_called_once()
        mock_os.update_by_query.assert_called_once()

    @patch("lambda_function.get_opensearch_client")
    @patch("lambda_function.get_cached_person_info")
    @patch("lambda_function.cache_table")
    def test_reverse_lookup_written_when_key_present(self, mock_table, mock_pi, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_pi.return_value = SAMPLE_PERSON_INFO
        mock_os.update_by_query.return_value = {"updated": 1}
        lf.process_customer_addnl_address(SAMPLE_ADDNL_ADDR_PAYLOAD_WITH_KEY, "c")
        assert mock_table.put_item.call_count == 2
        # First call: main item
        main_item = mock_table.put_item.call_args_list[0][1]["Item"]
        assert main_item["PK"] == lf.ENTITY_CUSTOMER_ADDNL_ADDRESS
        assert main_item["SK"] == "CKEY001#BillTo"
        # Second call: reverse lookup
        reverse_item = mock_table.put_item.call_args_list[1][1]["Item"]
        assert reverse_item["PK"] == lf.ENTITY_CUSTOMER_ADDNL_ADDRESS_KEY
        assert reverse_item["SK"] == SAMPLE_ADDNL_ADDR_KEY
        assert reverse_item["customerKey"] == "CKEY001"
        assert reverse_item["addressType"] == "BillTo"

    @patch("lambda_function.get_opensearch_client")
    @patch("lambda_function.cache_table")
    def test_delete_removes_both_cache_items(self, mock_table, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.return_value = {"updated": 1}
        with patch.dict("os.environ", {"ENABLE_HARD_DELETE": "true"}):
            lf.process_customer_addnl_address(SAMPLE_ADDNL_ADDR_PAYLOAD_WITH_KEY, "d")
        assert mock_table.delete_item.call_count == 2
        keys = [c[1]["Key"] for c in mock_table.delete_item.call_args_list]
        assert {"PK": lf.ENTITY_CUSTOMER_ADDNL_ADDRESS, "SK": "CKEY001#BillTo"} in keys
        assert {"PK": lf.ENTITY_CUSTOMER_ADDNL_ADDRESS_KEY, "SK": SAMPLE_ADDNL_ADDR_KEY} in keys

    @patch("lambda_function.get_opensearch_client")
    @patch("lambda_function.cache_table")
    def test_delete_without_addnl_key_removes_one_item(self, mock_table, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.return_value = {"updated": 1}
        with patch.dict("os.environ", {"ENABLE_HARD_DELETE": "true"}):
            lf.process_customer_addnl_address(SAMPLE_ADDNL_ADDR_PAYLOAD, "d")
        mock_table.delete_item.assert_called_once_with(
            Key={"PK": lf.ENTITY_CUSTOMER_ADDNL_ADDRESS, "SK": "CKEY001#BillTo"}
        )

    def test_non_billto_shipto_soldto_skipped(self):
        payload = {**SAMPLE_ADDNL_ADDR_PAYLOAD, "IS_DEFAULT_BILL_TO": "N"}
        result = lf.process_customer_addnl_address(payload, "c")
        assert result["statusCode"] == 200
        assert "Skipped" in result["body"]

    def test_non_default_billto_skipped(self):
        payload = {**SAMPLE_ADDNL_ADDR_PAYLOAD, "IS_DEFAULT_BILL_TO": "N"}
        result = lf.process_customer_addnl_address(payload, "c")
        assert result["statusCode"] == 200
        assert "Skipped" in result["body"]

    def test_missing_customer_key_returns_400(self):
        result = lf.process_customer_addnl_address({"ADDRESS_TYPE": "BillTo"}, "c")
        assert result["statusCode"] == 400

    @patch("lambda_function.get_cached_person_info", return_value=None)
    @patch("lambda_function.cache_table")
    def test_person_info_cache_miss_returns_200(self, mock_table, mock_pi):
        result = lf.process_customer_addnl_address(SAMPLE_ADDNL_ADDR_PAYLOAD, "c")
        assert result["statusCode"] == 200
        assert "roles=" in result["body"]

    @patch("lambda_function.cache_table")
    def test_no_person_info_key_returns_200(self, mock_table):
        payload = {**SAMPLE_ADDNL_ADDR_PAYLOAD, "PERSON_INFO_KEY": None}
        result = lf.process_customer_addnl_address(payload, "c")
        assert result["statusCode"] == 200
        assert "roles=" in result["body"]

    @patch("lambda_function.get_opensearch_client")
    @patch("lambda_function.get_cached_person_info")
    @patch("lambda_function.cache_table")
    def test_billto_address_field_name(self, mock_table, mock_pi, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_pi.return_value = SAMPLE_PERSON_INFO
        mock_os.update_by_query.return_value = {"updated": 1}
        lf.process_customer_addnl_address(SAMPLE_ADDNL_ADDR_PAYLOAD, "c")
        params = mock_os.update_by_query.call_args[1]["body"]["script"]["params"]
        assert params["addressField"] == "billTo"

    @patch("lambda_function.get_opensearch_client")
    @patch("lambda_function.get_cached_person_info")
    @patch("lambda_function.cache_table")
    def test_shipto_address_field_name(self, mock_table, mock_pi, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_pi.return_value = SAMPLE_PERSON_INFO
        mock_os.update_by_query.return_value = {"updated": 0}
        payload = {
            **SAMPLE_ADDNL_ADDR_PAYLOAD,
            "ADDRESS_TYPE": "ShipTo",
            "IS_DEFAULT_BILL_TO": "N",
            "IS_DEFAULT_SHIP_TO": "Y",
        }
        lf.process_customer_addnl_address(payload, "c")
        params = mock_os.update_by_query.call_args[1]["body"]["script"]["params"]
        assert params["addressField"] == "shipTo"

    @patch("lambda_function.get_opensearch_client")
    @patch("lambda_function.get_cached_person_info")
    @patch("lambda_function.cache_table")
    def test_soldto_address_field_name(self, mock_table, mock_pi, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_pi.return_value = SAMPLE_PERSON_INFO
        mock_os.update_by_query.return_value = {"updated": 0}
        payload = {
            **SAMPLE_ADDNL_ADDR_PAYLOAD,
            "ADDRESS_TYPE": "SoldTo",
            "IS_DEFAULT_BILL_TO": "N",
            "IS_DEFAULT_SOLD_TO": "Y",
        }
        lf.process_customer_addnl_address(payload, "c")
        params = mock_os.update_by_query.call_args[1]["body"]["script"]["params"]
        assert params["addressField"] == "soldTo"

    @patch("lambda_function.get_opensearch_client")
    @patch("lambda_function.get_cached_person_info")
    @patch("lambda_function.cache_table")
    def test_delete_sets_address_to_none(self, mock_table, mock_pi, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.return_value = {"updated": 1}
        lf.process_customer_addnl_address(SAMPLE_ADDNL_ADDR_PAYLOAD, "d")
        params = mock_os.update_by_query.call_args[1]["body"]["script"]["params"]
        assert params["address"] is None

    @patch("lambda_function.get_opensearch_client")
    @patch("lambda_function.get_cached_person_info")
    @patch("lambda_function.cache_table")
    def test_address_obj_contains_person_info_fields(self, mock_table, mock_pi, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_pi.return_value = SAMPLE_PERSON_INFO
        mock_os.update_by_query.return_value = {"updated": 1}
        lf.process_customer_addnl_address(SAMPLE_ADDNL_ADDR_PAYLOAD, "c")
        params = mock_os.update_by_query.call_args[1]["body"]["script"]["params"]
        addr = params["address"]
        assert addr["city"] == "Chicago"
        assert addr["state"] == "IL"
        assert addr["fullName"] == "John B Smith"

    @patch("lambda_function.get_opensearch_client")
    @patch("lambda_function.get_cached_person_info")
    @patch("lambda_function.cache_table")
    def test_fanout_failure_non_fatal(self, mock_table, mock_pi, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_pi.return_value = SAMPLE_PERSON_INFO
        mock_os.update_by_query.side_effect = Exception("OS down")
        result = lf.process_customer_addnl_address(SAMPLE_ADDNL_ADDR_PAYLOAD, "c")
        assert result["statusCode"] == 200

    @patch("lambda_function.get_opensearch_client")
    @patch("lambda_function.get_cached_person_info")
    @patch("lambda_function.cache_table")
    def test_query_uses_customer_key(self, mock_table, mock_pi, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_pi.return_value = SAMPLE_PERSON_INFO
        mock_os.update_by_query.return_value = {"updated": 1}
        lf.process_customer_addnl_address(SAMPLE_ADDNL_ADDR_PAYLOAD, "c")
        body = mock_os.update_by_query.call_args[1]["body"]
        assert body["query"]["term"]["customer.customerKey"] == "CKEY001"


# =============================================================================
# process_single_event routing
# =============================================================================

class TestProcessSingleEvent:

    @patch("lambda_function.process_customer_addnl_address")
    def test_routes_yfs_customer_addnl_address(self, mock_proc):
        mock_proc.return_value = {"statusCode": 200}
        lf.process_single_event("YFS_CUSTOMER_ADDNL_ADDRESS", "c", {})
        mock_proc.assert_called_once()

    def test_unknown_table_returns_400(self):
        result = lf.process_single_event("UNKNOWN", "c", {})
        assert result["statusCode"] == 400


# =============================================================================
# process_msk_event
# =============================================================================

class TestProcessMskEvent:

    @patch("lambda_function.process_single_event")
    def test_create_event_processed(self, mock_proc):
        mock_proc.return_value = {"statusCode": 200}
        event = _make_msk_event({"op": "c", "after": SAMPLE_ADDNL_ADDR_PAYLOAD, "ts_ms": 123})
        result = lf.process_msk_event(event)
        assert result["statusCode"] == 200
        mock_proc.assert_called_once()

    @patch("lambda_function.process_single_event")
    def test_delete_uses_before_image(self, mock_proc):
        mock_proc.return_value = {"statusCode": 200}
        before = {**SAMPLE_ADDNL_ADDR_PAYLOAD, "CUSTOMER_KEY": "DELKEY"}
        event = _make_msk_event({"op": "d", "before": before, "ts_ms": 1})
        lf.process_msk_event(event)
        _, _, payload, _ = mock_proc.call_args[0]
        assert payload.get("CUSTOMER_KEY") == "DELKEY"

    @patch("lambda_function.process_single_event")
    def test_payload_keys_uppercased(self, mock_proc):
        mock_proc.return_value = {"statusCode": 200}
        event = _make_msk_event({"op": "c", "after": {"customer_key": "K1"}, "ts_ms": 1})
        lf.process_msk_event(event)
        _, _, payload, _ = mock_proc.call_args[0]
        assert "CUSTOMER_KEY" in payload

    @patch("lambda_function.process_single_event")
    def test_bad_base64_returns_207(self, mock_proc):
        event = {
            "records": {
                "topic-0": [{"value": "NOT_VALID!!!", "topic": "glenraven-dev.STERLING.yfs_customer_addnl_address", "offset": 1}]
            }
        }
        result = lf.process_msk_event(event)
        assert result["statusCode"] == 207

    @patch("lambda_function.process_single_event")
    def test_empty_records_returns_200(self, mock_proc):
        result = lf.process_msk_event({"records": {}})
        assert result["statusCode"] == 200
        mock_proc.assert_not_called()

    def test_msk_level_tombstone_skipped(self):
        """Kafka-level null value (true tombstone) is skipped without error."""
        event = {"records": {"topic-0": [{"value": None, "topic": "glenraven-dev.STERLING.yfs_customer_addnl_address", "offset": 1}]}}
        result = lf.process_msk_event(event)
        assert result["statusCode"] == 200

    @patch("lambda_function.process_delete_by_key")
    def test_null_payload_triggers_delete_by_key(self, mock_del):
        """Debezium null-payload message routes to process_delete_by_key, not process_single_event."""
        mock_del.return_value = {"statusCode": 200, "body": "ok"}
        event = _make_null_payload_msk_event(SAMPLE_ADDNL_ADDR_KEY)
        result = lf.process_msk_event(event)
        assert result["statusCode"] == 200
        mock_del.assert_called_once()

    @patch("lambda_function.process_delete_by_key")
    def test_null_payload_delete_failure_returns_207(self, mock_del):
        mock_del.return_value = {"statusCode": 500, "body": "error"}
        event = _make_null_payload_msk_event(SAMPLE_ADDNL_ADDR_KEY)
        result = lf.process_msk_event(event)
        assert result["statusCode"] == 207


# =============================================================================
# lambda_handler entry point
# =============================================================================

class TestLambdaHandler:

    @patch("lambda_function.process_single_event")
    def test_direct_invocation(self, mock_proc):
        mock_proc.return_value = {"statusCode": 200}
        result = lf.lambda_handler({"table": "YFS_CUSTOMER_ADDNL_ADDRESS", "operation": "c", "payload": SAMPLE_ADDNL_ADDR_PAYLOAD}, None)
        assert result["statusCode"] == 200

    @patch("lambda_function.process_msk_event")
    def test_msk_trigger(self, mock_msk):
        mock_msk.return_value = {"statusCode": 200}
        lf.lambda_handler({"records": {}}, None)
        mock_msk.assert_called_once()

    @patch("lambda_function.process_single_event")
    def test_unhandled_exception_returns_500(self, mock_proc):
        mock_proc.side_effect = RuntimeError("Boom")
        result = lf.lambda_handler({"table": "YFS_CUSTOMER_ADDNL_ADDRESS", "operation": "c", "payload": {}}, None)
        assert result["statusCode"] == 500
        assert "Boom" in result["body"]


# =============================================================================
# Real CDC event — from Kafka topic (flattened SMT format)
# =============================================================================

REAL_CUSTOMER_ADDNL_ADDRESS_CDC_EVENT = {
    "schema": {
        "type": "struct",
        "optional": False,
        "name": "glenraven-dev.STERLING.YFS_CUSTOMER_ADDNL_ADDRESS.Value",
        "fields": []
    },
    "payload": {
        "CUSTOMER_ADDNL_ADDRESS_KEY": "202206211106454046067294",
        "CUSTOMER_KEY": "2013022010080477482176  ",
        "CUSTOMER_ADDNL_ADDRESS_ID": "101216072               ",
        "ADDRESS_TYPE": "                        ",
        "PERSON_INFO_KEY": "202206211106454046067293",
        "IS_SHIP_TO": "Y",
        "IS_BILL_TO": "Y",
        "IS_SOLD_TO": "Y",
        "IS_DEFAULT_SHIP_TO": None,
        "IS_DEFAULT_BILL_TO": None,
        "IS_DEFAULT_SOLD_TO": None,
        "CUSTOMER_CONTACT_KEY": "2013022010080477482182  ",
        "LOCKID": 0.0,
        "CREATETS": 1655809604000,
        "MODIFYTS": 1655809604000,
        "CREATEUSERID": "newsk@dist.us.glenraven.com",
        "MODIFYUSERID": "newsk@dist.us.glenraven.com",
        "CREATEPROGID": "ISCCSSYS001",
        "MODIFYPROGID": "ISCCSSYS001",
        "__op": "r",
        "__table": "YFS_CUSTOMER_ADDNL_ADDRESS",
        "__source_ts_ms": 1773211028000
    }
}

# Variant with BillTo set as default — tests the full processing path
REAL_CUSTOMER_ADDNL_ADDRESS_CDC_EVENT_BILLTO = {
    **REAL_CUSTOMER_ADDNL_ADDRESS_CDC_EVENT,
    "payload": {
        **REAL_CUSTOMER_ADDNL_ADDRESS_CDC_EVENT["payload"],
        "ADDRESS_TYPE": "BillTo                  ",
        "IS_DEFAULT_BILL_TO": "Y",
        "CUSTOMER_CONTACT_KEY": None,
    }
}


def _make_msk_event_from_raw(cdc_event: dict,
                              topic: str = "glenraven-dev.STERLING.yfs_customer_addnl_address") -> dict:
    encoded = base64.b64encode(json.dumps(cdc_event).encode()).decode()
    return {
        "eventSource": "aws:kafka",
        "records": {
            f"{topic}-0": [{"topic": topic, "offset": 0, "value": encoded}]
        }
    }


class TestRealCustomerAddnlAddressCdcEvent:
    """End-to-end tests using actual Kafka CDC data — YFS_CUSTOMER_ADDNL_ADDRESS."""

    @patch("lambda_function.process_single_event")
    def test_blank_address_type_skipped(self, mock_proc):
        """ADDRESS_TYPE is all spaces in real data — should be skipped, not an error."""
        mock_proc.return_value = {"statusCode": 200, "body": "Skipped address_type=None"}
        event = _make_msk_event_from_raw(REAL_CUSTOMER_ADDNL_ADDRESS_CDC_EVENT)
        result = lf.lambda_handler(event, None)
        assert result["statusCode"] == 200

    def test_blank_address_type_skipped_directly(self):
        """Direct process call: blank ADDRESS_TYPE resolves to None → skipped."""
        payload = {k: v for k, v in REAL_CUSTOMER_ADDNL_ADDRESS_CDC_EVENT["payload"].items()
                   if not k.startswith("__")}
        # Strip values as the MSK path does
        payload = {k: v.strip() if isinstance(v, str) else v for k, v in payload.items()}
        result = lf.process_customer_addnl_address(payload, "r")
        assert result["statusCode"] == 200
        assert "Skipped" in result["body"]

    def test_null_default_flags_treated_as_non_default(self):
        """IS_DEFAULT_BILL_TO=null means non-default — even if ADDRESS_TYPE is set."""
        payload = {
            "CUSTOMER_KEY": "2013022010080477482176",
            "ADDRESS_TYPE": "BillTo",
            "PERSON_INFO_KEY": "202206211106454046067293",
            "IS_DEFAULT_BILL_TO": None,
            "IS_DEFAULT_SHIP_TO": None,
            "IS_DEFAULT_SOLD_TO": None,
        }
        result = lf.process_customer_addnl_address(payload, "r")
        assert result["statusCode"] == 200
        assert "non-default" in result["body"]

    @patch("lambda_function.get_opensearch_client")
    @patch("lambda_function.get_cached_person_info")
    @patch("lambda_function.cache_table")
    def test_billto_variant_processes_successfully(self, mock_table, mock_pi, mock_client):
        """When ADDRESS_TYPE=BillTo and IS_DEFAULT_BILL_TO=Y, full processing runs."""
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_pi.return_value = SAMPLE_PERSON_INFO
        mock_os.update_by_query.return_value = {"updated": 1}
        event = _make_msk_event_from_raw(REAL_CUSTOMER_ADDNL_ADDRESS_CDC_EVENT_BILLTO)
        result = lf.lambda_handler(event, None)
        assert result["statusCode"] == 200

    @patch("lambda_function.get_opensearch_client")
    @patch("lambda_function.get_cached_person_info")
    @patch("lambda_function.cache_table")
    def test_billto_variant_customer_key_stripped(self, mock_table, mock_pi, mock_client):
        """CUSTOMER_KEY trailing spaces are stripped before use."""
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_pi.return_value = SAMPLE_PERSON_INFO
        mock_os.update_by_query.return_value = {"updated": 1}
        event = _make_msk_event_from_raw(REAL_CUSTOMER_ADDNL_ADDRESS_CDC_EVENT_BILLTO)
        lf.lambda_handler(event, None)
        body = mock_os.update_by_query.call_args[1]["body"]
        assert body["query"]["term"]["customer.customerKey"] == "2013022010080477482176"

    @patch("lambda_function.get_opensearch_client")
    @patch("lambda_function.get_cached_person_info")
    @patch("lambda_function.cache_table")
    def test_billto_variant_dynamo_sk_uses_stripped_values(self, mock_table, mock_pi, mock_client):
        """DynamoDB SK is customer_key#address_type — both must be trimmed."""
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_pi.return_value = SAMPLE_PERSON_INFO
        mock_os.update_by_query.return_value = {"updated": 1}
        event = _make_msk_event_from_raw(REAL_CUSTOMER_ADDNL_ADDRESS_CDC_EVENT_BILLTO)
        lf.lambda_handler(event, None)
        # call_args_list[0] = main item, [1] = reverse lookup
        main_item = mock_table.put_item.call_args_list[0][1]["Item"]
        assert main_item["SK"] == "2013022010080477482176#BillTo"

    @patch("lambda_function.get_opensearch_client")
    @patch("lambda_function.get_cached_person_info")
    @patch("lambda_function.cache_table")
    def test_billto_variant_person_info_key_stored(self, mock_table, mock_pi, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_pi.return_value = SAMPLE_PERSON_INFO
        mock_os.update_by_query.return_value = {"updated": 1}
        event = _make_msk_event_from_raw(REAL_CUSTOMER_ADDNL_ADDRESS_CDC_EVENT_BILLTO)
        lf.lambda_handler(event, None)
        main_item = mock_table.put_item.call_args_list[0][1]["Item"]
        assert main_item["personInfoKey"] == "202206211106454046067293"

    @patch("lambda_function.get_opensearch_client")
    @patch("lambda_function.get_cached_person_info")
    @patch("lambda_function.cache_table")
    def test_billto_variant_reverse_lookup_written(self, mock_table, mock_pi, mock_client):
        """Real event has CUSTOMER_ADDNL_ADDRESS_KEY — reverse lookup item should be written."""
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_pi.return_value = SAMPLE_PERSON_INFO
        mock_os.update_by_query.return_value = {"updated": 1}
        event = _make_msk_event_from_raw(REAL_CUSTOMER_ADDNL_ADDRESS_CDC_EVENT_BILLTO)
        lf.lambda_handler(event, None)
        assert mock_table.put_item.call_count == 2
        reverse_item = mock_table.put_item.call_args_list[1][1]["Item"]
        assert reverse_item["PK"] == lf.ENTITY_CUSTOMER_ADDNL_ADDRESS_KEY
        assert reverse_item["SK"] == "202206211106454046067294"
        assert reverse_item["customerKey"] == "2013022010080477482176"
        assert reverse_item["addressType"] == "BillTo"

    @patch("lambda_function.process_single_event")
    def test_dunder_keys_not_passed_to_processor(self, mock_proc):
        """__op, __table, __source_ts_ms must be stripped before process_single_event."""
        mock_proc.return_value = {"statusCode": 200}
        event = _make_msk_event_from_raw(REAL_CUSTOMER_ADDNL_ADDRESS_CDC_EVENT)
        lf.lambda_handler(event, None)
        _, _, payload, _ = mock_proc.call_args[0]
        assert not any(k.startswith("__") for k in payload)

    @patch("lambda_function.process_single_event")
    def test_source_timestamp_passed_correctly(self, mock_proc):
        mock_proc.return_value = {"statusCode": 200}
        event = _make_msk_event_from_raw(REAL_CUSTOMER_ADDNL_ADDRESS_CDC_EVENT)
        lf.lambda_handler(event, None)
        _, _, _, source_ts = mock_proc.call_args[0]
        assert source_ts == 1773211028000


# =============================================================================
# get_opensearch_client — auth branches
# =============================================================================

class TestGetOpensearchClient:

    def setup_method(self):
        lf.opensearch_client = None

    @patch("lambda_function.OpenSearch")
    @patch("lambda_function.AWSV4SignerAuth")
    @patch("lambda_function.boto3.Session")
    @patch("lambda_function.OPENSEARCH_AUTH", "iam")
    @patch("lambda_function.OPENSEARCH_MODE", "provisioned")
    def test_iam_provisioned_uses_es_service(self, mock_session, mock_auth, mock_os):
        mock_session.return_value.get_credentials.return_value = MagicMock()
        mock_auth.return_value = MagicMock()
        mock_os.return_value = MagicMock()
        lf.get_opensearch_client()
        mock_auth.assert_called_once_with(mock_session.return_value.get_credentials.return_value, lf.AWS_REGION, 'es')

    @patch("lambda_function.OpenSearch")
    @patch("lambda_function.AWSV4SignerAuth")
    @patch("lambda_function.boto3.Session")
    @patch("lambda_function.OPENSEARCH_AUTH", "iam")
    @patch("lambda_function.OPENSEARCH_MODE", "serverless")
    def test_iam_serverless_uses_aoss_service(self, mock_session, mock_auth, mock_os):
        mock_session.return_value.get_credentials.return_value = MagicMock()
        mock_auth.return_value = MagicMock()
        mock_os.return_value = MagicMock()
        lf.get_opensearch_client()
        mock_auth.assert_called_once_with(mock_session.return_value.get_credentials.return_value, lf.AWS_REGION, 'aoss')

    @patch("lambda_function.OpenSearch")
    @patch("lambda_function.OPENSEARCH_AUTH", "basic")
    def test_basic_auth(self, mock_os):
        mock_os.return_value = MagicMock()
        client = lf.get_opensearch_client()
        assert client is not None

    @patch("lambda_function.OpenSearch")
    @patch("lambda_function.OPENSEARCH_AUTH", "basic")
    def test_singleton(self, mock_os):
        mock_os.return_value = MagicMock()
        c1 = lf.get_opensearch_client()
        c2 = lf.get_opensearch_client()
        assert c1 is c2
        assert mock_os.call_count == 1

    @patch("lambda_function.boto3.Session")
    @patch("lambda_function.OPENSEARCH_AUTH", "iam")
    def test_no_credentials_raises(self, mock_session):
        mock_session.return_value.get_credentials.return_value = None
        with pytest.raises(RuntimeError, match="AWS credentials not found"):
            lf.get_opensearch_client()


# =============================================================================
# get_cached_addnl_address_by_addnl_key
# =============================================================================

class TestGetCachedAddnlAddressByAddnlKey:

    @patch("lambda_function.cache_table")
    def test_returns_item_without_metadata(self, mock_table):
        mock_table.get_item.return_value = {"Item": {
            "PK": lf.ENTITY_CUSTOMER_ADDNL_ADDRESS_KEY,
            "SK": "KEY001",
            "customerKey": "CKEY001",
            "addressType": "BillTo",
            "updatedAt": "2024-01-01",
        }}
        result = lf.get_cached_addnl_address_by_addnl_key("KEY001")
        assert result == {"customerKey": "CKEY001", "addressType": "BillTo"}

    @patch("lambda_function.cache_table")
    def test_returns_none_when_missing(self, mock_table):
        mock_table.get_item.return_value = {}
        assert lf.get_cached_addnl_address_by_addnl_key("MISSING") is None

    @patch("lambda_function.cache_table")
    def test_returns_none_on_error(self, mock_table):
        mock_table.get_item.side_effect = Exception("DynamoDB error")
        assert lf.get_cached_addnl_address_by_addnl_key("KEY001") is None


# =============================================================================
# process_delete_by_key
# =============================================================================

SAMPLE_REVERSE_LOOKUP = {"customerKey": "CKEY001", "addressType": "BillTo"}


class TestProcessDeleteByKey:

    @patch("lambda_function.get_opensearch_client")
    @patch("lambda_function.get_cached_addnl_address_by_addnl_key")
    @patch("lambda_function.cache_table")
    def test_valid_key_deletes_both_items_and_fans_out(self, mock_table, mock_lookup, mock_client):
        mock_lookup.return_value = SAMPLE_REVERSE_LOOKUP
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.return_value = {"updated": 1}

        with patch.dict("os.environ", {"ENABLE_HARD_DELETE": "true"}):
            result = lf.process_delete_by_key("YFS_CUSTOMER_ADDNL_ADDRESS", _make_key_b64(SAMPLE_ADDNL_ADDR_KEY))

        assert result["statusCode"] == 200
        assert mock_table.delete_item.call_count == 2
        keys = [c[1]["Key"] for c in mock_table.delete_item.call_args_list]
        assert {"PK": lf.ENTITY_CUSTOMER_ADDNL_ADDRESS, "SK": "CKEY001#BillTo"} in keys
        assert {"PK": lf.ENTITY_CUSTOMER_ADDNL_ADDRESS_KEY, "SK": SAMPLE_ADDNL_ADDR_KEY} in keys
        mock_os.update_by_query.assert_called_once()

    @patch("lambda_function.get_opensearch_client")
    @patch("lambda_function.get_cached_addnl_address_by_addnl_key")
    @patch("lambda_function.cache_table")
    def test_opensearch_query_nulls_correct_field(self, mock_table, mock_lookup, mock_client):
        mock_lookup.return_value = SAMPLE_REVERSE_LOOKUP
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.return_value = {"updated": 1}

        lf.process_delete_by_key("YFS_CUSTOMER_ADDNL_ADDRESS", _make_key_b64(SAMPLE_ADDNL_ADDR_KEY))

        body = mock_os.update_by_query.call_args[1]["body"]
        assert body["query"]["term"]["customer.customerKey"] == "CKEY001"
        assert body["script"]["params"]["addressField"] == "billTo"

    @patch("lambda_function.get_cached_addnl_address_by_addnl_key")
    def test_no_key_returns_200_skipped(self, mock_lookup):
        result = lf.process_delete_by_key("YFS_CUSTOMER_ADDNL_ADDRESS", None)
        assert result["statusCode"] == 200
        assert "no key" in result["body"]
        mock_lookup.assert_not_called()

    @patch("lambda_function.get_cached_addnl_address_by_addnl_key")
    def test_invalid_key_base64_returns_200_skipped(self, mock_lookup):
        result = lf.process_delete_by_key("YFS_CUSTOMER_ADDNL_ADDRESS", "NOT_VALID!!!")
        assert result["statusCode"] == 200
        assert "key decode failed" in result["body"]
        mock_lookup.assert_not_called()

    @patch("lambda_function.get_cached_addnl_address_by_addnl_key", return_value=None)
    def test_reverse_lookup_miss_returns_200_skipped(self, mock_lookup):
        result = lf.process_delete_by_key("YFS_CUSTOMER_ADDNL_ADDRESS", _make_key_b64(SAMPLE_ADDNL_ADDR_KEY))
        assert result["statusCode"] == 200
        assert "reverse lookup missing" in result["body"]

    @patch("lambda_function.get_opensearch_client")
    @patch("lambda_function.get_cached_addnl_address_by_addnl_key")
    @patch("lambda_function.cache_table")
    def test_opensearch_failure_non_fatal(self, mock_table, mock_lookup, mock_client):
        mock_lookup.return_value = SAMPLE_REVERSE_LOOKUP
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.side_effect = Exception("OS down")
        result = lf.process_delete_by_key("YFS_CUSTOMER_ADDNL_ADDRESS", _make_key_b64(SAMPLE_ADDNL_ADDR_KEY))
        assert result["statusCode"] == 200

    @patch("lambda_function.get_opensearch_client")
    @patch("lambda_function.get_cached_addnl_address_by_addnl_key")
    @patch("lambda_function.cache_table")
    def test_shipto_address_field(self, mock_table, mock_lookup, mock_client):
        mock_lookup.return_value = {"customerKey": "CKEY001", "addressType": "ShipTo"}
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.return_value = {"updated": 1}
        lf.process_delete_by_key("YFS_CUSTOMER_ADDNL_ADDRESS", _make_key_b64(SAMPLE_ADDNL_ADDR_KEY))
        params = mock_os.update_by_query.call_args[1]["body"]["script"]["params"]
        assert params["addressField"] == "shipTo"

    @patch("lambda_function.get_opensearch_client")
    @patch("lambda_function.get_cached_addnl_address_by_addnl_key")
    @patch("lambda_function.cache_table")
    def test_unknown_address_type_skips_opensearch(self, mock_table, mock_lookup, mock_client):
        mock_lookup.return_value = {"customerKey": "CKEY001", "addressType": "Unknown"}
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        result = lf.process_delete_by_key("YFS_CUSTOMER_ADDNL_ADDRESS", _make_key_b64(SAMPLE_ADDNL_ADDR_KEY))
        assert result["statusCode"] == 200
        mock_os.update_by_query.assert_not_called()


# =============================================================================
# Soft Delete — cdc-customeraddnladdress-processor
# =============================================================================

class TestSoftDeleteCustomerAddnlAddress:
    """
    Tests for the ENABLE_HARD_DELETE toggle on the customeraddnladdress lambda.

    Soft delete (default, ENABLE_HARD_DELETE=false):
      - Both CUSTOMER_ADDNL_ADDRESS and CUSTOMER_ADDNL_ADDRESS_KEY DynamoDB rows
        are marked with _deleted=True via update_item
      - delete_item is NOT called

    Hard delete (ENABLE_HARD_DELETE=true):
      - delete_item is called for both rows (existing behavior)
      - update_item is NOT called for the delete path
    """

    # ------------------------------------------------------------------
    # Soft delete (default)
    # ------------------------------------------------------------------

    @patch("lambda_function.get_opensearch_client")
    @patch("lambda_function.cache_table")
    def test_soft_delete_calls_update_item_not_delete_item(self, mock_table, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.return_value = {"updated": 0}
        with patch.dict("os.environ", {"ENABLE_HARD_DELETE": "false"}):
            result = lf.process_customer_addnl_address(SAMPLE_ADDNL_ADDR_PAYLOAD_WITH_KEY, "d")
        assert result["statusCode"] == 200
        assert mock_table.update_item.call_count >= 1
        mock_table.delete_item.assert_not_called()

    @patch("lambda_function.get_opensearch_client")
    @patch("lambda_function.cache_table")
    def test_soft_delete_marks_main_cache_row(self, mock_table, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.return_value = {"updated": 0}
        with patch.dict("os.environ", {"ENABLE_HARD_DELETE": "false"}):
            lf.process_customer_addnl_address(SAMPLE_ADDNL_ADDR_PAYLOAD_WITH_KEY, "d")
        pks = [c[1]["Key"]["PK"] for c in mock_table.update_item.call_args_list]
        assert lf.ENTITY_CUSTOMER_ADDNL_ADDRESS in pks

    @patch("lambda_function.get_opensearch_client")
    @patch("lambda_function.cache_table")
    def test_soft_delete_sets_deleted_true_on_rows(self, mock_table, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.return_value = {"updated": 0}
        with patch.dict("os.environ", {"ENABLE_HARD_DELETE": "false"}):
            lf.process_customer_addnl_address(SAMPLE_ADDNL_ADDR_PAYLOAD_WITH_KEY, "d")
        for call in mock_table.update_item.call_args_list:
            expr_vals = call[1]["ExpressionAttributeValues"]
            assert any(v is True for v in expr_vals.values()), \
                f"Expected _deleted=True in ExpressionAttributeValues: {expr_vals}"

    # ------------------------------------------------------------------
    # Hard delete
    # ------------------------------------------------------------------

    @patch("lambda_function.get_opensearch_client")
    @patch("lambda_function.cache_table")
    def test_hard_delete_calls_delete_item_not_update_item(self, mock_table, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.return_value = {"updated": 0}
        with patch.dict("os.environ", {"ENABLE_HARD_DELETE": "true"}):
            result = lf.process_customer_addnl_address(SAMPLE_ADDNL_ADDR_PAYLOAD_WITH_KEY, "d")
        assert result["statusCode"] == 200
        assert mock_table.delete_item.call_count >= 1
        mock_table.update_item.assert_not_called()

    @patch("lambda_function.get_opensearch_client")
    @patch("lambda_function.cache_table")
    def test_hard_delete_removes_main_cache_row(self, mock_table, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.return_value = {"updated": 0}
        with patch.dict("os.environ", {"ENABLE_HARD_DELETE": "true"}):
            lf.process_customer_addnl_address(SAMPLE_ADDNL_ADDR_PAYLOAD_WITH_KEY, "d")
        keys_deleted = [c[1]["Key"] for c in mock_table.delete_item.call_args_list]
        assert {"PK": lf.ENTITY_CUSTOMER_ADDNL_ADDRESS, "SK": "CKEY001#BillTo"} in keys_deleted


# =============================================================================
# Edge Case & Deep Dive Tests
# =============================================================================

class TestCustomerAddnlAddressDeepDive:

    @patch("lambda_function.get_opensearch_client")
    @patch("lambda_function.get_cached_person_info")
    @patch("lambda_function.cache_table")
    def test_multi_role_atomic_update(self, mock_table, mock_pi, mock_client):
        """A row satisfying both BillTo and SoldTo must update both fields in OpenSearch in a single call."""
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_pi.return_value = SAMPLE_PERSON_INFO
        mock_os.update_by_query.return_value = {"updated": 1}

        payload = {
            "CUSTOMER_KEY": "CUST_MULTI_001",
            "CUSTOMER_ADDNL_ADDRESS_KEY": "ADDR_KEY_MULTI",
            "PERSON_INFO_KEY": "PIK001",
            "IS_BILL_TO": "Y",
            "IS_DEFAULT_BILL_TO": "Y",
            "IS_SOLD_TO": "Y",
            "IS_DEFAULT_SOLD_TO": "Y",
            "IS_SHIP_TO": "N",
            "IS_DEFAULT_SHIP_TO": "N",
        }
        result = lf.process_customer_addnl_address(payload, "c")
        assert result["statusCode"] == 200

        # DynamoDB writes: 2 main role items (BillTo, SoldTo) + 1 reverse item
        assert mock_table.put_item.call_count == 3
        sks = [c[1]["Item"]["SK"] for c in mock_table.put_item.call_args_list]
        assert "CUST_MULTI_001#BillTo" in sks
        assert "CUST_MULTI_001#SoldTo" in sks
        assert "ADDR_KEY_MULTI" in sks

        # OpenSearch: single atomic update_by_query call for both fields
        mock_os.update_by_query.assert_called_once()
        params = mock_os.update_by_query.call_args[1]["body"]["script"]["params"]
        assert set(params["addressFields"]) == {"billTo", "soldTo"}

    @patch("lambda_function.get_opensearch_client")
    @patch("lambda_function.get_cached_addnl_address_by_addnl_key")
    @patch("lambda_function.cache_table")
    def test_delete_resolves_customer_key_from_reverse_lookup(self, mock_table, mock_lookup, mock_client):
        """If customer_key is missing from delete payload, resolves it from cached reverse lookup."""
        mock_lookup.return_value = {
            "customerKey": "CUST_RESOLVED_999",
            "roles": ["ShipTo"],
        }
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.return_value = {"updated": 1}

        payload = {
            "CUSTOMER_ADDNL_ADDRESS_KEY": "ADDR_KEY_TOMBSTONE",
        }
        with patch.dict("os.environ", {"ENABLE_HARD_DELETE": "true"}):
            result = lf.process_customer_addnl_address(payload, "d")

        assert result["statusCode"] == 200
        # Verified it deleted the resolved customer's sort key
        keys = [c[1]["Key"] for c in mock_table.delete_item.call_args_list]
        assert {"PK": lf.ENTITY_CUSTOMER_ADDNL_ADDRESS, "SK": "CUST_RESOLVED_999#ShipTo"} in keys
        assert {"PK": lf.ENTITY_CUSTOMER_ADDNL_ADDRESS_KEY, "SK": "ADDR_KEY_TOMBSTONE"} in keys

    @patch("lambda_function.get_cached_addnl_address_by_addnl_key")
    @patch("lambda_function.cache_table")
    def test_fast_skip_on_snapshot_read_avoids_dynamo_read(self, mock_table, mock_lookup):
        """During snapshot read ('r'), non-default addresses must be skipped without any DynamoDB reads."""
        payload = {
            "CUSTOMER_KEY": "CUST_001",
            "CUSTOMER_ADDNL_ADDRESS_KEY": "ADDR_KEY_NON_DEFAULT",
            "IS_DEFAULT_BILL_TO": "N",
            "IS_DEFAULT_SHIP_TO": "N",
            "IS_DEFAULT_SOLD_TO": "N",
        }
        result = lf.process_customer_addnl_address(payload, "r")
        assert result["statusCode"] == 200
        assert "Skipped" in result["body"]
        mock_lookup.assert_not_called()
        mock_table.get_item.assert_not_called()
        mock_table.put_item.assert_not_called()

    @patch("lambda_function.get_opensearch_client")
    @patch("lambda_function.get_cached_addnl_address_by_addnl_key")
    @patch("lambda_function.cache_table")
    def test_live_update_coming_as_operation_c_revokes_role(self, mock_table, mock_lookup, mock_client):
        """When an update arrives as operation 'c' with default flags removed, revokes the old role."""
        mock_lookup.return_value = {
            "customerKey": "CUST_REVOKE_001",
            "roles": ["BillTo"],
        }
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.return_value = {"updated": 1}

        payload = {
            "CUSTOMER_KEY": "CUST_REVOKE_001",
            "CUSTOMER_ADDNL_ADDRESS_KEY": "ADDR_KEY_WAS_BILLTO",
            "IS_DEFAULT_BILL_TO": "N",
            "IS_DEFAULT_SHIP_TO": "N",
            "IS_DEFAULT_SOLD_TO": "N",
        }
        with patch.dict("os.environ", {"ENABLE_HARD_DELETE": "true"}):
            result = lf.process_customer_addnl_address(payload, "c")

        assert result["statusCode"] == 200
        # Verified it deleted the old role item and reverse item from DynamoDB
        keys = [c[1]["Key"] for c in mock_table.delete_item.call_args_list]
        assert {"PK": lf.ENTITY_CUSTOMER_ADDNL_ADDRESS, "SK": "CUST_REVOKE_001#BillTo"} in keys
        assert {"PK": lf.ENTITY_CUSTOMER_ADDNL_ADDRESS_KEY, "SK": "ADDR_KEY_WAS_BILLTO"} in keys

        # Verified it cleared billTo in OpenSearch
        mock_os.update_by_query.assert_called_once()
        params = mock_os.update_by_query.call_args[1]["body"]["script"]["params"]
        assert params["addressFields"] == ["billTo"]


