"""
Unit tests for cdc-lambda-customercontact/lambda_function.py

Run with:
    pytest test_lambda_function.py -v
"""

import base64
import json

from unittest.mock import patch, MagicMock, call
import pytest
import lambda_function as lf

# =============================================================================
# Sample data
# =============================================================================

SAMPLE_CONTACT_PAYLOAD = {
    "CUSTOMER_KEY":          "CKEY001",
    "CUSTOMER_CONTACT_KEY":  "CCKEY001",
    "FIRST_NAME":            "Jane",
    "MIDDLE_NAME":           "A",
    "LAST_NAME":             "Doe",
    "EMAILID":               "jane@example.com",
    "MOBILE_PHONE":          "555-1234",
    "JOB_TITLE":             "Buyer",
    "DEPARTMENT":            "Procurement",
    "EXTN_CONTACT_TYPE":     "PRIMARY",
    "COMPANY":               "Acme Corp",
    "DOB":                   "1990-01-15",
}

SAMPLE_CUSTOMER_ITEM = {
    "PK": "CUSTOMER",
    "SK": "CKEY001",
    "customerKey": "CKEY001",
    "customerId": "CUST001",
    "buyerOrganizationCode": "ORG001",
}


def _make_msk_event(debezium_event: dict, topic: str = "glenraven-dev.STERLING.yfs_customer_contact") -> dict:
    encoded = base64.b64encode(json.dumps(debezium_event).encode()).decode()
    return {
        "records": {
            "topic-0": [{"value": encoded, "topic": topic, "offset": 1}]
        }
    }


def _mock_get_item_for_customer(customer_item=None):
    """Helper: returns a side_effect for cache_table.get_item that returns a CUSTOMER record."""
    def side_effect(Key=None):
        if Key and Key.get('PK') == 'CUSTOMER':
            return {'Item': customer_item} if customer_item else {}
        return {}
    return side_effect


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

    def test_integer_cast_to_string(self):
        assert lf.get_value({"A": 42}, "A") == "42"


class TestNowMs:
    def test_returns_int(self):
        assert isinstance(lf.now_ms(), int)

    def test_recent(self):
        assert lf.now_ms() > 1_700_000_000_000


class TestComputeCustomerContactHash:
    def test_stable_output(self):
        obj = {'contactName': 'Jane', 'contactEmail': 'j@e.com'}
        assert lf.compute_customer_contact_hash(obj) == lf.compute_customer_contact_hash(obj)

    def test_different_data_different_hash(self):
        obj1 = {'contactName': 'Jane'}
        obj2 = {'contactName': 'John'}
        assert lf.compute_customer_contact_hash(obj1) != lf.compute_customer_contact_hash(obj2)

    def test_returns_16_char_hex(self):
        h = lf.compute_customer_contact_hash({'contactName': 'Test'})
        assert len(h) == 16
        assert all(c in '0123456789abcdef' for c in h)

    def test_none_input(self):
        h = lf.compute_customer_contact_hash(None)
        assert isinstance(h, str) and len(h) == 16


# =============================================================================
# extract_table_from_topic
# =============================================================================

class TestExtractTableFromTopic:
    def test_yfs_customer_contact(self):
        assert lf.extract_table_from_topic("glenraven-dev.STERLING.yfs_customer_contact") == "YFS_CUSTOMER_CONTACT"

    def test_unknown_uppercased(self):
        assert lf.extract_table_from_topic("db.schema.some_table") == "SOME_TABLE"


# =============================================================================
# process_customer_contact — Phase 1 (DynamoDB cache)
# =============================================================================

class TestProcessCustomerContactPhase1:

    @patch("lambda_function.get_opensearch_client")
    @patch("lambda_function.cache_table")
    def test_composite_sk_format(self, mock_table, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.return_value = {"updated": 0}
        mock_table.get_item.side_effect = _mock_get_item_for_customer(SAMPLE_CUSTOMER_ITEM)
        lf.process_customer_contact(SAMPLE_CONTACT_PAYLOAD, "c")
        item = mock_table.put_item.call_args_list[0][1]["Item"]
        assert item["SK"] == "CKEY001#CCKEY001"

    @patch("lambda_function.get_opensearch_client")
    @patch("lambda_function.cache_table")
    def test_customer_contact_key_stored(self, mock_table, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.return_value = {"updated": 0}
        mock_table.get_item.side_effect = _mock_get_item_for_customer(SAMPLE_CUSTOMER_ITEM)
        lf.process_customer_contact(SAMPLE_CONTACT_PAYLOAD, "c")
        item = mock_table.put_item.call_args_list[0][1]["Item"]
        assert item["customerContactKey"] == "CCKEY001"

    @patch("lambda_function.get_opensearch_client")
    @patch("lambda_function.cache_table")
    def test_full_name_built_correctly(self, mock_table, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.return_value = {"updated": 0}
        mock_table.get_item.side_effect = _mock_get_item_for_customer(SAMPLE_CUSTOMER_ITEM)
        lf.process_customer_contact(SAMPLE_CONTACT_PAYLOAD, "c")
        item = mock_table.put_item.call_args_list[0][1]["Item"]
        assert item["contactName"] == "Jane A Doe"

    @patch("lambda_function.get_opensearch_client")
    @patch("lambda_function.cache_table")
    def test_contact_company_stored(self, mock_table, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.return_value = {"updated": 0}
        mock_table.get_item.side_effect = _mock_get_item_for_customer(SAMPLE_CUSTOMER_ITEM)
        lf.process_customer_contact(SAMPLE_CONTACT_PAYLOAD, "c")
        item = mock_table.put_item.call_args_list[0][1]["Item"]
        assert item["contactCompany"] == "Acme Corp"

    @patch("lambda_function.get_opensearch_client")
    @patch("lambda_function.cache_table")
    def test_contact_dob_stored(self, mock_table, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.return_value = {"updated": 0}
        mock_table.get_item.side_effect = _mock_get_item_for_customer(SAMPLE_CUSTOMER_ITEM)
        lf.process_customer_contact(SAMPLE_CONTACT_PAYLOAD, "c")
        item = mock_table.put_item.call_args_list[0][1]["Item"]
        assert item["contactDob"] == "1990-01-15"

    @patch("lambda_function.get_opensearch_client")
    @patch("lambda_function.cache_table")
    def test_delete_uses_composite_sk(self, mock_table, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.return_value = {"updated": 1}
        mock_table.get_item.side_effect = _mock_get_item_for_customer(SAMPLE_CUSTOMER_ITEM)
        result = lf.process_customer_contact(SAMPLE_CONTACT_PAYLOAD, "d")
        assert result["statusCode"] == 200
        mock_table.delete_item.assert_any_call(
            Key={'PK': 'CUSTOMER_CONTACT', 'SK': 'CKEY001#CCKEY001'}
        )

    def test_missing_customer_key_returns_400(self):
        result = lf.process_customer_contact({}, "c")
        assert result["statusCode"] == 400

    @patch("lambda_function.get_opensearch_client")
    @patch("lambda_function.cache_table")
    def test_phone_fallback_mobile_first(self, mock_table, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.return_value = {"updated": 0}
        mock_table.get_item.side_effect = _mock_get_item_for_customer(SAMPLE_CUSTOMER_ITEM)
        lf.process_customer_contact(SAMPLE_CONTACT_PAYLOAD, "c")
        item = mock_table.put_item.call_args_list[0][1]["Item"]
        assert item["contactPhone"] == "555-1234"

    @patch("lambda_function.get_opensearch_client")
    @patch("lambda_function.cache_table")
    def test_phone_fallback_day_phone(self, mock_table, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.return_value = {"updated": 0}
        mock_table.get_item.side_effect = _mock_get_item_for_customer(SAMPLE_CUSTOMER_ITEM)
        payload = {**SAMPLE_CONTACT_PAYLOAD, "MOBILE_PHONE": None, "DAY_PHONE": "555-9999"}
        lf.process_customer_contact(payload, "c")
        item = mock_table.put_item.call_args_list[0][1]["Item"]
        assert item["contactPhone"] == "555-9999"

    @patch("lambda_function.get_opensearch_client")
    @patch("lambda_function.cache_table")
    def test_reads_customer_for_buyer_org_code(self, mock_table, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.return_value = {"updated": 0}
        mock_table.get_item.side_effect = _mock_get_item_for_customer(SAMPLE_CUSTOMER_ITEM)
        lf.process_customer_contact(SAMPLE_CONTACT_PAYLOAD, "c")
        get_item_calls = mock_table.get_item.call_args_list
        customer_read = [c for c in get_item_calls if c[1].get('Key', {}).get('PK') == 'CUSTOMER']
        assert len(customer_read) == 1
        assert customer_read[0][1]['Key']['SK'] == 'CKEY001'


# =============================================================================
# process_customer_contact — Phase 2 (CUSTOMER_CONTACT_BY_ORG_KEY)
# =============================================================================

class TestProcessCustomerContactPhase2:

    @patch("lambda_function.get_opensearch_client")
    @patch("lambda_function.cache_table")
    def test_writes_customer_contact_by_org_key(self, mock_table, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.return_value = {"updated": 0}
        mock_table.get_item.side_effect = _mock_get_item_for_customer(SAMPLE_CUSTOMER_ITEM)
        lf.process_customer_contact(SAMPLE_CONTACT_PAYLOAD, "c")
        assert mock_table.put_item.call_count == 2
        org_item = mock_table.put_item.call_args_list[1][1]["Item"]
        assert org_item["PK"] == "CUSTOMER_CONTACT_BY_ORG_KEY"
        assert org_item["SK"] == "ORG001"
        assert org_item["customerKey"] == "CKEY001"
        assert org_item["customerContactKey"] == "CCKEY001"

    @patch("lambda_function.get_opensearch_client")
    @patch("lambda_function.cache_table")
    def test_phase2_includes_hash(self, mock_table, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.return_value = {"updated": 0}
        mock_table.get_item.side_effect = _mock_get_item_for_customer(SAMPLE_CUSTOMER_ITEM)
        lf.process_customer_contact(SAMPLE_CONTACT_PAYLOAD, "c")
        org_item = mock_table.put_item.call_args_list[1][1]["Item"]
        assert "customerContactHash" in org_item
        assert len(org_item["customerContactHash"]) == 16

    @patch("lambda_function.get_opensearch_client")
    @patch("lambda_function.cache_table")
    def test_phase2_includes_company_and_dob(self, mock_table, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.return_value = {"updated": 0}
        mock_table.get_item.side_effect = _mock_get_item_for_customer(SAMPLE_CUSTOMER_ITEM)
        lf.process_customer_contact(SAMPLE_CONTACT_PAYLOAD, "c")
        org_item = mock_table.put_item.call_args_list[1][1]["Item"]
        assert org_item["contactCompany"] == "Acme Corp"
        assert org_item["contactDob"] == "1990-01-15"

    @patch("lambda_function.get_opensearch_client")
    @patch("lambda_function.cache_table")
    def test_phase2_skipped_when_no_buyer_org_code(self, mock_table, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.return_value = {"updated": 0}
        mock_table.get_item.side_effect = _mock_get_item_for_customer(
            {"PK": "CUSTOMER", "SK": "CKEY001", "customerKey": "CKEY001"}
        )
        lf.process_customer_contact(SAMPLE_CONTACT_PAYLOAD, "c")
        assert mock_table.put_item.call_count == 1

    @patch("lambda_function.get_opensearch_client")
    @patch("lambda_function.cache_table")
    def test_phase2_skipped_when_no_customer_record(self, mock_table, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.return_value = {"updated": 0}
        mock_table.get_item.return_value = {}
        lf.process_customer_contact(SAMPLE_CONTACT_PAYLOAD, "c")
        assert mock_table.put_item.call_count == 1

    @patch("lambda_function.get_opensearch_client")
    @patch("lambda_function.cache_table")
    def test_phase2_deletes_on_delete(self, mock_table, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.return_value = {"updated": 0}
        mock_table.get_item.side_effect = _mock_get_item_for_customer(SAMPLE_CUSTOMER_ITEM)
        lf.process_customer_contact(SAMPLE_CONTACT_PAYLOAD, "d")
        # Should delete CUSTOMER_CONTACT_BY_ORG_KEY (Phase 2)
        delete_calls = mock_table.delete_item.call_args_list
        org_key_deletes = [c for c in delete_calls if c[1]['Key'].get('PK') == 'CUSTOMER_CONTACT_BY_ORG_KEY']
        assert len(org_key_deletes) == 1
        assert org_key_deletes[0][1]['Key']['SK'] == 'ORG001'

    @patch("lambda_function.get_opensearch_client")
    @patch("lambda_function.cache_table")
    def test_phase2_contains_contact_fields(self, mock_table, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.return_value = {"updated": 0}
        mock_table.get_item.side_effect = _mock_get_item_for_customer(SAMPLE_CUSTOMER_ITEM)
        lf.process_customer_contact(SAMPLE_CONTACT_PAYLOAD, "c")
        org_item = mock_table.put_item.call_args_list[1][1]["Item"]
        assert org_item["contactName"] == "Jane A Doe"
        assert org_item["contactEmail"] == "jane@example.com"
        assert org_item["contactPhone"] == "555-1234"


# =============================================================================
# process_customer_contact — Phase 3 (OpenSearch fan-out)
# =============================================================================

class TestProcessCustomerContactPhase3:

    @patch("lambda_function.build_contacts_array")
    @patch("lambda_function.get_opensearch_client")
    @patch("lambda_function.cache_table")
    def test_fanout_sends_contacts_array(self, mock_table, mock_client, mock_build):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.return_value = {"updated": 1}
        mock_table.get_item.side_effect = _mock_get_item_for_customer(SAMPLE_CUSTOMER_ITEM)
        mock_build.return_value = [
            {'contactName': 'Jane A Doe', 'contactEmail': 'jane@example.com'},
            {'contactName': 'Bob Lee', 'contactEmail': 'bob@example.com'},
        ]
        lf.process_customer_contact(SAMPLE_CONTACT_PAYLOAD, "c")
        body = mock_os.update_by_query.call_args[1]["body"]
        params = body["script"]["params"]
        assert "customerContact" in params
        assert isinstance(params["customerContact"], list)
        assert len(params["customerContact"]) == 2
        assert params["customerContact"][0]["contactName"] == "Jane A Doe"

    @patch("lambda_function.get_opensearch_client")
    @patch("lambda_function.cache_table")
    def test_fanout_query_uses_customer_dot_customer_key(self, mock_table, mock_client):
        """Verify query targets customer.customerKey (nested field in org index)."""
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.return_value = {"updated": 1}
        mock_table.get_item.side_effect = _mock_get_item_for_customer(SAMPLE_CUSTOMER_ITEM)
        lf.process_customer_contact(SAMPLE_CONTACT_PAYLOAD, "c")
        body = mock_os.update_by_query.call_args[1]["body"]
        assert body["query"]["term"]["customer.customerKey"] == "CKEY001"

    @patch("lambda_function.build_contacts_array")
    @patch("lambda_function.get_opensearch_client")
    @patch("lambda_function.cache_table")
    def test_delete_rebuilds_contacts_array(self, mock_table, mock_client, mock_build):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.return_value = {"updated": 1}
        mock_table.get_item.side_effect = _mock_get_item_for_customer(SAMPLE_CUSTOMER_ITEM)
        mock_build.return_value = [{'contactName': 'Remaining Contact'}]
        lf.process_customer_contact(SAMPLE_CONTACT_PAYLOAD, "d")
        body = mock_os.update_by_query.call_args[1]["body"]
        params = body["script"]["params"]
        assert isinstance(params["customerContact"], list)
        assert len(params["customerContact"]) == 1

    @patch("lambda_function.get_opensearch_client")
    @patch("lambda_function.cache_table")
    def test_fanout_failure_non_fatal(self, mock_table, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.side_effect = Exception("OS down")
        mock_table.get_item.side_effect = _mock_get_item_for_customer(SAMPLE_CUSTOMER_ITEM)
        result = lf.process_customer_contact(SAMPLE_CONTACT_PAYLOAD, "c")
        assert result["statusCode"] == 200

    @patch("lambda_function.get_opensearch_client")
    @patch("lambda_function.cache_table")
    def test_conflicts_proceed(self, mock_table, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.return_value = {"updated": 0}
        mock_table.get_item.side_effect = _mock_get_item_for_customer(SAMPLE_CUSTOMER_ITEM)
        lf.process_customer_contact(SAMPLE_CONTACT_PAYLOAD, "c")
        assert mock_os.update_by_query.call_args[1]["conflicts"] == "proceed"


# =============================================================================
# process_single_event routing
# =============================================================================

class TestProcessSingleEvent:

    @patch("lambda_function.process_customer_contact")
    def test_routes_yfs_customer_contact(self, mock_proc):
        mock_proc.return_value = {"statusCode": 200}
        lf.process_single_event("YFS_CUSTOMER_CONTACT", "c", {})
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
        event = _make_msk_event({"op": "c", "after": SAMPLE_CONTACT_PAYLOAD, "ts_ms": 123})
        result = lf.process_msk_event(event)
        assert result["statusCode"] == 200
        mock_proc.assert_called_once()

    @patch("lambda_function.process_single_event")
    def test_delete_uses_before_image(self, mock_proc):
        mock_proc.return_value = {"statusCode": 200}
        before = {**SAMPLE_CONTACT_PAYLOAD, "CUSTOMER_KEY": "DELKEY"}
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
                "topic-0": [{"value": "NOT_VALID!!!", "topic": "glenraven-dev.STERLING.yfs_customer_contact", "offset": 1}]
            }
        }
        result = lf.process_msk_event(event)
        assert result["statusCode"] == 207

    @patch("lambda_function.process_single_event")
    def test_non_200_result_returns_207(self, mock_proc):
        mock_proc.return_value = {"statusCode": 400, "body": "bad"}
        event = _make_msk_event({"op": "c", "after": SAMPLE_CONTACT_PAYLOAD, "ts_ms": 1})
        result = lf.process_msk_event(event)
        assert result["statusCode"] == 207

    @patch("lambda_function.process_single_event")
    def test_empty_records_returns_200(self, mock_proc):
        result = lf.process_msk_event({"records": {}})
        assert result["statusCode"] == 200
        mock_proc.assert_not_called()


# =============================================================================
# lambda_handler entry point
# =============================================================================

class TestLambdaHandler:

    @patch("lambda_function.process_single_event")
    def test_direct_invocation(self, mock_proc):
        mock_proc.return_value = {"statusCode": 200}
        result = lf.lambda_handler({"table": "YFS_CUSTOMER_CONTACT", "operation": "c", "payload": SAMPLE_CONTACT_PAYLOAD}, None)
        assert result["statusCode"] == 200

    @patch("lambda_function.process_msk_event")
    def test_msk_trigger(self, mock_msk):
        mock_msk.return_value = {"statusCode": 200}
        lf.lambda_handler({"records": {}}, None)
        mock_msk.assert_called_once()

    @patch("lambda_function.process_single_event")
    def test_unhandled_exception_returns_500(self, mock_proc):
        mock_proc.side_effect = RuntimeError("Boom")
        result = lf.lambda_handler({"table": "YFS_CUSTOMER_CONTACT", "operation": "c", "payload": {}}, None)
        assert result["statusCode"] == 500
        assert "Boom" in result["body"]


# =============================================================================
# Real CDC event — from Kafka topic (flattened SMT format)
# =============================================================================

REAL_CUSTOMER_CONTACT_CDC_EVENT = {
    "schema": {
        "type": "struct",
        "optional": False,
        "name": "glenraven-dev.STERLING.YFS_CUSTOMER_CONTACT.Value",
        "fields": []
    },
    "payload": {
        "CUSTOMER_CONTACT_KEY": "202512240732206844486578",
        "CUSTOMER_KEY": "202512240718126844486389",
        "USER_ID": " ",
        "CUSTOMER_CONTACT_ID": "100212988",
        "TITLE": " ",
        "FIRST_NAME": "namm",
        "MIDDLE_NAME": " ",
        "LAST_NAME": "test",
        "EMAILID": "namrata@gmail.com",
        "COMPANY": " ",
        "DAY_PHONE": "                                        ",
        "EVENING_PHONE": "                                        ",
        "MOBILE_PHONE": "                                        ",
        "DAY_FAX_NO": "                                        ",
        "EVENING_FAX_N0": "                                        ",
        "DOB": None,
        "SPOUSE_DOB": None,
        "WEDDING_ANNIVERSARY": None,
        "SPENDING_LIMIT_CURRENCY": None,
        "SPENDING_LIMIT": None,
        "APPROVER_USER_ID": None,
        "APPROVER_PROXY_USER_ID": None,
        "DEPARTMENT": None,
        "JOB_TITLE": None,
        "STATUS": "10",
        "LOCKID": 0.0,
        "CREATETS": 1766561539000,
        "MODIFYTS": 1766561539000,
        "CREATEUSERID": "CSASINHN",
        "MODIFYUSERID": "CSASINHN",
        "CREATEPROGID": "ISCCSSYS001",
        "MODIFYPROGID": "ISCCSSYS001",
        "EXTN_CONTACT_TYPE": "REGULAR",
        "EXTN_STATEMENT_EMAIL": "namSinha@gmail.com",
        "EXTN_NOTIFICATION_TYPE": "Email",
        "EMAILID_LC": "namrata@gmail.com",
        "LAST_NAME_LC": "test",
        "FIRST_NAME_LC": "namm",
        "__op": "r",
        "__table": "YFS_CUSTOMER_CONTACT",
        "__source_ts_ms": 1773211028000
    }
}

REAL_CUSTOMER_ITEM = {
    "PK": "CUSTOMER",
    "SK": "202512240718126844486389",
    "customerKey": "202512240718126844486389",
    "customerId": "100212988",
    "buyerOrganizationCode": "REAL_ORG_001",
}


def _make_msk_event_from_raw(cdc_event: dict,
                              topic: str = "glenraven-dev.STERLING.yfs_customer_contact") -> dict:
    encoded = base64.b64encode(json.dumps(cdc_event).encode()).decode()
    return {
        "eventSource": "aws:kafka",
        "records": {
            f"{topic}-0": [{"topic": topic, "offset": 0, "value": encoded}]
        }
    }


class TestRealCustomerContactCdcEvent:
    """End-to-end tests using actual Kafka CDC data — YFS_CUSTOMER_CONTACT."""

    @patch("lambda_function.get_opensearch_client")
    @patch("lambda_function.cache_table")
    def test_processes_successfully(self, mock_table, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.return_value = {"updated": 0}
        mock_table.get_item.side_effect = _mock_get_item_for_customer(REAL_CUSTOMER_ITEM)
        event = _make_msk_event_from_raw(REAL_CUSTOMER_CONTACT_CDC_EVENT)
        result = lf.lambda_handler(event, None)
        assert result["statusCode"] == 200

    @patch("lambda_function.get_opensearch_client")
    @patch("lambda_function.cache_table")
    def test_composite_sk_in_cache(self, mock_table, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.return_value = {"updated": 0}
        mock_table.get_item.side_effect = _mock_get_item_for_customer(REAL_CUSTOMER_ITEM)
        event = _make_msk_event_from_raw(REAL_CUSTOMER_CONTACT_CDC_EVENT)
        lf.lambda_handler(event, None)
        item = mock_table.put_item.call_args_list[0][1]["Item"]
        assert item["SK"] == "202512240718126844486389#202512240732206844486578"

    @patch("lambda_function.get_opensearch_client")
    @patch("lambda_function.cache_table")
    def test_full_name_built_from_first_and_last(self, mock_table, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.return_value = {"updated": 0}
        mock_table.get_item.side_effect = _mock_get_item_for_customer(REAL_CUSTOMER_ITEM)
        event = _make_msk_event_from_raw(REAL_CUSTOMER_CONTACT_CDC_EVENT)
        lf.lambda_handler(event, None)
        item = mock_table.put_item.call_args_list[0][1]["Item"]
        assert item["contactName"] == "namm test"

    @patch("lambda_function.get_opensearch_client")
    @patch("lambda_function.cache_table")
    def test_email_stored(self, mock_table, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.return_value = {"updated": 0}
        mock_table.get_item.side_effect = _mock_get_item_for_customer(REAL_CUSTOMER_ITEM)
        event = _make_msk_event_from_raw(REAL_CUSTOMER_CONTACT_CDC_EVENT)
        lf.lambda_handler(event, None)
        item = mock_table.put_item.call_args_list[0][1]["Item"]
        assert item["contactEmail"] == "namrata@gmail.com"

    @patch("lambda_function.get_opensearch_client")
    @patch("lambda_function.cache_table")
    def test_all_phones_blank_so_no_phone_in_cache(self, mock_table, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.return_value = {"updated": 0}
        mock_table.get_item.side_effect = _mock_get_item_for_customer(REAL_CUSTOMER_ITEM)
        event = _make_msk_event_from_raw(REAL_CUSTOMER_CONTACT_CDC_EVENT)
        lf.lambda_handler(event, None)
        item = mock_table.put_item.call_args_list[0][1]["Item"]
        assert "contactPhone" not in item

    @patch("lambda_function.get_opensearch_client")
    @patch("lambda_function.cache_table")
    def test_fanout_targets_customer_dot_customer_key(self, mock_table, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.return_value = {"updated": 0}
        mock_table.get_item.side_effect = _mock_get_item_for_customer(REAL_CUSTOMER_ITEM)
        event = _make_msk_event_from_raw(REAL_CUSTOMER_CONTACT_CDC_EVENT)
        lf.lambda_handler(event, None)
        body = mock_os.update_by_query.call_args[1]["body"]
        assert body["query"]["term"]["customer.customerKey"] == "202512240718126844486389"

    @patch("lambda_function.get_opensearch_client")
    @patch("lambda_function.cache_table")
    def test_fanout_uses_nested_customer_contact(self, mock_table, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.return_value = {"updated": 0}
        mock_table.get_item.side_effect = _mock_get_item_for_customer(REAL_CUSTOMER_ITEM)
        event = _make_msk_event_from_raw(REAL_CUSTOMER_CONTACT_CDC_EVENT)
        lf.lambda_handler(event, None)
        body = mock_os.update_by_query.call_args[1]["body"]
        assert "customerContact" in body["script"]["params"]
        # customerContact is now a list (from build_contacts_array)
        cc = body["script"]["params"]["customerContact"]
        assert cc is None or isinstance(cc, list)

    @patch("lambda_function.get_opensearch_client")
    @patch("lambda_function.cache_table")
    def test_phase2_writes_org_key_entry_with_hash(self, mock_table, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.return_value = {"updated": 0}
        mock_table.get_item.side_effect = _mock_get_item_for_customer(REAL_CUSTOMER_ITEM)
        event = _make_msk_event_from_raw(REAL_CUSTOMER_CONTACT_CDC_EVENT)
        lf.lambda_handler(event, None)
        assert mock_table.put_item.call_count == 2
        org_item = mock_table.put_item.call_args_list[1][1]["Item"]
        assert org_item["PK"] == "CUSTOMER_CONTACT_BY_ORG_KEY"
        assert org_item["SK"] == "REAL_ORG_001"
        assert "customerContactHash" in org_item

    @patch("lambda_function.get_opensearch_client")
    @patch("lambda_function.cache_table")
    def test_cdc_operation_extracted_as_r(self, mock_table, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.return_value = {"updated": 0}
        mock_table.get_item.side_effect = _mock_get_item_for_customer(REAL_CUSTOMER_ITEM)
        event = _make_msk_event_from_raw(REAL_CUSTOMER_CONTACT_CDC_EVENT)
        lf.lambda_handler(event, None)
        assert mock_table.put_item.call_count >= 1

    @patch("lambda_function.get_opensearch_client")
    @patch("lambda_function.cache_table")
    def test_dunder_keys_not_in_cache(self, mock_table, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.return_value = {"updated": 0}
        mock_table.get_item.side_effect = _mock_get_item_for_customer(REAL_CUSTOMER_ITEM)
        event = _make_msk_event_from_raw(REAL_CUSTOMER_CONTACT_CDC_EVENT)
        lf.lambda_handler(event, None)
        item = mock_table.put_item.call_args_list[0][1]["Item"]
        assert not any(k.startswith("__") for k in item)


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
# Soft Delete — cdc-customercontact-processor
# =============================================================================

class TestSoftDeleteCustomerContact:
    """
    Tests for the ENABLE_HARD_DELETE toggle on the customercontact lambda.

    Soft delete (default, ENABLE_HARD_DELETE=false):
      - CUSTOMER_CONTACT and CUSTOMER_CONTACT_BY_ORG_KEY rows are marked with
        _deleted=True via update_item
      - delete_item is NOT called

    Hard delete (ENABLE_HARD_DELETE=true):
      - delete_item is called for both rows
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
        mock_table.get_item.side_effect = _mock_get_item_for_customer(SAMPLE_CUSTOMER_ITEM)
        with patch.dict("os.environ", {"ENABLE_HARD_DELETE": "false"}):
            result = lf.process_customer_contact(SAMPLE_CONTACT_PAYLOAD, "d")
        assert result["statusCode"] == 200
        assert mock_table.update_item.call_count >= 1
        mock_table.delete_item.assert_not_called()

    @patch("lambda_function.get_opensearch_client")
    @patch("lambda_function.cache_table")
    def test_soft_delete_marks_customer_contact_row(self, mock_table, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.return_value = {"updated": 0}
        mock_table.get_item.side_effect = _mock_get_item_for_customer(SAMPLE_CUSTOMER_ITEM)
        with patch.dict("os.environ", {"ENABLE_HARD_DELETE": "false"}):
            lf.process_customer_contact(SAMPLE_CONTACT_PAYLOAD, "d")
        pks = [c[1]["Key"]["PK"] for c in mock_table.update_item.call_args_list]
        assert "CUSTOMER_CONTACT" in pks

    @patch("lambda_function.get_opensearch_client")
    @patch("lambda_function.cache_table")
    def test_soft_delete_sets_deleted_true_on_all_rows(self, mock_table, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.return_value = {"updated": 0}
        mock_table.get_item.side_effect = _mock_get_item_for_customer(SAMPLE_CUSTOMER_ITEM)
        with patch.dict("os.environ", {"ENABLE_HARD_DELETE": "false"}):
            lf.process_customer_contact(SAMPLE_CONTACT_PAYLOAD, "d")
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
        mock_table.get_item.side_effect = _mock_get_item_for_customer(SAMPLE_CUSTOMER_ITEM)
        with patch.dict("os.environ", {"ENABLE_HARD_DELETE": "true"}):
            result = lf.process_customer_contact(SAMPLE_CONTACT_PAYLOAD, "d")
        assert result["statusCode"] == 200
        assert mock_table.delete_item.call_count >= 1
        mock_table.update_item.assert_not_called()

    @patch("lambda_function.get_opensearch_client")
    @patch("lambda_function.cache_table")
    def test_hard_delete_removes_customer_contact_row(self, mock_table, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.return_value = {"updated": 0}
        mock_table.get_item.side_effect = _mock_get_item_for_customer(SAMPLE_CUSTOMER_ITEM)
        with patch.dict("os.environ", {"ENABLE_HARD_DELETE": "true"}):
            lf.process_customer_contact(SAMPLE_CONTACT_PAYLOAD, "d")
        keys_deleted = [c[1]["Key"] for c in mock_table.delete_item.call_args_list]
        assert any(k["PK"] == "CUSTOMER_CONTACT" for k in keys_deleted)

