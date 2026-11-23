"""
Unit tests for cdc-lambda-personinfo/lambda_function.py

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

SAMPLE_PERSON_INFO_PAYLOAD = {
    "PERSON_INFO_KEY": "PIK001",
    "FIRST_NAME":      "John",
    "MIDDLE_NAME":     "W",
    "LAST_NAME":       "Doe",
    "COMPANY":         "Acme Corp",
    "ADDRESS_LINE1":   "123 Main St",
    "CITY":            "Springfield",
    "STATE":           "IL",
    "ZIP_CODE":        "62701",
    "COUNTRY":         "US",
    "EMAILID":         "john@example.com",
    "DAY_PHONE":       "555-1111",
    "MOBILE_PHONE":    "555-2222",
    "EVENING_PHONE":   "555-3333",
}


def _make_msk_event(debezium_event: dict, topic: str = "glenraven-dev.STERLING.yfs_person_info") -> dict:
    encoded = base64.b64encode(json.dumps(debezium_event).encode()).decode()
    return {
        "records": {
            "topic-0": [{"value": encoded, "topic": topic, "offset": 1}]
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

    def test_integer_cast_to_string(self):
        assert lf.get_value({"A": 42}, "A") == "42"


class TestNowMs:
    def test_returns_int(self):
        assert isinstance(lf.now_ms(), int)

    def test_recent(self):
        assert lf.now_ms() > 1_700_000_000_000


# =============================================================================
# extract_table_from_topic
# =============================================================================

class TestExtractTableFromTopic:
    def test_yfs_person_info(self):
        assert lf.extract_table_from_topic("glenraven-dev.STERLING.yfs_person_info") == "YFS_PERSON_INFO"

    def test_unknown_uppercased(self):
        assert lf.extract_table_from_topic("db.schema.some_table") == "SOME_TABLE"


# =============================================================================
# process_person_info — Phase 1 DynamoDB
# =============================================================================

class TestProcessPersonInfo:

    @patch("lambda_function.update_org_address_roles")
    @patch("lambda_function.update_org_contact")
    @patch("lambda_function.update_org_corporate_address")
    @patch("lambda_function.update_orders_with_person_info")
    @patch("lambda_function.cache_table")
    def test_caches_and_calls_all_phases(self, mock_table, mock_orders, mock_corp, mock_contact, mock_roles):
        result = lf.process_person_info(SAMPLE_PERSON_INFO_PAYLOAD, "c")
        assert result["statusCode"] == 200
        mock_table.put_item.assert_called_once()
        mock_orders.assert_called_once()
        mock_corp.assert_called_once()
        mock_contact.assert_called_once()
        mock_roles.assert_called_once()

    @patch("lambda_function.update_org_address_roles")
    @patch("lambda_function.update_org_contact")
    @patch("lambda_function.update_org_corporate_address")
    @patch("lambda_function.update_orders_with_person_info")
    @patch("lambda_function.cache_table")
    def test_cache_item_contains_full_name(self, mock_table, mock_orders, mock_corp, mock_contact, mock_roles):
        lf.process_person_info(SAMPLE_PERSON_INFO_PAYLOAD, "c")
        item = mock_table.put_item.call_args[1]["Item"]
        assert item["fullName"] == "John W Doe"

    @patch("lambda_function.update_org_address_roles")
    @patch("lambda_function.update_org_contact")
    @patch("lambda_function.update_org_corporate_address")
    @patch("lambda_function.update_orders_with_person_info")
    @patch("lambda_function.cache_table")
    def test_cache_item_pk_sk(self, mock_table, mock_orders, mock_corp, mock_contact, mock_roles):
        lf.process_person_info(SAMPLE_PERSON_INFO_PAYLOAD, "c")
        item = mock_table.put_item.call_args[1]["Item"]
        assert item["PK"] == "PERSON_INFO"
        assert item["SK"] == "PIK001"

    @patch("lambda_function.update_org_address_roles")
    @patch("lambda_function.update_org_contact")
    @patch("lambda_function.update_org_corporate_address")
    @patch("lambda_function.update_orders_with_person_info")
    @patch("lambda_function.cache_table")
    def test_delete_removes_from_cache(self, mock_table, mock_orders, mock_corp, mock_contact, mock_roles):
        result = lf.process_person_info(SAMPLE_PERSON_INFO_PAYLOAD, "d")
        assert result["statusCode"] == 200
        mock_table.delete_item.assert_called_once()
        mock_table.put_item.assert_not_called()

    def test_missing_person_info_key_returns_400(self):
        result = lf.process_person_info({}, "c")
        assert result["statusCode"] == 400


# =============================================================================
# update_orders_with_person_info — Phase 2
# =============================================================================

class TestUpdateOrdersWithPersonInfo:

    @patch("lambda_function.get_opensearch_client")
    def test_queries_all_three_roles(self, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.return_value = {"updated": 0}
        lf.update_orders_with_person_info("PIK001", SAMPLE_PERSON_INFO_PAYLOAD, "c")
        assert mock_os.update_by_query.call_count == 3

    @patch("lambda_function.get_opensearch_client")
    def test_queries_all_key_fields(self, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.return_value = {"updated": 0}
        lf.update_orders_with_person_info("PIK001", SAMPLE_PERSON_INFO_PAYLOAD, "c")
        calls = mock_os.update_by_query.call_args_list
        query_terms = [c[1]["body"]["query"]["term"] for c in calls]
        assert {"billToKey": "PIK001"} in query_terms
        assert {"shipToKey": "PIK001"} in query_terms
        assert {"soldToKey": "PIK001"} in query_terms

    @patch("lambda_function.get_opensearch_client")
    def test_address_object_full_name(self, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.return_value = {"updated": 0}
        lf.update_orders_with_person_info("PIK001", SAMPLE_PERSON_INFO_PAYLOAD, "c")
        params = mock_os.update_by_query.call_args_list[0][1]["body"]["script"]["params"]
        assert params["address"]["fullName"] == "John W Doe"

    @patch("lambda_function.get_opensearch_client")
    def test_address_object_has_person_info_key(self, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.return_value = {"updated": 0}
        lf.update_orders_with_person_info("PIK001", SAMPLE_PERSON_INFO_PAYLOAD, "c")
        params = mock_os.update_by_query.call_args_list[0][1]["body"]["script"]["params"]
        assert params["address"]["personInfoKey"] == "PIK001"

    @patch("lambda_function.get_opensearch_client")
    def test_delete_sets_address_to_null(self, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.return_value = {"updated": 0}
        lf.update_orders_with_person_info("PIK001", SAMPLE_PERSON_INFO_PAYLOAD, "d")
        params = mock_os.update_by_query.call_args_list[0][1]["body"]["script"]["params"]
        assert params["address"] is None

    @patch("lambda_function.get_opensearch_client")
    def test_role_param_cycles_through_all_roles(self, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.return_value = {"updated": 0}
        lf.update_orders_with_person_info("PIK001", SAMPLE_PERSON_INFO_PAYLOAD, "c")
        roles = [c[1]["body"]["script"]["params"]["role"] for c in mock_os.update_by_query.call_args_list]
        assert roles == ["billTo", "shipTo", "soldTo"]

    @patch("lambda_function.get_opensearch_client")
    def test_conflicts_proceed(self, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.return_value = {"updated": 0}
        lf.update_orders_with_person_info("PIK001", SAMPLE_PERSON_INFO_PAYLOAD, "c")
        assert all(c[1]["conflicts"] == "proceed" for c in mock_os.update_by_query.call_args_list)

    @patch("lambda_function.get_opensearch_client")
    def test_failure_non_fatal(self, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.side_effect = Exception("OS down")
        lf.update_orders_with_person_info("PIK001", SAMPLE_PERSON_INFO_PAYLOAD, "c")  # must not raise


# =============================================================================
# update_org_corporate_address — Phase 3
# =============================================================================

class TestUpdateOrgCorporateAddress:

    @patch("lambda_function.get_opensearch_client")
    def test_queries_corporate_address_key(self, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.return_value = {"updated": 1}
        lf.update_org_corporate_address("PIK001", SAMPLE_PERSON_INFO_PAYLOAD, "c")
        body = mock_os.update_by_query.call_args[1]["body"]
        assert body["query"]["bool"]["must"]["term"]["corporateAddressKey"] == "PIK001"

    @patch("lambda_function.get_opensearch_client")
    def test_sets_address_fields(self, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.return_value = {"updated": 1}
        lf.update_org_corporate_address("PIK001", SAMPLE_PERSON_INFO_PAYLOAD, "c")
        params = mock_os.update_by_query.call_args[1]["body"]["script"]["params"]
        assert params["address"]["city"] == "Springfield"
        assert params["address"]["state"] == "IL"
        assert params["address"]["zipCode"] == "62701"
        assert params["address"]["country"] == "US"

    @patch("lambda_function.get_opensearch_client")
    def test_delete_nulls_address_fields(self, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.return_value = {"updated": 0}
        lf.update_org_corporate_address("PIK001", SAMPLE_PERSON_INFO_PAYLOAD, "d")
        params = mock_os.update_by_query.call_args[1]["body"]["script"]["params"]
        assert params["address"] is None

    @patch("lambda_function.get_opensearch_client")
    def test_conflicts_proceed(self, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.return_value = {"updated": 0}
        lf.update_org_corporate_address("PIK001", SAMPLE_PERSON_INFO_PAYLOAD, "c")
        assert mock_os.update_by_query.call_args[1]["conflicts"] == "proceed"

    @patch("lambda_function.get_opensearch_client")
    def test_failure_non_fatal(self, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.side_effect = Exception("OS down")
        lf.update_org_corporate_address("PIK001", SAMPLE_PERSON_INFO_PAYLOAD, "c")  # must not raise


# =============================================================================
# update_org_contact — Phase 4
# =============================================================================

class TestUpdateOrgContact:

    @patch("lambda_function.get_opensearch_client")
    def test_queries_contact_address_key(self, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.return_value = {"updated": 1}
        lf.update_org_contact("PIK001", SAMPLE_PERSON_INFO_PAYLOAD, "c")
        body = mock_os.update_by_query.call_args[1]["body"]
        assert body["query"]["bool"]["must"]["term"]["contactAddressKey"] == "PIK001"

    @patch("lambda_function.get_opensearch_client")
    def test_contact_name_built_correctly(self, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.return_value = {"updated": 0}
        lf.update_org_contact("PIK001", SAMPLE_PERSON_INFO_PAYLOAD, "c")
        params = mock_os.update_by_query.call_args[1]["body"]["script"]["params"]
        assert params["contact"]["fullName"] == "John W Doe"
        assert params["contact"]["email"] == "john@example.com"

    @patch("lambda_function.get_opensearch_client")
    def test_phone_priority_mobile_first(self, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.return_value = {"updated": 0}
        lf.update_org_contact("PIK001", SAMPLE_PERSON_INFO_PAYLOAD, "c")
        params = mock_os.update_by_query.call_args[1]["body"]["script"]["params"]
        assert params["contact"]["phone"] == "555-2222"  # mobile takes priority

    @patch("lambda_function.get_opensearch_client")
    def test_phone_fallback_day_phone(self, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.return_value = {"updated": 0}
        payload = {**SAMPLE_PERSON_INFO_PAYLOAD, "MOBILE_PHONE": None}
        lf.update_org_contact("PIK001", payload, "c")
        params = mock_os.update_by_query.call_args[1]["body"]["script"]["params"]
        assert params["contact"]["phone"] == "555-1111"

    @patch("lambda_function.get_opensearch_client")
    def test_phone_fallback_evening_phone(self, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.return_value = {"updated": 0}
        payload = {**SAMPLE_PERSON_INFO_PAYLOAD, "MOBILE_PHONE": None, "DAY_PHONE": None}
        lf.update_org_contact("PIK001", payload, "c")
        params = mock_os.update_by_query.call_args[1]["body"]["script"]["params"]
        assert params["contact"]["phone"] == "555-3333"

    @patch("lambda_function.get_opensearch_client")
    def test_delete_nulls_contact_fields(self, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.return_value = {"updated": 0}
        lf.update_org_contact("PIK001", SAMPLE_PERSON_INFO_PAYLOAD, "d")
        params = mock_os.update_by_query.call_args[1]["body"]["script"]["params"]
        assert params["contact"] is None

    @patch("lambda_function.get_opensearch_client")
    def test_failure_non_fatal(self, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.side_effect = Exception("OS down")
        lf.update_org_contact("PIK001", SAMPLE_PERSON_INFO_PAYLOAD, "c")  # must not raise


# =============================================================================
# update_org_address_roles — Phase 5
# =============================================================================

class TestUpdateOrgAddressRoles:

    @patch("lambda_function.get_opensearch_client")
    def test_queries_all_three_roles(self, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.return_value = {"updated": 0}
        lf.update_org_address_roles("PIK001", SAMPLE_PERSON_INFO_PAYLOAD, "c")
        assert mock_os.update_by_query.call_count == 3

    @patch("lambda_function.get_opensearch_client")
    def test_queries_use_nested_person_info_key(self, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.return_value = {"updated": 0}
        lf.update_org_address_roles("PIK001", SAMPLE_PERSON_INFO_PAYLOAD, "c")
        calls = mock_os.update_by_query.call_args_list
        query_terms = [list(c[1]["body"]["query"]["term"].keys())[0] for c in calls]
        assert "billTo.personInfoKey" in query_terms
        assert "shipTo.personInfoKey" in query_terms
        assert "soldTo.personInfoKey" in query_terms

    @patch("lambda_function.get_opensearch_client")
    def test_address_object_has_person_info_key(self, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.return_value = {"updated": 0}
        lf.update_org_address_roles("PIK001", SAMPLE_PERSON_INFO_PAYLOAD, "c")
        params = mock_os.update_by_query.call_args_list[0][1]["body"]["script"]["params"]
        assert params["address"]["personInfoKey"] == "PIK001"

    @patch("lambda_function.get_opensearch_client")
    def test_address_object_full_name(self, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.return_value = {"updated": 0}
        lf.update_org_address_roles("PIK001", SAMPLE_PERSON_INFO_PAYLOAD, "c")
        params = mock_os.update_by_query.call_args_list[0][1]["body"]["script"]["params"]
        assert params["address"]["fullName"] == "John W Doe"

    @patch("lambda_function.get_opensearch_client")
    def test_delete_sets_address_to_null(self, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.return_value = {"updated": 0}
        lf.update_org_address_roles("PIK001", SAMPLE_PERSON_INFO_PAYLOAD, "d")
        params = mock_os.update_by_query.call_args_list[0][1]["body"]["script"]["params"]
        assert params["address"] is None

    @patch("lambda_function.get_opensearch_client")
    def test_conflicts_proceed(self, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.return_value = {"updated": 0}
        lf.update_org_address_roles("PIK001", SAMPLE_PERSON_INFO_PAYLOAD, "c")
        assert all(c[1]["conflicts"] == "proceed" for c in mock_os.update_by_query.call_args_list)

    @patch("lambda_function.get_opensearch_client")
    def test_failure_non_fatal(self, mock_client):
        mock_os = MagicMock()
        mock_client.return_value = mock_os
        mock_os.update_by_query.side_effect = Exception("OS down")
        lf.update_org_address_roles("PIK001", SAMPLE_PERSON_INFO_PAYLOAD, "c")  # must not raise


# =============================================================================
# process_single_event routing
# =============================================================================

class TestProcessSingleEvent:

    @patch("lambda_function.process_person_info")
    def test_routes_yfs_person_info(self, mock_proc):
        mock_proc.return_value = {"statusCode": 200}
        lf.process_single_event("YFS_PERSON_INFO", "c", {})
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
        event = _make_msk_event({"op": "c", "after": SAMPLE_PERSON_INFO_PAYLOAD, "ts_ms": 123})
        result = lf.process_msk_event(event)
        assert result["statusCode"] == 200
        mock_proc.assert_called_once()

    @patch("lambda_function.process_single_event")
    def test_delete_uses_before_image(self, mock_proc):
        mock_proc.return_value = {"statusCode": 200}
        before = {**SAMPLE_PERSON_INFO_PAYLOAD, "PERSON_INFO_KEY": "DELKEY"}
        event = _make_msk_event({"op": "d", "before": before, "ts_ms": 1})
        lf.process_msk_event(event)
        _, _, payload, _ = mock_proc.call_args[0]
        assert payload.get("PERSON_INFO_KEY") == "DELKEY"

    @patch("lambda_function.process_single_event")
    def test_payload_keys_uppercased(self, mock_proc):
        mock_proc.return_value = {"statusCode": 200}
        event = _make_msk_event({"op": "c", "after": {"person_info_key": "K1"}, "ts_ms": 1})
        lf.process_msk_event(event)
        _, _, payload, _ = mock_proc.call_args[0]
        assert "PERSON_INFO_KEY" in payload

    @patch("lambda_function.process_single_event")
    def test_bad_base64_returns_207(self, mock_proc):
        event = {
            "records": {
                "topic-0": [{"value": "NOT_VALID!!!", "topic": "glenraven-dev.STERLING.yfs_person_info", "offset": 1}]
            }
        }
        result = lf.process_msk_event(event)
        assert result["statusCode"] == 207

    @patch("lambda_function.process_single_event")
    def test_non_200_result_returns_207(self, mock_proc):
        mock_proc.return_value = {"statusCode": 400, "body": "bad"}
        event = _make_msk_event({"op": "c", "after": SAMPLE_PERSON_INFO_PAYLOAD, "ts_ms": 1})
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
        result = lf.lambda_handler(
            {"table": "YFS_PERSON_INFO", "operation": "c", "payload": SAMPLE_PERSON_INFO_PAYLOAD}, None
        )
        assert result["statusCode"] == 200

    @patch("lambda_function.process_msk_event")
    def test_msk_trigger(self, mock_msk):
        mock_msk.return_value = {"statusCode": 200}
        lf.lambda_handler({"records": {}}, None)
        mock_msk.assert_called_once()

    @patch("lambda_function.process_single_event")
    def test_unhandled_exception_returns_500(self, mock_proc):
        mock_proc.side_effect = RuntimeError("Boom")
        result = lf.lambda_handler(
            {"table": "YFS_PERSON_INFO", "operation": "c", "payload": {}}, None
        )
        assert result["statusCode"] == 500
        assert "Boom" in result["body"]


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
        mock_auth.assert_called_once_with(
            mock_session.return_value.get_credentials.return_value, lf.AWS_REGION, 'es'
        )

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
        mock_auth.assert_called_once_with(
            mock_session.return_value.get_credentials.return_value, lf.AWS_REGION, 'aoss'
        )

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
# _parse_debezium_message
# =============================================================================

class TestParseDebeziumMessage:
    def test_flattened_smt_format_detected(self):
        event = {
            "schema": {"type": "struct", "fields": []},
            "payload": {
                "PERSON_INFO_KEY": "PIK001",
                "__op": "r",
                "__source_ts_ms": 1773133137000
            }
        }
        op, ts, payload = lf._parse_debezium_message(event)
        assert op == "r"
        assert ts == 1773133137000
        assert "PERSON_INFO_KEY" in payload
        assert "__op" not in payload
        assert "__source_ts_ms" not in payload

    def test_standard_envelope_create(self):
        event = {"op": "c", "ts_ms": 1700000000000, "before": None, "after": {"person_info_key": "K1"}}
        op, ts, payload = lf._parse_debezium_message(event)
        assert op == "c"
        assert ts == 1700000000000
        assert "person_info_key" in payload

    def test_standard_envelope_delete_uses_before(self):
        event = {"op": "d", "ts_ms": 1700000000000, "before": {"person_info_key": "K1"}, "after": None}
        op, ts, payload = lf._parse_debezium_message(event)
        assert op == "d"
        assert "person_info_key" in payload

    def test_null_op_defaults_to_c(self):
        event = {"schema": {}, "payload": {"PERSON_INFO_KEY": "K1", "__op": None, "__source_ts_ms": 0}}
        op, _, _ = lf._parse_debezium_message(event)
        assert op == "c"

    def test_dunder_keys_stripped(self):
        event = {
            "schema": {},
            "payload": {"PERSON_INFO_KEY": "K1", "__op": "r", "__source_ts_ms": 1, "__table": "YFS_PERSON_INFO"}
        }
        _, _, payload = lf._parse_debezium_message(event)
        for k in payload:
            assert not k.startswith("__")


# =============================================================================
# Tombstone handling
# =============================================================================

class TestTombstoneRecord:
    @patch("lambda_function.process_single_event")
    def test_null_value_skipped_not_counted_as_error(self, mock_proc):
        event = {
            "eventSource": "aws:kafka",
            "records": {
                "topic-0": [{"topic": "glenraven-dev.STERLING.yfs_person_info", "offset": 0, "value": None}]
            }
        }
        result = lf.process_msk_event(event)
        assert result["statusCode"] == 200
        assert "errors 0" in result["body"]
        mock_proc.assert_not_called()


# =============================================================================
# Real YFS_PERSON_INFO CDC event (flattened SMT format, op=r)
# =============================================================================

REAL_PERSON_INFO_CDC_EVENT = {
    "schema": {
        "type": "struct",
        "fields": [],
        "optional": False,
        "name": "glenraven-dev.STERLING.YFS_PERSON_INFO.Value"
    },
    "payload": {
        "PERSON_INFO_KEY": "202109202325543721990239",
        "PERSON_ID": "                                   ",
        "TITLE": " ",
        "FIRST_NAME": "Rosemary",
        "MIDDLE_NAME": " ",
        "LAST_NAME": "Snider",
        "SUFFIX": " ",
        "DEPARTMENT": " ",
        "COMPANY": " ",
        "JOB_TITLE": " ",
        "ADDRESS_LINE1": "5909 Luther Lane",
        "ADDRESS_LINE2": "apt. 1004",
        "ADDRESS_LINE3": " ",
        "ADDRESS_LINE4": " ",
        "ADDRESS_LINE5": " ",
        "ADDRESS_LINE6": " ",
        "CITY": "Dallas                             ",
        "STATE": "TX                                 ",
        "ZIP_CODE": "75225",
        "COUNTRY": "US",
        "DAY_PHONE": "214-215-0252                            ",
        "EVENING_PHONE": "                                        ",
        "MOBILE_PHONE": "                                        ",
        "BEEPER": "214-215-0252                            ",
        "OTHER_PHONE": "                                        ",
        "DAY_FAX_NO": "                                        ",
        "EVENING_FAX_NO": "                                        ",
        "EMAILID": " ",
        "ALTERNATE_EMAILID": " ",
        "PREFERRED_SHIP_ADDRESS": " ",
        "HTTP_URL": " ",
        "USE_COUNT": 0.0,
        "VERIFICATION_STATUS": "               ",
        "IS_ADDRESS_VERIFIED": None,
        "LATITUDE": None,
        "LONGITUDE": None,
        "TAX_GEO_CODE": None,
        "ERROR_TXT": " ",
        "IS_COMMERCIAL_ADDRESS": "N",
        "TIME_ZONE": None,
        "LOCKID": 0.0,
        "CREATETS": 1632180353000,
        "MODIFYTS": 1632180353000,
        "CREATEUSERID": "WaveAgentServer",
        "MODIFYUSERID": "WaveAgentServer",
        "CREATEPROGID": "CONSOLIDATE_TO_SHIPMENT",
        "MODIFYPROGID": "CONSOLIDATE_TO_SHIPMENT",
        "ADDRESS_ID": None,
        "SHORT_ZIP_CODE": "75225",
        "EXTN_IS_TAX_EXEMPTED": "Y",
        "EXTN_TAX_EXEMPTION_CODE": "NO TAX",
        "EXTN_TAX_EXEMPTION_CERTIFICATE": "EXPORT",
        "EXTN_TAX_EXEMPT_EXPIRY_DATE": 2587593600000,
        "__op": "r",
        "__table": "YFS_PERSON_INFO",
        "__source_ts_ms": 1773133137000
    }
}


def _make_msk_event_from_raw(raw_event: dict, topic: str = "glenraven-prod.STERLING.yfs_person_info") -> dict:
    """Wrap a raw CDC event dict as an MSK trigger (base64-encoded JSON value)."""
    encoded = base64.b64encode(json.dumps(raw_event).encode()).decode()
    return {
        "eventSource": "aws:kafka",
        "records": {
            f"{topic}-0": [{"topic": topic, "offset": 0, "value": encoded}]
        }
    }


class TestRealPersonInfoCdcEvent:
    """End-to-end tests using a real YFS_PERSON_INFO CDC event (flattened SMT, op=r)."""

    @pytest.fixture(autouse=True)
    def setup(self):
        # Mock OpenSearch for fan-out phases
        mock_os = MagicMock()
        mock_os.update_by_query.return_value = {"updated": 0}
        lf.opensearch_client = mock_os
        self.mock_os = mock_os

        # Mock DynamoDB cache table
        self.mock_table = MagicMock()
        self.mock_table.put_item.return_value = {}
        self.mock_table.delete_item.return_value = {}

        self.msk_event = _make_msk_event_from_raw(REAL_PERSON_INFO_CDC_EVENT)
        yield
        lf.opensearch_client = None

    def _run_and_get_cached_item(self) -> dict:
        with patch("lambda_function.cache_table", self.mock_table):
            lf.process_msk_event(self.msk_event)
        return self.mock_table.put_item.call_args[1]["Item"]

    def _run(self):
        with patch("lambda_function.cache_table", self.mock_table):
            return lf.process_msk_event(self.msk_event)

    def test_processes_successfully(self):
        result = self._run()
        assert result["statusCode"] == 200
        assert "Processed 1" in result["body"]
        assert "errors 0" in result["body"]

    # -- Phase 1: DynamoDB cache --

    def test_dynamo_write_called(self):
        self._run()
        self.mock_table.put_item.assert_called_once()

    def test_dynamo_pk_sk(self):
        item = self._run_and_get_cached_item()
        assert item["PK"] == "PERSON_INFO"
        assert item["SK"] == "202109202325543721990239"

    def test_first_name(self):
        item = self._run_and_get_cached_item()
        assert item["firstName"] == "Rosemary"

    def test_last_name(self):
        item = self._run_and_get_cached_item()
        assert item["lastName"] == "Snider"

    def test_full_name_no_middle_when_blank(self):
        # MIDDLE_NAME is " " → stripped to None → not included in fullName
        item = self._run_and_get_cached_item()
        assert item["fullName"] == "Rosemary Snider"

    def test_middle_name_excluded_when_blank(self):
        item = self._run_and_get_cached_item()
        assert "middleName" not in item

    def test_address_line1(self):
        item = self._run_and_get_cached_item()
        assert item["addressLine1"] == "5909 Luther Lane"

    def test_address_line2(self):
        item = self._run_and_get_cached_item()
        assert item["addressLine2"] == "apt. 1004"

    def test_address_line3_excluded_when_blank(self):
        item = self._run_and_get_cached_item()
        assert "addressLine3" not in item

    def test_city_stripped(self):
        item = self._run_and_get_cached_item()
        assert item["city"] == "Dallas"

    def test_state_stripped(self):
        item = self._run_and_get_cached_item()
        assert item["state"] == "TX"

    def test_zip_code(self):
        item = self._run_and_get_cached_item()
        assert item["zipCode"] == "75225"

    def test_country(self):
        item = self._run_and_get_cached_item()
        assert item["country"] == "US"

    def test_day_phone_stripped(self):
        item = self._run_and_get_cached_item()
        assert item["dayPhone"] == "214-215-0252"

    def test_email_excluded_when_blank(self):
        item = self._run_and_get_cached_item()
        assert "emailId" not in item

    def test_mobile_phone_excluded_when_blank(self):
        item = self._run_and_get_cached_item()
        assert "mobilePhone" not in item

    def test_company_excluded_when_blank(self):
        item = self._run_and_get_cached_item()
        assert "company" not in item

    def test_none_fields_excluded_from_cache(self):
        item = self._run_and_get_cached_item()
        for k, v in item.items():
            assert v is not None, f"Field '{k}' should not be None in cache item"

    # -- Phase 2: Fan-out to orders --

    def test_orders_fanout_called_for_all_three_roles(self):
        self._run()
        order_calls = [
            c for c in self.mock_os.update_by_query.call_args_list
            if c[1].get("index") == lf.INDEX_ORDERS
        ]
        assert len(order_calls) == 3

    def test_orders_fanout_address_first_name(self):
        self._run()
        order_calls = [
            c for c in self.mock_os.update_by_query.call_args_list
            if c[1].get("index") == lf.INDEX_ORDERS
        ]
        addr = order_calls[0][1]["body"]["script"]["params"]["address"]
        assert addr["firstName"] == "Rosemary"
        assert addr["lastName"] == "Snider"

    def test_orders_fanout_address_full_name(self):
        self._run()
        order_calls = [
            c for c in self.mock_os.update_by_query.call_args_list
            if c[1].get("index") == lf.INDEX_ORDERS
        ]
        addr = order_calls[0][1]["body"]["script"]["params"]["address"]
        assert addr["fullName"] == "Rosemary Snider"

    def test_orders_fanout_day_phone_stripped(self):
        self._run()
        order_calls = [
            c for c in self.mock_os.update_by_query.call_args_list
            if c[1].get("index") == lf.INDEX_ORDERS
        ]
        addr = order_calls[0][1]["body"]["script"]["params"]["address"]
        assert addr["dayPhone"] == "214-215-0252"

    def test_orders_fanout_city_stripped(self):
        self._run()
        order_calls = [
            c for c in self.mock_os.update_by_query.call_args_list
            if c[1].get("index") == lf.INDEX_ORDERS
        ]
        addr = order_calls[0][1]["body"]["script"]["params"]["address"]
        assert addr["city"] == "Dallas"

    # -- Phase 4: Org contact — phone fallback uses dayPhone (no mobile) --

    def test_contact_phone_falls_back_to_day_phone(self):
        self._run()
        org_contact_calls = [
            c for c in self.mock_os.update_by_query.call_args_list
            if c[1].get("index") == lf.INDEX_ORGANIZATION
            and "contactAddressKey" in c[1]["body"]["query"].get("bool", {}).get("must", {}).get("term", {})
        ]
        assert len(org_contact_calls) == 1
        params = org_contact_calls[0][1]["body"]["script"]["params"]
        assert params["contact"]["phone"] == "214-215-0252"

    def test_contact_name_no_middle(self):
        self._run()
        org_contact_calls = [
            c for c in self.mock_os.update_by_query.call_args_list
            if c[1].get("index") == lf.INDEX_ORGANIZATION
            and "contactAddressKey" in c[1]["body"]["query"].get("bool", {}).get("must", {}).get("term", {})
        ]
        params = org_contact_calls[0][1]["body"]["script"]["params"]
        assert params["contact"]["fullName"] == "Rosemary Snider"

    def test_contact_email_none_when_blank(self):
        self._run()
        org_contact_calls = [
            c for c in self.mock_os.update_by_query.call_args_list
            if c[1].get("index") == lf.INDEX_ORGANIZATION
            and "contactAddressKey" in c[1]["body"]["query"].get("bool", {}).get("must", {}).get("term", {})
        ]
        params = org_contact_calls[0][1]["body"]["script"]["params"]
        # blank EMAILID → None → excluded from nested contact object
        assert "email" not in params["contact"]

    # -- cdc metadata --

    def test_cdc_operation_is_read_snapshot(self):
        # op "r" comes through correctly from flattened SMT
        op, ts, _ = lf._parse_debezium_message(REAL_PERSON_INFO_CDC_EVENT)
        assert op == "r"

    def test_cdc_timestamp_from_source_ts_ms(self):
        op, ts, _ = lf._parse_debezium_message(REAL_PERSON_INFO_CDC_EVENT)
        assert ts == 1773133137000


# =============================================================================
# Soft Delete — cdc-personinfo-processor
# =============================================================================

class TestSoftDeletePersonInfo:
    """
    Tests for the ENABLE_HARD_DELETE toggle on the personinfo lambda.

    Soft delete (default, ENABLE_HARD_DELETE=false):
      - DynamoDB PERSON_INFO row is marked with _deleted=True via update_item
      - delete_item is NOT called

    Hard delete (ENABLE_HARD_DELETE=true):
      - DynamoDB row is physically removed via delete_item
      - update_item is NOT called for the delete
    """

    # ------------------------------------------------------------------
    # Soft delete (default)
    # ------------------------------------------------------------------

    @patch("lambda_function.update_org_address_roles")
    @patch("lambda_function.update_org_contact")
    @patch("lambda_function.update_org_corporate_address")
    @patch("lambda_function.update_orders_with_person_info")
    @patch("lambda_function.cache_table")
    def test_soft_delete_calls_update_item_not_delete_item(
        self, mock_table, mock_orders, mock_corp, mock_contact, mock_roles
    ):
        with patch.object(lf, "ENABLE_HARD_DELETE", False):
            result = lf.process_person_info(SAMPLE_PERSON_INFO_PAYLOAD, "d")
        assert result["statusCode"] == 200
        mock_table.update_item.assert_called_once()
        mock_table.delete_item.assert_not_called()

    @patch("lambda_function.update_org_address_roles")
    @patch("lambda_function.update_org_contact")
    @patch("lambda_function.update_org_corporate_address")
    @patch("lambda_function.update_orders_with_person_info")
    @patch("lambda_function.cache_table")
    def test_soft_delete_marks_correct_pk_sk(
        self, mock_table, mock_orders, mock_corp, mock_contact, mock_roles
    ):
        with patch.object(lf, "ENABLE_HARD_DELETE", False):
            lf.process_person_info(SAMPLE_PERSON_INFO_PAYLOAD, "d")
        call_kwargs = mock_table.update_item.call_args[1]
        assert call_kwargs["Key"]["PK"] == "PERSON_INFO"
        assert call_kwargs["Key"]["SK"] == "PIK001"

    @patch("lambda_function.update_org_address_roles")
    @patch("lambda_function.update_org_contact")
    @patch("lambda_function.update_org_corporate_address")
    @patch("lambda_function.update_orders_with_person_info")
    @patch("lambda_function.cache_table")
    def test_soft_delete_sets_deleted_true(
        self, mock_table, mock_orders, mock_corp, mock_contact, mock_roles
    ):
        with patch.object(lf, "ENABLE_HARD_DELETE", False):
            lf.process_person_info(SAMPLE_PERSON_INFO_PAYLOAD, "d")
        expr_vals = mock_table.update_item.call_args[1]["ExpressionAttributeValues"]
        assert any(v is True for v in expr_vals.values()), \
            f"Expected _deleted=True in ExpressionAttributeValues: {expr_vals}"

    # ------------------------------------------------------------------
    # Hard delete
    # ------------------------------------------------------------------

    @patch("lambda_function.update_org_address_roles")
    @patch("lambda_function.update_org_contact")
    @patch("lambda_function.update_org_corporate_address")
    @patch("lambda_function.update_orders_with_person_info")
    @patch("lambda_function.cache_table")
    def test_hard_delete_calls_delete_item_not_update_item(
        self, mock_table, mock_orders, mock_corp, mock_contact, mock_roles
    ):
        with patch.object(lf, "ENABLE_HARD_DELETE", True):
            result = lf.process_person_info(SAMPLE_PERSON_INFO_PAYLOAD, "d")
        assert result["statusCode"] == 200
        mock_table.delete_item.assert_called_once()
        mock_table.update_item.assert_not_called()

    @patch("lambda_function.update_org_address_roles")
    @patch("lambda_function.update_org_contact")
    @patch("lambda_function.update_org_corporate_address")
    @patch("lambda_function.update_orders_with_person_info")
    @patch("lambda_function.cache_table")
    def test_hard_delete_removes_correct_pk_sk(
        self, mock_table, mock_orders, mock_corp, mock_contact, mock_roles
    ):
        with patch.object(lf, "ENABLE_HARD_DELETE", True):
            lf.process_person_info(SAMPLE_PERSON_INFO_PAYLOAD, "d")
        call_kwargs = mock_table.delete_item.call_args[1]
        assert call_kwargs["Key"]["PK"] == "PERSON_INFO"
        assert call_kwargs["Key"]["SK"] == "PIK001"
