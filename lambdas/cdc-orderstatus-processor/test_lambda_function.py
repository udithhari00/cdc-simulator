"""
Unit tests for order-status-update/lambda_function.py

Run with:
    pytest test_lambda_function.py -v --cov=lambda_function --cov-report=term-missing
"""

import base64
import json
import os
from unittest.mock import MagicMock, patch

import pytest

# Patch AWS clients before module import so no real connections are attempted
os.environ.setdefault("OPENSEARCH_HOST", "localhost")
os.environ.setdefault("OPENSEARCH_AUTH", "basic")
os.environ.setdefault("AWS_REGION", "us-east-1")
os.environ.setdefault("DYNAMODB_ENDPOINT", "http://localhost:8000")

with patch("boto3.resource"), patch("boto3.Session"):
    import lambda_function as lf


# =============================================================================
# Helpers
# =============================================================================

VALID_ENRICH = {
    "orderlinekey": [
        {"OrderLineKey": "LINE-001", "Status": "Created", "StatusCode": "1100"},
        {"OrderLineKey": "LINE-002", "Status": "Shipped", "StatusCode": "3700"},
    ]
}

VALID_PAYLOAD = {
    "orderHeaderKey": "OHK-001",
    "statusName":     "Shipped",
    "enrich":         VALID_ENRICH,
}

VALID_PAYLOAD_WITH_STATUS_CODE = {
    **VALID_PAYLOAD,
    "statusCode": "3700",
}

VALID_BODY = {
    "operation": "u",
    "payload":   VALID_PAYLOAD,
}

VALID_BODY_WITH_STATUS_CODE = {
    "operation": "u",
    "payload":   VALID_PAYLOAD_WITH_STATUS_CODE,
}


def make_event(body_dict: dict, base64_encode: bool = False) -> dict:
    raw = json.dumps(body_dict)
    if base64_encode:
        return {"body": base64.b64encode(raw.encode()).decode(), "isBase64Encoded": True}
    return {"body": raw}


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture(autouse=True)
def reset_os_client():
    lf.opensearch_client = None
    yield
    lf.opensearch_client = None


@pytest.fixture
def mock_os_client():
    client = MagicMock()
    client.update.return_value = {"result": "updated"}
    lf.opensearch_client = client
    return client


@pytest.fixture
def mock_cache_table():
    table = MagicMock()
    table.put_item.return_value = {}
    original = lf.cache_table
    lf.cache_table = table
    yield table
    lf.cache_table = original


# =============================================================================
# parse_body
# =============================================================================

class TestParseBody:
    def test_missing_body_returns_none(self):
        assert lf.parse_body({}) is None

    def test_none_body_returns_none(self):
        assert lf.parse_body({"body": None}) is None

    def test_dict_body_returned_as_is(self):
        payload = {"orderHeaderKey": "OHK-1"}
        assert lf.parse_body({"body": payload}) == payload

    def test_json_string_body(self):
        payload = {"orderHeaderKey": "OHK-1"}
        assert lf.parse_body({"body": json.dumps(payload)}) == payload

    def test_base64_encoded_body(self):
        payload = {"orderHeaderKey": "OHK-1"}
        encoded = base64.b64encode(json.dumps(payload).encode()).decode()
        assert lf.parse_body({"body": encoded, "isBase64Encoded": True}) == payload

    def test_invalid_json_returns_none(self):
        assert lf.parse_body({"body": "{bad json"}) is None

    def test_invalid_base64_returns_none(self):
        assert lf.parse_body({"body": "!!!notbase64!!!", "isBase64Encoded": True}) is None

    def test_unsupported_type_returns_none(self):
        assert lf.parse_body({"body": 12345}) is None


# =============================================================================
# save_order_status
# =============================================================================

class TestSaveOrderStatus:
    def test_puts_item_with_correct_keys(self, mock_cache_table):
        lf.save_order_status("OHK-001", "Shipped", VALID_ENRICH)
        call_kwargs = mock_cache_table.put_item.call_args[1]
        item = call_kwargs["Item"]
        assert item["PK"] == "ORDER_STATUS"
        assert item["SK"] == "OHK-001"
        assert item["statusName"] == "Shipped"
        assert item["enrich"] == VALID_ENRICH
        assert "statusCode" not in item
        assert "updatedAt" in item

    def test_puts_item_with_optional_status_code(self, mock_cache_table):
        lf.save_order_status("OHK-001", "Shipped", VALID_ENRICH, status_code="3700")
        call_kwargs = mock_cache_table.put_item.call_args[1]
        item = call_kwargs["Item"]
        assert item["statusCode"] == "3700"
        assert item["enrich"] == VALID_ENRICH

    def test_dynamodb_error_propagates(self, mock_cache_table):
        mock_cache_table.put_item.side_effect = Exception("DynamoDB unavailable")
        with pytest.raises(Exception, match="DynamoDB unavailable"):
            lf.save_order_status("OHK-001", "Shipped", VALID_ENRICH)


# =============================================================================
# update_order_in_index
# =============================================================================

class TestUpdateOrderInIndex:
    def test_calls_update_with_correct_args(self, mock_os_client):
        lf.update_order_in_index("OHK-001", "Shipped")
        call_kwargs = mock_os_client.update.call_args[1]
        assert call_kwargs["index"] == lf.INDEX_ORDER
        assert call_kwargs["id"] == "OHK-001"
        doc = call_kwargs["body"]["doc"]
        assert doc["statusName"] == "Shipped"
        assert "enrich" not in doc
        assert "statusCode" not in doc

    def test_calls_update_with_optional_status_code(self, mock_os_client):
        lf.update_order_in_index("OHK-001", "Shipped", status_code="3700")
        doc = mock_os_client.update.call_args[1]["body"]["doc"]
        assert doc["statusCode"] == "3700"
        assert "enrich" not in doc

    def test_opensearch_error_is_logged_not_raised(self, mock_os_client):
        mock_os_client.update.side_effect = Exception("404 Not Found")
        # Should NOT raise — a missing order is logged as a warning
        lf.update_order_in_index("OHK-MISSING", "Shipped")


class TestUpdateOrderLinesInIndex:
    def test_updates_each_order_line(self, mock_os_client):
        lf.update_order_lines_in_index(VALID_ENRICH)
        assert mock_os_client.update.call_count == 2
        # Verify first call targets LINE-001 in order-lines index
        first_call = mock_os_client.update.call_args_list[0][1]
        assert first_call["index"] == lf.INDEX_ORDER_LINES
        assert first_call["id"] == "LINE-001"
        doc = first_call["body"]["doc"]
        assert doc["statusName"] == "Created"
        assert doc["statusCode"] == "1100"

    def test_skips_entry_missing_order_line_key(self, mock_os_client):
        enrich = {"orderlinekey": [{"Status": "Shipped", "StatusCode": "3700"}]}
        lf.update_order_lines_in_index(enrich)
        mock_os_client.update.assert_not_called()

    def test_skips_entry_missing_status_and_status_code(self, mock_os_client):
        enrich = {"orderlinekey": [{"OrderLineKey": "LINE-001"}]}
        lf.update_order_lines_in_index(enrich)
        mock_os_client.update.assert_not_called()

    def test_opensearch_error_is_logged_not_raised(self, mock_os_client):
        mock_os_client.update.side_effect = Exception("404 Not Found")
        # Should NOT raise — a missing order line is logged as a warning
        lf.update_order_lines_in_index(VALID_ENRICH)


# =============================================================================
# get_opensearch_client
# =============================================================================

class TestGetOpenSearchClient:
    def test_returns_cached_client(self, mock_os_client):
        result = lf.get_opensearch_client()
        assert result is mock_os_client

    @patch("lambda_function.OPENSEARCH_AUTH", "iam")
    @patch("lambda_function.boto3.Session")
    @patch("lambda_function.OpenSearch")
    @patch("lambda_function.AWSV4SignerAuth")
    def test_iam_creates_client(self, mock_auth, mock_os_cls, mock_session):
        lf.opensearch_client = None
        mock_session.return_value.get_credentials.return_value = MagicMock()
        mock_os_cls.return_value = MagicMock()
        client = lf.get_opensearch_client()
        assert client is not None
        mock_os_cls.assert_called_once()

    @patch("lambda_function.OPENSEARCH_AUTH", "iam")
    @patch("lambda_function.boto3.Session")
    def test_iam_no_credentials_raises(self, mock_session):
        lf.opensearch_client = None
        mock_session.return_value.get_credentials.return_value = None
        with pytest.raises(RuntimeError, match="AWS credentials not found"):
            lf.get_opensearch_client()

    @patch("lambda_function.OPENSEARCH_AUTH", "basic")
    @patch("lambda_function.OpenSearch")
    def test_basic_auth_creates_client(self, mock_os_cls):
        lf.opensearch_client = None
        mock_os_cls.return_value = MagicMock()
        client = lf.get_opensearch_client()
        assert client is not None


# =============================================================================
# lambda_handler — validation
# =============================================================================

class TestLambdaHandlerValidation:
    def test_missing_body_returns_400(self):
        result = lf.lambda_handler({}, None)
        assert result["statusCode"] == 400

    def test_invalid_json_body_returns_400(self):
        result = lf.lambda_handler({"body": "{bad"}, None)
        assert result["statusCode"] == 400

    def test_missing_payload_field_returns_400(self):
        result = lf.lambda_handler(make_event({"operation": "u"}), None)
        assert result["statusCode"] == 400
        assert "payload" in result["body"]

    def test_empty_payload_returns_400(self):
        result = lf.lambda_handler(make_event({"operation": "u", "payload": {}}), None)
        assert result["statusCode"] == 400

    def test_payload_not_dict_returns_400(self):
        result = lf.lambda_handler(make_event({"operation": "u", "payload": "bad"}), None)
        assert result["statusCode"] == 400

    def test_missing_order_header_key_returns_400(self):
        body = {"operation": "u", "payload": {"statusName": "Shipped", "enrich": VALID_ENRICH}}
        result = lf.lambda_handler(make_event(body), None)
        assert result["statusCode"] == 400
        assert "orderHeaderKey" in result["body"]

    def test_empty_order_header_key_returns_400(self):
        body = {"operation": "u", "payload": {"orderHeaderKey": "  ", "statusName": "Shipped", "enrich": VALID_ENRICH}}
        result = lf.lambda_handler(make_event(body), None)
        assert result["statusCode"] == 400

    def test_missing_status_name_returns_400(self):
        body = {"operation": "u", "payload": {"orderHeaderKey": "OHK-001", "enrich": VALID_ENRICH}}
        result = lf.lambda_handler(make_event(body), None)
        assert result["statusCode"] == 400
        assert "statusName" in result["body"]

    def test_enrich_not_dict_returns_400(self):
        body = {"operation": "u", "payload": {"orderHeaderKey": "OHK-001", "statusName": "Shipped", "enrich": "bad"}}
        result = lf.lambda_handler(make_event(body), None)
        assert result["statusCode"] == 400
        assert "enrich" in result["body"]

    def test_orderlinekey_not_list_returns_400(self):
        body = {"operation": "u", "payload": {"orderHeaderKey": "OHK-001", "statusName": "Shipped", "enrich": {"orderlinekey": "bad"}}}
        result = lf.lambda_handler(make_event(body), None)
        assert result["statusCode"] == 400
        assert "orderlinekey" in result["body"]

    def test_orderlinekey_entry_not_dict_returns_400(self):
        body = {"operation": "u", "payload": {"orderHeaderKey": "OHK-001", "statusName": "Shipped", "enrich": {"orderlinekey": ["bad"]}}}
        result = lf.lambda_handler(make_event(body), None)
        assert result["statusCode"] == 400
        assert "must be an object" in result["body"]

    def test_orderlinekey_entry_missing_order_line_key_returns_400(self):
        body = {"operation": "u", "payload": {"orderHeaderKey": "OHK-001", "statusName": "Shipped", "enrich": {"orderlinekey": [{"Status": "Created"}]}}}
        result = lf.lambda_handler(make_event(body), None)
        assert result["statusCode"] == 400
        assert "OrderLineKey" in result["body"]

    def test_orderlinekey_entry_missing_status_returns_400(self):
        body = {"operation": "u", "payload": {"orderHeaderKey": "OHK-001", "statusName": "Shipped", "enrich": {"orderlinekey": [{"OrderLineKey": "LINE-001"}]}}}
        result = lf.lambda_handler(make_event(body), None)
        assert result["statusCode"] == 400
        assert "Status" in result["body"]

    def test_status_code_is_optional(self, mock_os_client, mock_cache_table):
        """statusCode is not required — request should succeed without it."""
        body = {"operation": "u", "payload": {"orderHeaderKey": "OHK-001", "statusName": "Shipped", "enrich": VALID_ENRICH}}
        result = lf.lambda_handler(make_event(body), None)
        assert result["statusCode"] == 200


# =============================================================================
# lambda_handler — success path
# =============================================================================

class TestLambdaHandlerSuccess:
    def test_valid_request_returns_200(self, mock_os_client, mock_cache_table):
        result = lf.lambda_handler(make_event(VALID_BODY), None)
        assert result["statusCode"] == 200

    def test_missing_enrich_succeeds(self, mock_os_client, mock_cache_table):
        body = {"operation": "u", "payload": {"orderHeaderKey": "OHK-001", "statusName": "Shipped"}}
        result = lf.lambda_handler(make_event(body), None)
        assert result["statusCode"] == 200

    def test_empty_orderlinekey_succeeds(self, mock_os_client, mock_cache_table):
        body = {"operation": "u", "payload": {"orderHeaderKey": "OHK-001", "statusName": "Shipped", "enrich": {"orderlinekey": []}}}
        result = lf.lambda_handler(make_event(body), None)
        assert result["statusCode"] == 200

    def test_missing_orderlinekey_in_enrich_succeeds(self, mock_os_client, mock_cache_table):
        body = {"operation": "u", "payload": {"orderHeaderKey": "OHK-001", "statusName": "Shipped", "enrich": {}}}
        result = lf.lambda_handler(make_event(body), None)
        assert result["statusCode"] == 200

    def test_response_body_contains_required_fields(self, mock_os_client, mock_cache_table):
        result = lf.lambda_handler(make_event(VALID_BODY), None)
        body = json.loads(result["body"])
        assert body["orderHeaderKey"] == "OHK-001"
        assert body["statusName"] == "Shipped"
        assert body["enrich"] == VALID_ENRICH
        assert "statusCode" not in body  # not sent in VALID_BODY

    def test_response_includes_status_code_when_provided(self, mock_os_client, mock_cache_table):
        result = lf.lambda_handler(make_event(VALID_BODY_WITH_STATUS_CODE), None)
        body = json.loads(result["body"])
        assert body["statusCode"] == "3700"
        assert body["enrich"] == VALID_ENRICH

    def test_dynamo_put_called(self, mock_os_client, mock_cache_table):
        lf.lambda_handler(make_event(VALID_BODY), None)
        mock_cache_table.put_item.assert_called_once()

    def test_opensearch_update_called(self, mock_os_client, mock_cache_table):
        lf.lambda_handler(make_event(VALID_BODY), None)
        # Called 3 times: 1 for orders index + 2 for order-lines (one per line)
        assert mock_os_client.update.call_count == 3

    def test_base64_encoded_body_accepted(self, mock_os_client, mock_cache_table):
        result = lf.lambda_handler(make_event(VALID_BODY, base64_encode=True), None)
        assert result["statusCode"] == 200

    def test_dict_body_accepted(self, mock_os_client, mock_cache_table):
        result = lf.lambda_handler({"body": VALID_BODY}, None)
        assert result["statusCode"] == 200

    def test_operation_field_is_optional(self, mock_os_client, mock_cache_table):
        result = lf.lambda_handler(make_event({"payload": VALID_PAYLOAD}), None)
        assert result["statusCode"] == 200


# =============================================================================
# lambda_handler — delete path
# =============================================================================

class TestLambdaHandlerDelete:
    def _make_delete_event(self):
        return make_event({"operation": "d", "payload": {"orderHeaderKey": "OHK-001"}})

    def test_delete_returns_200(self, mock_os_client, mock_cache_table):
        result = lf.lambda_handler(self._make_delete_event(), None)
        assert result["statusCode"] == 200

    def test_delete_response_contains_deleted_flag(self, mock_os_client, mock_cache_table):
        result = lf.lambda_handler(self._make_delete_event(), None)
        body = json.loads(result["body"])
        assert body["deleted"] is True
        assert body["orderHeaderKey"] == "OHK-001"

    def test_delete_calls_dynamo_delete(self, mock_os_client, mock_cache_table):
        lf.lambda_handler(self._make_delete_event(), None)
        mock_cache_table.delete_item.assert_called_once_with(
            Key={"PK": "ORDER_STATUS", "SK": "OHK-001"}
        )

    def test_delete_clears_status_in_opensearch(self, mock_os_client, mock_cache_table):
        lf.lambda_handler(self._make_delete_event(), None)
        call_kwargs = mock_os_client.update.call_args[1]
        assert call_kwargs["body"]["doc"]["statusCode"] is None
        assert call_kwargs["body"]["doc"]["statusName"] is None
        assert "enrich" not in call_kwargs["body"]["doc"]

    def test_delete_opensearch_error_does_not_fail_request(self, mock_os_client, mock_cache_table):
        mock_os_client.update.side_effect = Exception("404 Not Found")
        result = lf.lambda_handler(self._make_delete_event(), None)
        assert result["statusCode"] == 200
        mock_cache_table.delete_item.assert_called_once()

    def test_delete_does_not_require_status_fields(self, mock_os_client, mock_cache_table):
        """statusCode and statusName are not validated on a delete."""
        event = make_event({"operation": "d", "payload": {"orderHeaderKey": "OHK-001"}})
        result = lf.lambda_handler(event, None)
        assert result["statusCode"] == 200

    def test_opensearch_404_does_not_fail_request(self, mock_os_client, mock_cache_table):
        """Status is cached in DynamoDB even if the order doc doesn't exist yet in OS."""
        mock_os_client.update.side_effect = Exception("document_missing_exception")
        result = lf.lambda_handler(make_event(VALID_BODY), None)
        assert result["statusCode"] == 200
        mock_cache_table.put_item.assert_called_once()

    def test_dynamo_failure_returns_500(self, mock_os_client, mock_cache_table):
        mock_cache_table.put_item.side_effect = Exception("ProvisionedThroughputExceededException")
        result = lf.lambda_handler(make_event(VALID_BODY), None)
        assert result["statusCode"] == 500


# =============================================================================
# now_ms
# =============================================================================

class TestNowMs:
    def test_returns_int(self):
        assert isinstance(lf.now_ms(), int)

    def test_is_recent(self):
        assert lf.now_ms() > 1_700_000_000_000


# =============================================================================
# Soft Delete — order-status-update
# =============================================================================

class TestSoftDeleteOrderStatus:
    """
    Tests for ENABLE_HARD_DELETE toggle on the order-status-update lambda.

    Soft delete (default, ENABLE_HARD_DELETE=false):
      - DynamoDB ORDER_STATUS row is marked with _deleted=True via update_item
      - delete_item is NOT called on DynamoDB
      - OpenSearch still has statusCode/statusName nulled out (same either way)

    Hard delete (ENABLE_HARD_DELETE=true):
      - DynamoDB row is physically removed via delete_item
      - update_item is NOT called for the delete path
    """

    @pytest.fixture(autouse=True)
    def setup(self, mock_os_client, mock_cache_table):
        self.os = mock_os_client
        self.table = mock_cache_table

    def _delete_event(self):
        return make_event({"operation": "d", "payload": {"orderHeaderKey": "OHK-001"}})

    # ------------------------------------------------------------------
    # Soft delete (default)
    # ------------------------------------------------------------------

    def test_soft_delete_calls_update_item_not_delete_item(self):
        with patch.dict("os.environ", {"ENABLE_HARD_DELETE": "false"}):
            result = lf.lambda_handler(self._delete_event(), None)
        assert result["statusCode"] == 200
        self.table.update_item.assert_called_once()
        self.table.delete_item.assert_not_called()

    def test_soft_delete_marks_correct_pk_sk(self):
        with patch.dict("os.environ", {"ENABLE_HARD_DELETE": "false"}):
            lf.lambda_handler(self._delete_event(), None)
        call_kwargs = self.table.update_item.call_args[1]
        assert call_kwargs["Key"]["PK"] == "ORDER_STATUS"
        assert call_kwargs["Key"]["SK"] == "OHK-001"

    def test_soft_delete_sets_deleted_true(self):
        with patch.dict("os.environ", {"ENABLE_HARD_DELETE": "false"}):
            lf.lambda_handler(self._delete_event(), None)
        expr_vals = self.table.update_item.call_args[1]["ExpressionAttributeValues"]
        assert any(v is True for v in expr_vals.values()), \
            f"Expected _deleted=True in values: {expr_vals}"

    def test_soft_delete_opensearch_still_clears_status_fields(self):
        """OpenSearch null-out is independent of soft/hard toggle."""
        with patch.dict("os.environ", {"ENABLE_HARD_DELETE": "false"}):
            lf.lambda_handler(self._delete_event(), None)
        call_kwargs = self.os.update.call_args[1]
        doc = call_kwargs["body"]["doc"]
        assert doc["statusCode"] is None
        assert doc["statusName"] is None
        assert "enrich" not in doc

    # ------------------------------------------------------------------
    # Hard delete
    # ------------------------------------------------------------------

    def test_hard_delete_calls_delete_item_not_update_item(self):
        with patch.dict("os.environ", {"ENABLE_HARD_DELETE": "true"}):
            result = lf.lambda_handler(self._delete_event(), None)
        assert result["statusCode"] == 200
        self.table.delete_item.assert_called_once_with(
            Key={"PK": "ORDER_STATUS", "SK": "OHK-001"}
        )
        self.table.update_item.assert_not_called()

    def test_hard_delete_opensearch_still_clears_status_fields(self):
        with patch.dict("os.environ", {"ENABLE_HARD_DELETE": "true"}):
            lf.lambda_handler(self._delete_event(), None)
        doc = self.os.update.call_args[1]["body"]["doc"]
        assert doc["statusCode"] is None
        assert doc["statusName"] is None
        assert "enrich" not in doc
