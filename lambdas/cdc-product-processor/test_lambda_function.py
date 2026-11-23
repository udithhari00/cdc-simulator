"""
Pytest test suite for lambda_function.py
Target coverage: >= 90%

Run with:
    pytest test_lambda_function.py -v --cov=lambda_function --cov-report=term-missing
"""

import base64
import importlib
import json
import sys
from unittest.mock import MagicMock, patch, PropertyMock
import pytest
# ---------------------------------------------------------------------------
# Helpers to reload the module with a clean singleton between tests
# ---------------------------------------------------------------------------

def reload_lambda(env_overrides: dict = None):
    """
    Force-reload lambda_function so that:
      1. Module-level env vars are re-evaluated.
      2. The opensearch_client singleton is reset to None.
    """
    env_overrides = env_overrides or {}
    with patch.dict("os.environ", env_overrides, clear=False):
        if "lambda_function" in sys.modules:
            del sys.modules["lambda_function"]
        import lambda_function as lf
    return lf


# ===========================================================================
# Fixtures
# ===========================================================================

@pytest.fixture(autouse=True)
def reset_singleton():
    """Reset the global opensearch_client singleton before every test."""
    import lambda_function as lf
    lf.opensearch_client = None
    yield
    lf.opensearch_client = None


@pytest.fixture()
def mock_os_client():
    """Return a MagicMock that pretends to be an OpenSearch client."""
    client = MagicMock()
    client.update.return_value = {"result": "updated"}
    client.delete.return_value = {"result": "deleted"}
    return client


def _make_function_url_event(body_dict, time_epoch=1583348638390):
    """Helper to build a Function URL style event with requestContext."""
    return {
        "body": json.dumps(body_dict),
        "requestContext": {
            "timeEpoch": time_epoch,
        },
    }


@pytest.fixture()
def valid_upsert_event():
    """A well-formed upsert event delivered via Function URL."""
    body = {
        "operation": "u",
        "payload": {
            "uniqueID": "prod-001",
            "name": "Widget A",
            "price": 9.99,
        },
    }
    return _make_function_url_event(body, time_epoch=1700000000000)


@pytest.fixture()
def valid_delete_event():
    body = {
        "operation": "d",
        "payload": {"uniqueID": "prod-001"},
    }
    return _make_function_url_event(body)


# ===========================================================================
# 1. parse_body
# ===========================================================================

class TestParseBody:
    def test_no_body_key_returns_none(self):
        import lambda_function as lf
        assert lf.parse_body({}) is None

    def test_body_is_none_returns_none(self):
        import lambda_function as lf
        assert lf.parse_body({"body": None}) is None

    def test_body_already_dict(self):
        import lambda_function as lf
        payload = {"operation": "u", "payload": {}}
        assert lf.parse_body({"body": payload}) == payload

    def test_body_is_json_string(self):
        import lambda_function as lf
        payload = {"key": "value"}
        assert lf.parse_body({"body": json.dumps(payload)}) == payload

    def test_body_is_base64_encoded_json(self):
        import lambda_function as lf
        payload = {"hello": "world"}
        encoded = base64.b64encode(json.dumps(payload).encode()).decode()
        event = {"body": encoded, "isBase64Encoded": True}
        assert lf.parse_body(event) == payload

    def test_body_is_invalid_base64_returns_none(self):
        import lambda_function as lf
        event = {"body": "!!!not-base64!!!", "isBase64Encoded": True}
        assert lf.parse_body(event) is None

    def test_body_is_invalid_json_string_returns_none(self):
        import lambda_function as lf
        assert lf.parse_body({"body": "{bad json"}) is None

    def test_body_is_unsupported_type_returns_none(self):
        import lambda_function as lf
        # integer body – not a dict, not a string
        assert lf.parse_body({"body": 12345}) is None

    def test_base64_decodes_to_invalid_json_returns_none(self):
        import lambda_function as lf
        bad_json = base64.b64encode(b"not json at all").decode()
        event = {"body": bad_json, "isBase64Encoded": True}
        assert lf.parse_body(event) is None


# ===========================================================================
# 2. get_opensearch_client — IAM branch
# ===========================================================================

class TestGetOpenSearchClientIAM:
    def test_returns_cached_client_on_second_call(self, mock_os_client):
        import lambda_function as lf
        lf.opensearch_client = mock_os_client
        result = lf.get_opensearch_client()
        assert result is mock_os_client

    @patch("lambda_function.OPENSEARCH_AUTH", "iam")
    @patch("lambda_function.boto3.Session")
    @patch("lambda_function.OpenSearch")
    @patch("lambda_function.AWSV4SignerAuth")
    def test_iam_auth_creates_client(self, mock_auth_cls, mock_os_cls, mock_session):
        import lambda_function as lf
        lf.opensearch_client = None

        mock_creds = MagicMock()
        mock_session.return_value.get_credentials.return_value = mock_creds
        mock_auth_cls.return_value = MagicMock()
        mock_os_cls.return_value = MagicMock()

        client = lf.get_opensearch_client()
        assert client is not None
        mock_os_cls.assert_called_once()

    @patch("lambda_function.OPENSEARCH_AUTH", "iam")
    @patch("lambda_function.boto3.Session")
    def test_iam_no_credentials_raises(self, mock_session):
        import lambda_function as lf
        lf.opensearch_client = None
        mock_session.return_value.get_credentials.return_value = None

        with pytest.raises(RuntimeError, match="AWS credentials not found"):
            lf.get_opensearch_client()


# ===========================================================================
# 3. get_opensearch_client — Basic auth branch
# ===========================================================================

class TestGetOpenSearchClientBasic:
    @patch("lambda_function.OPENSEARCH_AUTH", "basic")
    @patch("lambda_function.OpenSearch")
    def test_basic_auth_creates_client(self, mock_os_cls):
        import lambda_function as lf
        lf.opensearch_client = None
        mock_os_cls.return_value = MagicMock()

        client = lf.get_opensearch_client()
        assert client is not None
        mock_os_cls.assert_called_once()
        call_kwargs = mock_os_cls.call_args[1]
        assert call_kwargs["verify_certs"] is False


# ===========================================================================
# 4. lambda_handler — validation
# ===========================================================================

class TestLambdaHandlerValidation:
    def test_missing_body_returns_400(self):
        import lambda_function as lf
        result = lf.lambda_handler({}, None)
        assert result["statusCode"] == 400
        assert "body" in result["body"].lower() or "missing" in result["body"].lower()

    def test_no_payload_field_returns_400(self):
        import lambda_function as lf
        event = {"body": json.dumps({"operation": "u"})}
        result = lf.lambda_handler(event, None)
        assert result["statusCode"] == 400
        assert "payload" in result["body"]

    def test_payload_not_dict_returns_400(self):
        import lambda_function as lf
        event = {"body": json.dumps({"payload": "not-a-dict"})}
        result = lf.lambda_handler(event, None)
        assert result["statusCode"] == 400

    def test_empty_payload_dict_returns_400(self):
        import lambda_function as lf
        event = {"body": json.dumps({"payload": {}})}
        result = lf.lambda_handler(event, None)
        assert result["statusCode"] == 400

    def test_missing_unique_id_returns_400(self):
        import lambda_function as lf
        event = {"body": json.dumps({"payload": {"name": "Widget"}})}
        result = lf.lambda_handler(event, None)
        assert result["statusCode"] == 400
        assert "uniqueID" in result["body"]

    def test_unparseable_body_returns_400(self):
        import lambda_function as lf
        result = lf.lambda_handler({"body": "{bad json"}, None)
        assert result["statusCode"] == 400


# ===========================================================================
# 5. lambda_handler — upsert (insert / update)
# ===========================================================================

class TestLambdaHandlerUpsert:
    def _make_event(self, operation="u", time_epoch=1700000000000):
        body = {
            "operation": operation,
            "payload": {"uniqueID": "prod-123", "name": "Widget"},
        }
        return _make_function_url_event(body, time_epoch=time_epoch)

    @patch("lambda_function.get_opensearch_client")
    def test_upsert_returns_200(self, mock_get_client, mock_os_client):
        import lambda_function as lf
        mock_get_client.return_value = mock_os_client
        result = lf.lambda_handler(self._make_event("u"), None)
        assert result["statusCode"] == 200
        assert "Upserted" in result["body"]

    @patch("lambda_function.get_opensearch_client")
    def test_upsert_calls_update_with_correct_index(self, mock_get_client, mock_os_client):
        import lambda_function as lf
        mock_get_client.return_value = mock_os_client
        lf.lambda_handler(self._make_event("u"), None)
        call_kwargs = mock_os_client.update.call_args[1]
        assert call_kwargs["index"] == lf.INDEX_PRODUCTS
        assert call_kwargs["id"] == "prod-123"

    @patch("lambda_function.get_opensearch_client")
    def test_upsert_enriches_payload_fields(self, mock_get_client, mock_os_client):
        import lambda_function as lf
        mock_get_client.return_value = mock_os_client
        lf.lambda_handler(self._make_event("u", time_epoch=1700000000000), None)
        doc = mock_os_client.update.call_args[1]["body"]["doc"]
        assert doc["cdcOperation"] == "u"
        assert doc["cdcTimestamp"] == 1700000000000
        assert "indexedAt" in doc

    @patch("lambda_function.get_opensearch_client")
    def test_upsert_uses_now_ms_when_no_request_context(self, mock_get_client, mock_os_client):
        import lambda_function as lf
        mock_get_client.return_value = mock_os_client
        # Event without requestContext — falls back to now_ms()
        event = {"body": json.dumps({"operation": "u", "payload": {"uniqueID": "p1"}})}
        lf.lambda_handler(event, None)
        doc = mock_os_client.update.call_args[1]["body"]["doc"]
        assert isinstance(doc["cdcTimestamp"], int)

    @patch("lambda_function.get_opensearch_client")
    def test_upsert_opensearch_error_returns_500(self, mock_get_client, mock_os_client):
        import lambda_function as lf
        mock_os_client.update.side_effect = Exception("Connection refused")
        mock_get_client.return_value = mock_os_client
        result = lf.lambda_handler(self._make_event("u"), None)
        assert result["statusCode"] == 500
        assert "Upsert failed" in result["body"]

    @patch("lambda_function.get_opensearch_client")
    def test_insert_operation_also_upserts(self, mock_get_client, mock_os_client):
        """Any operation that is not 'd' should go through upsert path."""
        import lambda_function as lf
        mock_get_client.return_value = mock_os_client
        result = lf.lambda_handler(self._make_event("i"), None)
        assert result["statusCode"] == 200
        mock_os_client.update.assert_called_once()


# ===========================================================================
# 6. lambda_handler — delete
# ===========================================================================

class TestLambdaHandlerDelete:
    def _make_delete_event(self):
        body = {
            "operation": "d",
            "payload": {"uniqueID": "prod-999"},
        }
        return _make_function_url_event(body)

    @patch("lambda_function.get_opensearch_client")
    def test_delete_returns_200(self, mock_get_client, mock_os_client):
        import lambda_function as lf
        mock_get_client.return_value = mock_os_client
        result = lf.lambda_handler(self._make_delete_event(), None)
        assert result["statusCode"] == 200
        assert "Deleted" in result["body"]

    @patch("lambda_function.get_opensearch_client")
    def test_delete_calls_client_delete(self, mock_get_client, mock_os_client):
        import lambda_function as lf
        mock_get_client.return_value = mock_os_client
        lf.lambda_handler(self._make_delete_event(), None)
        mock_os_client.delete.assert_called_once_with(
            index=lf.INDEX_PRODUCTS, id="prod-999", ignore=[404]
        )

    @patch("lambda_function.get_opensearch_client")
    def test_delete_not_found_returns_200_with_warning(self, mock_get_client, mock_os_client):
        """Deleting a non-existent doc still returns 200 but with a 'not found' message."""
        import lambda_function as lf
        mock_os_client.delete.return_value = {"result": "not_found"}
        mock_get_client.return_value = mock_os_client
        result = lf.lambda_handler(self._make_delete_event(), None)
        assert result["statusCode"] == 200
        assert "not found" in result["body"]

    @patch("lambda_function.get_opensearch_client")
    def test_delete_opensearch_error_returns_500(self, mock_get_client, mock_os_client):
        import lambda_function as lf
        mock_os_client.delete.side_effect = Exception("Timeout")
        mock_get_client.return_value = mock_os_client
        result = lf.lambda_handler(self._make_delete_event(), None)
        assert result["statusCode"] == 500
        assert "Delete failed" in result["body"]


# ===========================================================================
# 7. lambda_handler — integration-style tests
# ===========================================================================

class TestLambdaHandlerIntegration:
    @patch("lambda_function.get_opensearch_client")
    def test_valid_upsert_event_returns_200(self, mock_get_client, mock_os_client, valid_upsert_event):
        import lambda_function as lf
        mock_get_client.return_value = mock_os_client
        result = lf.lambda_handler(valid_upsert_event, None)
        assert result["statusCode"] == 200

    @patch("lambda_function.get_opensearch_client")
    def test_valid_delete_event_returns_200(self, mock_get_client, mock_os_client, valid_delete_event):
        import lambda_function as lf
        mock_get_client.return_value = mock_os_client
        result = lf.lambda_handler(valid_delete_event, None)
        assert result["statusCode"] == 200

    def test_empty_event_returns_400(self):
        import lambda_function as lf
        result = lf.lambda_handler({}, None)
        assert result["statusCode"] == 400

    def test_unrecognised_event_returns_400(self):
        import lambda_function as lf
        result = lf.lambda_handler({"someRandomKey": "value"}, None)
        assert result["statusCode"] == 400

    def test_unhandled_exception_returns_500(self):
        import lambda_function as lf
        # Patch parse_body to raise an unexpected error
        with patch.object(lf, "parse_body", side_effect=RuntimeError("boom")):
            event = {"body": json.dumps({"payload": {"uniqueID": "x"}})}
            result = lf.lambda_handler(event, None)
            assert result["statusCode"] == 500
            assert "boom" in result["body"]

    @patch("lambda_function.get_opensearch_client")
    def test_large_event_is_truncated_in_log(self, mock_get_client, mock_os_client, caplog):
        import lambda_function as lf
        mock_get_client.return_value = mock_os_client
        big_payload = {"uniqueID": "x", "data": "a" * 3000}
        event = _make_function_url_event({"operation": "u", "payload": big_payload})
        with caplog.at_level("INFO", logger=""):
            lf.lambda_handler(event, None)
        assert "truncated" in caplog.text

    @patch("lambda_function.get_opensearch_client")
    def test_base64_encoded_event_is_handled(self, mock_get_client, mock_os_client):
        import lambda_function as lf
        mock_get_client.return_value = mock_os_client
        body = {"operation": "u", "payload": {"uniqueID": "b64-1"}}
        encoded = base64.b64encode(json.dumps(body).encode()).decode()
        event = {"body": encoded, "isBase64Encoded": True}
        result = lf.lambda_handler(event, None)
        assert result["statusCode"] == 200

    @patch("lambda_function.get_opensearch_client")
    def test_dict_body_event_is_handled(self, mock_get_client, mock_os_client):
        import lambda_function as lf
        mock_get_client.return_value = mock_os_client
        event = {"body": {"operation": "u", "payload": {"uniqueID": "dict-1"}}}
        result = lf.lambda_handler(event, None)
        assert result["statusCode"] == 200


# ===========================================================================
# 8. now_ms
# ===========================================================================

class TestNowMs:
    def test_returns_integer(self):
        import lambda_function as lf
        assert isinstance(lf.now_ms(), int)

    def test_is_recent_timestamp(self):
        import lambda_function as lf
        ts = lf.now_ms()
        # Should be after 2024-01-01 in milliseconds
        assert ts > 1_700_000_000_000


# ===========================================================================
# 9. lambda_handler — Soft Delete
# ===========================================================================

class TestSoftDeleteProduct:
    """
    Tests for the ENABLE_HARD_DELETE toggle on the product lambda.

    Soft delete (default, ENABLE_HARD_DELETE=false):
      - OpenSearch document is updated with _deleted=True and _deletedAt (epoch ms)
      - client.delete() is NOT called

    Hard delete (ENABLE_HARD_DELETE=true):
      - client.delete() is called with the product uniqueID and ignore=[404]
      - client.update() is NOT called for the delete
    """

    def _make_delete_event(self):
        body = {"operation": "d", "payload": {"uniqueID": "prod-soft-001"}}
        return _make_function_url_event(body)

    # ------------------------------------------------------------------
    # Soft delete (default)
    # ------------------------------------------------------------------

    @patch("lambda_function.get_opensearch_client")
    def test_soft_delete_calls_update_not_delete(self, mock_get_client, mock_os_client):
        import lambda_function as lf
        mock_get_client.return_value = mock_os_client
        with patch.dict("os.environ", {"ENABLE_HARD_DELETE": "false"}):
            result = lf.lambda_handler(self._make_delete_event(), None)
        assert result["statusCode"] == 200
        mock_os_client.update.assert_called_once()
        mock_os_client.delete.assert_not_called()

    @patch("lambda_function.get_opensearch_client")
    def test_soft_delete_sets_deleted_true(self, mock_get_client, mock_os_client):
        import lambda_function as lf
        mock_get_client.return_value = mock_os_client
        with patch.dict("os.environ", {"ENABLE_HARD_DELETE": "false"}):
            lf.lambda_handler(self._make_delete_event(), None)
        doc = mock_os_client.update.call_args[1]["body"]["doc"]
        assert doc["_deleted"] is True

    @patch("lambda_function.get_opensearch_client")
    def test_soft_delete_sets_deleted_at_epoch_millis(self, mock_get_client, mock_os_client):
        import lambda_function as lf
        mock_get_client.return_value = mock_os_client
        with patch.dict("os.environ", {"ENABLE_HARD_DELETE": "false"}):
            lf.lambda_handler(self._make_delete_event(), None)
        doc = mock_os_client.update.call_args[1]["body"]["doc"]
        assert isinstance(doc["_deletedAt"], int)
        assert doc["_deletedAt"] > 1_700_000_000_000

    @patch("lambda_function.get_opensearch_client")
    def test_soft_delete_targets_correct_id_and_index(self, mock_get_client, mock_os_client):
        import lambda_function as lf
        mock_get_client.return_value = mock_os_client
        with patch.dict("os.environ", {"ENABLE_HARD_DELETE": "false"}):
            lf.lambda_handler(self._make_delete_event(), None)
        call_kwargs = mock_os_client.update.call_args[1]
        assert call_kwargs["id"] == "prod-soft-001"
        assert call_kwargs["index"] == lf.INDEX_PRODUCTS

    # ------------------------------------------------------------------
    # Hard delete
    # ------------------------------------------------------------------

    @patch("lambda_function.get_opensearch_client")
    def test_hard_delete_calls_client_delete(self, mock_get_client, mock_os_client):
        import lambda_function as lf
        mock_get_client.return_value = mock_os_client
        with patch.dict("os.environ", {"ENABLE_HARD_DELETE": "true"}):
            result = lf.lambda_handler(self._make_delete_event(), None)
        assert result["statusCode"] == 200
        mock_os_client.delete.assert_called_once_with(
            index=lf.INDEX_PRODUCTS, id="prod-soft-001", ignore=[404]
        )
        mock_os_client.update.assert_not_called()

    @patch("lambda_function.get_opensearch_client")
    def test_hard_delete_not_found_returns_200(self, mock_get_client, mock_os_client):
        import lambda_function as lf
        mock_os_client.delete.return_value = {"result": "not_found"}
        mock_get_client.return_value = mock_os_client
        with patch.dict("os.environ", {"ENABLE_HARD_DELETE": "true"}):
            result = lf.lambda_handler(self._make_delete_event(), None)
        assert result["statusCode"] == 200


# ===========================================================================
# 10. Addon Backfill from DynamoDB
# ===========================================================================

class TestAddonBackfill:
    """
    Tests for the DynamoDB addon backfill logic added to the product processor.

    When a product is upserted with a partNumber, the processor checks DynamoDB
    (PK=PRODUCT_ADDON, SK=<partNumber>) for cached addon data and merges it
    into the product payload before indexing.
    """

    def _make_upsert_event(self, unique_id="prod-backfill-001", part_number="TPPLATINUM"):
        body = {
            "operation": "u",
            "payload": {
                "uniqueID": unique_id,
                "partNumber": part_number,
                "name": "Test Product",
            },
        }
        return _make_function_url_event(body, time_epoch=1700000000000)

    # ------------------------------------------------------------------
    # Backfill merges addon fields
    # ------------------------------------------------------------------

    @patch("lambda_function.cache_table")
    @patch("lambda_function.get_opensearch_client")
    def test_backfill_merges_addon_fields(self, mock_get_client, mock_cache_table, mock_os_client):
        import lambda_function as lf
        mock_get_client.return_value = mock_os_client
        mock_cache_table.get_item.return_value = {
            "Item": {
                "PK": "PRODUCT_ADDON",
                "SK": "TPPLATINUM",
                "uom": "EA",
                "currency": "USD",
                "defaultProductClass": "A",
                "description": "Trivantage Plus Platinum Member",
                "status": "3000",
                "primarySupplier": "15087:11",
            }
        }
        lf.cache_table = mock_cache_table
        result = lf.lambda_handler(self._make_upsert_event(), None)
        assert result["statusCode"] == 200

        doc = mock_os_client.update.call_args[1]["body"]["doc"]
        assert doc["uom"] == "EA"
        assert doc["currency"] == "USD"
        assert doc["defaultProductClass"] == "A"
        assert doc["description"] == "Trivantage Plus Platinum Member"
        assert doc["status"] == "3000"
        assert doc["primarySupplier"] == "15087:11"
        assert "addonEnrichedAt" in doc

    # ------------------------------------------------------------------
    # No addon cache entry — product indexes normally
    # ------------------------------------------------------------------

    @patch("lambda_function.cache_table")
    @patch("lambda_function.get_opensearch_client")
    def test_no_addon_cache_indexes_normally(self, mock_get_client, mock_cache_table, mock_os_client):
        import lambda_function as lf
        mock_get_client.return_value = mock_os_client
        mock_cache_table.get_item.return_value = {}  # no Item key
        lf.cache_table = mock_cache_table
        result = lf.lambda_handler(self._make_upsert_event(), None)
        assert result["statusCode"] == 200

        doc = mock_os_client.update.call_args[1]["body"]["doc"]
        assert "uom" not in doc
        assert "addonEnrichedAt" not in doc

    # ------------------------------------------------------------------
    # DynamoDB error is non-fatal
    # ------------------------------------------------------------------

    @patch("lambda_function.cache_table")
    @patch("lambda_function.get_opensearch_client")
    def test_dynamo_error_is_non_fatal(self, mock_get_client, mock_cache_table, mock_os_client):
        import lambda_function as lf
        from botocore.exceptions import ClientError
        mock_get_client.return_value = mock_os_client
        mock_cache_table.get_item.side_effect = ClientError(
            {"Error": {"Code": "InternalServerError", "Message": "fail"}},
            "GetItem",
        )
        lf.cache_table = mock_cache_table
        result = lf.lambda_handler(self._make_upsert_event(), None)
        # Product should still index successfully
        assert result["statusCode"] == 200
        mock_os_client.update.assert_called_once()

    # ------------------------------------------------------------------
    # No backfill when partNumber is missing
    # ------------------------------------------------------------------

    @patch("lambda_function.cache_table")
    @patch("lambda_function.get_opensearch_client")
    def test_no_backfill_without_part_number(self, mock_get_client, mock_cache_table, mock_os_client):
        import lambda_function as lf
        mock_get_client.return_value = mock_os_client
        body = {
            "operation": "u",
            "payload": {"uniqueID": "prod-no-pn", "name": "No PartNumber"},
        }
        event = _make_function_url_event(body, time_epoch=1700000000000)
        lf.cache_table = mock_cache_table
        result = lf.lambda_handler(event, None)
        assert result["statusCode"] == 200
        # DynamoDB should NOT be called
        mock_cache_table.get_item.assert_not_called()

    # ------------------------------------------------------------------
    # Addon fields do NOT overwrite existing payload values
    # ------------------------------------------------------------------

    @patch("lambda_function.cache_table")
    @patch("lambda_function.get_opensearch_client")
    def test_addon_does_not_overwrite_existing_values(self, mock_get_client, mock_cache_table, mock_os_client):
        import lambda_function as lf
        mock_get_client.return_value = mock_os_client
        mock_cache_table.get_item.return_value = {
            "Item": {
                "PK": "PRODUCT_ADDON",
                "SK": "TPPLATINUM",
                "uom": "EA",
                "status": "3000",
            }
        }
        lf.cache_table = mock_cache_table
        # Product payload already has uom set
        body = {
            "operation": "u",
            "payload": {
                "uniqueID": "prod-override-001",
                "partNumber": "TPPLATINUM",
                "uom": "BX",  # already set — should NOT be overwritten
            },
        }
        event = _make_function_url_event(body, time_epoch=1700000000000)
        result = lf.lambda_handler(event, None)
        assert result["statusCode"] == 200

        doc = mock_os_client.update.call_args[1]["body"]["doc"]
        # uom should remain "BX" (from payload), not "EA" (from addon)
        assert doc["uom"] == "BX"
        # status should be merged from addon since payload didn't have it
        assert doc["status"] == "3000"

    # ------------------------------------------------------------------
    # Backfill only runs for c/u operations, not delete
    # ------------------------------------------------------------------

    @patch("lambda_function.cache_table")
    @patch("lambda_function.get_opensearch_client")
    def test_no_backfill_on_delete_operation(self, mock_get_client, mock_cache_table, mock_os_client):
        import lambda_function as lf
        mock_get_client.return_value = mock_os_client
        body = {
            "operation": "d",
            "payload": {"uniqueID": "prod-del-001", "partNumber": "TPPLATINUM"},
        }
        event = _make_function_url_event(body, time_epoch=1700000000000)
        lf.cache_table = mock_cache_table
        result = lf.lambda_handler(event, None)
        assert result["statusCode"] == 200
        # DynamoDB get_item should NOT be called for deletes
        mock_cache_table.get_item.assert_not_called()
