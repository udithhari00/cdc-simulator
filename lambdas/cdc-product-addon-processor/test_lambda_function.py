"""
Pytest test suite for cdc-product-addon-processor lambda_function.py
Target coverage: >= 90%

Run with:
    pytest test_lambda_function.py -v --cov=lambda_function --cov-report=term-missing
"""

import base64
import json
import sys
from unittest.mock import MagicMock, patch, call
import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_function_url_event(body_dict):
    """Helper to build a Function URL style event."""
    return {
        "body": json.dumps(body_dict),
    }


def _sample_item(**overrides):
    """Return a valid addon item dict with optional overrides."""
    item = {
        "ItemID": "TPPLATINUM",
        "UnitOfMeasure": "EA",
        "CostCurrency": "USD",
        "DefaultProductClass": "A",
        "Description": "Trivantage Plus Platinum Member",
        "ShortDescription": "Trivantage Plus Platinum Member",
        "Status": "3000",
        "PrimarySupplier": "15087:11",
    }
    item.update(overrides)
    return item


def _make_upsert_event(items=None, operation="u"):
    """Build a standard upsert event with items."""
    if items is None:
        items = [_sample_item()]
    body = {
        "operation": operation,
        "payload": {"items": items},
    }
    return _make_function_url_event(body)


def _make_delete_event(items=None):
    """Build a standard delete event with items."""
    return _make_upsert_event(items=items, operation="d")


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
    client.search.return_value = {"hits": {"hits": []}}
    return client


@pytest.fixture()
def mock_cache_table():
    """Return a MagicMock for the DynamoDB cache table."""
    table = MagicMock()
    table.put_item.return_value = {}
    table.delete_item.return_value = {}
    table.get_item.return_value = {}
    return table


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

    def test_body_is_invalid_json_string_returns_none(self):
        import lambda_function as lf
        assert lf.parse_body({"body": "{bad json"}) is None

    def test_body_is_unsupported_type_returns_none(self):
        import lambda_function as lf
        assert lf.parse_body({"body": 12345}) is None


# ===========================================================================
# 2. map_addon_fields
# ===========================================================================

class TestMapAddonFields:
    def test_maps_all_fields(self):
        import lambda_function as lf
        item = _sample_item()
        mapped = lf.map_addon_fields(item)
        assert mapped == {
            "uom": "EA",
            "currency": "USD",
            "defaultProductClass": "A",
            "description": "Trivantage Plus Platinum Member",
            "status": "3000",
            "primarySupplier": "15087:11",
        }

    def test_ignores_short_description(self):
        import lambda_function as lf
        item = _sample_item()
        mapped = lf.map_addon_fields(item)
        assert "shortDescription" not in mapped
        assert "ShortDescription" not in mapped

    def test_ignores_item_id(self):
        import lambda_function as lf
        item = _sample_item()
        mapped = lf.map_addon_fields(item)
        assert "ItemID" not in mapped
        assert "partNumber" not in mapped

    def test_skips_none_values(self):
        import lambda_function as lf
        item = {"UnitOfMeasure": "EA", "CostCurrency": None}
        mapped = lf.map_addon_fields(item)
        assert mapped == {"uom": "EA"}

    def test_empty_item_returns_empty_dict(self):
        import lambda_function as lf
        assert lf.map_addon_fields({}) == {}

    def test_strips_whitespace_from_strings(self):
        import lambda_function as lf
        item = {"UnitOfMeasure": "  EA  ", "Status": " 3000 "}
        mapped = lf.map_addon_fields(item)
        assert mapped["uom"] == "EA"
        assert mapped["status"] == "3000"


# ===========================================================================
# 3. find_product_doc_id
# ===========================================================================

class TestFindProductDocId:
    def test_returns_doc_id_when_found(self, mock_os_client):
        import lambda_function as lf
        mock_os_client.search.return_value = {
            "hits": {"hits": [{"_id": "doc-123"}]}
        }
        result = lf.find_product_doc_id(mock_os_client, "TPPLATINUM")
        assert result == "doc-123"

    def test_returns_none_when_not_found(self, mock_os_client):
        import lambda_function as lf
        mock_os_client.search.return_value = {"hits": {"hits": []}}
        result = lf.find_product_doc_id(mock_os_client, "NONEXISTENT")
        assert result is None

    def test_returns_none_on_exception(self, mock_os_client):
        import lambda_function as lf
        mock_os_client.search.side_effect = Exception("Connection error")
        result = lf.find_product_doc_id(mock_os_client, "TPPLATINUM")
        assert result is None

    def test_searches_by_part_number_keyword(self, mock_os_client):
        import lambda_function as lf
        mock_os_client.search.return_value = {"hits": {"hits": []}}
        lf.find_product_doc_id(mock_os_client, "ABC-123")
        call_kwargs = mock_os_client.search.call_args[1]
        assert call_kwargs["body"]["query"]["term"]["partNumber.keyword"] == "ABC-123"


# ===========================================================================
# 4. DynamoDB cache operations
# ===========================================================================

class TestDynamoDBCache:
    @patch("lambda_function.cache_table")
    def test_cache_addon_to_dynamo_writes_correct_item(self, mock_table):
        import lambda_function as lf
        addon_data = {"uom": "EA", "status": "3000"}
        lf.cache_addon_to_dynamo("TPPLATINUM", addon_data)
        mock_table.put_item.assert_called_once()
        item = mock_table.put_item.call_args[1]["Item"]
        assert item["PK"] == "PRODUCT_ADDON"
        assert item["SK"] == "TPPLATINUM"
        assert item["uom"] == "EA"
        assert item["status"] == "3000"
        assert "updatedAt" in item

    @patch("lambda_function.cache_table")
    def test_cache_addon_raises_on_client_error(self, mock_table):
        import lambda_function as lf
        from botocore.exceptions import ClientError
        mock_table.put_item.side_effect = ClientError(
            {"Error": {"Code": "InternalServerError", "Message": "fail"}},
            "PutItem",
        )
        with pytest.raises(ClientError):
            lf.cache_addon_to_dynamo("TPPLATINUM", {"uom": "EA"})

    @patch("lambda_function.cache_table")
    def test_delete_addon_from_dynamo_deletes_correct_key(self, mock_table):
        import lambda_function as lf
        lf.delete_addon_from_dynamo("TPPLATINUM")
        mock_table.delete_item.assert_called_once_with(
            Key={"PK": "PRODUCT_ADDON", "SK": "TPPLATINUM"}
        )

    @patch("lambda_function.cache_table")
    def test_delete_addon_from_dynamo_handles_error_gracefully(self, mock_table):
        import lambda_function as lf
        from botocore.exceptions import ClientError
        mock_table.delete_item.side_effect = ClientError(
            {"Error": {"Code": "InternalServerError", "Message": "fail"}},
            "DeleteItem",
        )
        # Should not raise — error is logged
        lf.delete_addon_from_dynamo("TPPLATINUM")


# ===========================================================================
# 5. process_one — upsert (product exists)
# ===========================================================================

class TestHandleUpsert:
    @patch("lambda_function.cache_addon_to_dynamo")
    @patch("lambda_function.find_product_doc_id", return_value="doc-123")
    def test_enriches_existing_product(self, mock_find, mock_cache, mock_os_client):
        import lambda_function as lf
        item = _sample_item()
        result = lf.process_one(mock_os_client, "u", item)
        assert result["status"] == "enriched"
        assert result["docId"] == "doc-123"

    @patch("lambda_function.cache_addon_to_dynamo")
    @patch("lambda_function.find_product_doc_id", return_value="doc-123")
    def test_updates_opensearch_with_mapped_fields(self, mock_find, mock_cache, mock_os_client):
        import lambda_function as lf
        item = _sample_item()
        lf.process_one(mock_os_client, "u", item)
        call_kwargs = mock_os_client.update.call_args[1]
        doc = call_kwargs["body"]["doc"]
        assert doc["uom"] == "EA"
        assert doc["currency"] == "USD"
        assert doc["defaultProductClass"] == "A"
        assert doc["description"] == "Trivantage Plus Platinum Member"
        assert doc["status"] == "3000"
        assert doc["primarySupplier"] == "15087:11"
        assert "addonEnrichedAt" in doc

    @patch("lambda_function.cache_addon_to_dynamo")
    @patch("lambda_function.find_product_doc_id", return_value="doc-123")
    def test_also_caches_to_dynamo_on_enrich(self, mock_find, mock_cache, mock_os_client):
        import lambda_function as lf
        item = _sample_item()
        lf.process_one(mock_os_client, "u", item)
        mock_cache.assert_called_once()
        call_args = mock_cache.call_args
        assert call_args[0][0] == "TPPLATINUM"

    @patch("lambda_function.cache_addon_to_dynamo")
    @patch("lambda_function.find_product_doc_id", return_value="doc-123")
    def test_returns_error_on_opensearch_failure(self, mock_find, mock_cache, mock_os_client):
        import lambda_function as lf
        mock_os_client.update.side_effect = Exception("OS error")
        result = lf.process_one(mock_os_client, "u", _sample_item())
        assert result["status"] == "error"
        assert "OS error" in result["reason"]


# ===========================================================================
# 6. process_one — upsert (product not found, cache to DynamoDB)
# ===========================================================================

class TestHandleUpsertCacheForBackfill:
    @patch("lambda_function.cache_addon_to_dynamo")
    @patch("lambda_function.find_product_doc_id", return_value=None)
    def test_caches_to_dynamo_when_product_not_found(self, mock_find, mock_cache, mock_os_client):
        import lambda_function as lf
        result = lf.process_one(mock_os_client, "u", _sample_item())
        assert result["status"] == "cached_for_backfill"
        mock_cache.assert_called_once()

    @patch("lambda_function.cache_addon_to_dynamo")
    @patch("lambda_function.find_product_doc_id", return_value=None)
    def test_does_not_update_opensearch_when_not_found(self, mock_find, mock_cache, mock_os_client):
        import lambda_function as lf
        lf.process_one(mock_os_client, "u", _sample_item())
        mock_os_client.update.assert_not_called()

    @patch("lambda_function.cache_addon_to_dynamo")
    @patch("lambda_function.find_product_doc_id", return_value=None)
    def test_returns_error_on_dynamo_failure(self, mock_find, mock_cache, mock_os_client):
        import lambda_function as lf
        mock_cache.side_effect = Exception("DynamoDB error")
        result = lf.process_one(mock_os_client, "u", _sample_item())
        assert result["status"] == "error"


# ===========================================================================
# 7. process_one — delete
# ===========================================================================

class TestHandleDelete:
    @patch("lambda_function.delete_addon_from_dynamo")
    @patch("lambda_function.find_product_doc_id", return_value="doc-123")
    def test_nulls_addon_fields_when_product_exists(self, mock_find, mock_del_cache, mock_os_client):
        import lambda_function as lf
        item = _sample_item()
        result = lf.process_one(mock_os_client, "d", item)
        assert result["status"] == "addon_deleted"
        call_kwargs = mock_os_client.update.call_args[1]
        doc = call_kwargs["body"]["doc"]
        for field in lf.ADDON_FIELDS:
            assert doc[field] is None
        assert doc["addonEnrichedAt"] is None

    @patch("lambda_function.delete_addon_from_dynamo")
    @patch("lambda_function.find_product_doc_id", return_value="doc-123")
    def test_deletes_dynamo_cache_on_delete(self, mock_find, mock_del_cache, mock_os_client):
        import lambda_function as lf
        lf.process_one(mock_os_client, "d", _sample_item())
        mock_del_cache.assert_called_once_with("TPPLATINUM")

    @patch("lambda_function.delete_addon_from_dynamo")
    @patch("lambda_function.find_product_doc_id", return_value=None)
    def test_deletes_dynamo_cache_even_when_product_not_in_index(self, mock_find, mock_del_cache, mock_os_client):
        import lambda_function as lf
        result = lf.process_one(mock_os_client, "d", _sample_item())
        assert result["status"] == "addon_deleted"
        mock_del_cache.assert_called_once_with("TPPLATINUM")
        mock_os_client.update.assert_not_called()

    @patch("lambda_function.delete_addon_from_dynamo")
    @patch("lambda_function.find_product_doc_id", return_value="doc-123")
    def test_returns_error_on_opensearch_failure(self, mock_find, mock_del_cache, mock_os_client):
        import lambda_function as lf
        mock_os_client.update.side_effect = Exception("OS error")
        result = lf.process_one(mock_os_client, "d", _sample_item())
        assert result["status"] == "error"


# ===========================================================================
# 8. process_one — validation
# ===========================================================================

class TestProcessOneValidation:
    def test_missing_item_id_returns_error(self, mock_os_client):
        import lambda_function as lf
        item = {"UnitOfMeasure": "EA"}  # no ItemID
        result = lf.process_one(mock_os_client, "u", item)
        assert result["status"] == "error"
        assert "missing ItemID" in result["reason"]

    @patch("lambda_function.find_product_doc_id", return_value=None)
    @patch("lambda_function.cache_addon_to_dynamo")
    def test_no_mappable_fields_returns_skipped(self, mock_cache, mock_find, mock_os_client):
        import lambda_function as lf
        item = {"ItemID": "TPPLATINUM"}  # no mappable fields
        result = lf.process_one(mock_os_client, "u", item)
        assert result["status"] == "skipped"
        assert "no mappable fields" in result["reason"]

    @patch("lambda_function.cache_addon_to_dynamo")
    @patch("lambda_function.find_product_doc_id", return_value=None)
    def test_unknown_operation_treated_as_upsert(self, mock_find, mock_cache, mock_os_client):
        import lambda_function as lf
        result = lf.process_one(mock_os_client, "x", _sample_item())
        # Should go through upsert path (cached_for_backfill since find returns None)
        assert result["status"] == "cached_for_backfill"


# ===========================================================================
# 9. lambda_handler — validation
# ===========================================================================

class TestLambdaHandlerValidation:
    def test_missing_body_returns_400(self):
        import lambda_function as lf
        result = lf.lambda_handler({}, None)
        assert result["statusCode"] == 400

    def test_no_payload_field_returns_400(self):
        import lambda_function as lf
        event = {"body": json.dumps({"operation": "u"})}
        result = lf.lambda_handler(event, None)
        assert result["statusCode"] == 400
        assert "payload" in result["body"]

    def test_payload_not_dict_or_list_returns_400(self):
        import lambda_function as lf
        event = {"body": json.dumps({"payload": "not-valid"})}
        result = lf.lambda_handler(event, None)
        assert result["statusCode"] == 400

    def test_empty_items_returns_400(self):
        import lambda_function as lf
        event = {"body": json.dumps({"payload": {"items": []}})}
        result = lf.lambda_handler(event, None)
        assert result["statusCode"] == 400

    def test_missing_items_key_returns_400(self):
        import lambda_function as lf
        event = {"body": json.dumps({"payload": {"notitems": []}})}
        result = lf.lambda_handler(event, None)
        assert result["statusCode"] == 400

    def test_unparseable_body_returns_400(self):
        import lambda_function as lf
        result = lf.lambda_handler({"body": "{bad json"}, None)
        assert result["statusCode"] == 400


# ===========================================================================
# 10. lambda_handler — full integration-style tests
# ===========================================================================

class TestLambdaHandlerIntegration:
    @patch("lambda_function.cache_addon_to_dynamo")
    @patch("lambda_function.find_product_doc_id", return_value="doc-123")
    @patch("lambda_function.get_opensearch_client")
    def test_upsert_single_item_returns_200(self, mock_get_client, mock_find, mock_cache, mock_os_client):
        import lambda_function as lf
        mock_get_client.return_value = mock_os_client
        result = lf.lambda_handler(_make_upsert_event(), None)
        assert result["statusCode"] == 200
        assert result["body"]["results"][0]["status"] == "enriched"

    @patch("lambda_function.cache_addon_to_dynamo")
    @patch("lambda_function.find_product_doc_id", return_value=None)
    @patch("lambda_function.get_opensearch_client")
    def test_upsert_caches_when_product_not_found(self, mock_get_client, mock_find, mock_cache, mock_os_client):
        import lambda_function as lf
        mock_get_client.return_value = mock_os_client
        result = lf.lambda_handler(_make_upsert_event(), None)
        assert result["statusCode"] == 200
        assert result["body"]["results"][0]["status"] == "cached_for_backfill"

    @patch("lambda_function.delete_addon_from_dynamo")
    @patch("lambda_function.find_product_doc_id", return_value="doc-123")
    @patch("lambda_function.get_opensearch_client")
    def test_delete_returns_200(self, mock_get_client, mock_find, mock_del_cache, mock_os_client):
        import lambda_function as lf
        mock_get_client.return_value = mock_os_client
        result = lf.lambda_handler(_make_delete_event(), None)
        assert result["statusCode"] == 200
        assert result["body"]["results"][0]["status"] == "addon_deleted"

    @patch("lambda_function.cache_addon_to_dynamo")
    @patch("lambda_function.find_product_doc_id", return_value="doc-123")
    @patch("lambda_function.get_opensearch_client")
    def test_batch_multiple_items(self, mock_get_client, mock_find, mock_cache, mock_os_client):
        import lambda_function as lf
        mock_get_client.return_value = mock_os_client
        items = [
            _sample_item(ItemID="ITEM-001"),
            _sample_item(ItemID="ITEM-002"),
            _sample_item(ItemID="ITEM-003"),
        ]
        result = lf.lambda_handler(_make_upsert_event(items=items), None)
        assert result["statusCode"] == 200
        assert len(result["body"]["results"]) == 3
        assert all(r["status"] == "enriched" for r in result["body"]["results"])

    @patch("lambda_function.get_opensearch_client")
    def test_batch_with_mixed_results_returns_207(self, mock_get_client, mock_os_client):
        """One item succeeds, another fails — should return 207."""
        import lambda_function as lf
        mock_get_client.return_value = mock_os_client
        items = [
            _sample_item(ItemID="GOOD-001"),
            {"UnitOfMeasure": "EA"},  # missing ItemID → error
        ]
        result = lf.lambda_handler(_make_upsert_event(items=items), None)
        assert result["statusCode"] == 207

    @patch("lambda_function.cache_addon_to_dynamo")
    @patch("lambda_function.find_product_doc_id", return_value=None)
    @patch("lambda_function.get_opensearch_client")
    def test_flat_list_payload_format(self, mock_get_client, mock_find, mock_cache, mock_os_client):
        """Support payload as flat list of items (not inside 'items' key)."""
        import lambda_function as lf
        mock_get_client.return_value = mock_os_client
        body = {
            "operation": "u",
            "payload": [_sample_item()],
        }
        result = lf.lambda_handler(_make_function_url_event(body), None)
        assert result["statusCode"] == 200

    def test_empty_event_returns_400(self):
        import lambda_function as lf
        result = lf.lambda_handler({}, None)
        assert result["statusCode"] == 400

    def test_unhandled_exception_returns_500(self):
        import lambda_function as lf
        with patch.object(lf, "parse_body", side_effect=RuntimeError("boom")):
            event = {"body": json.dumps({"payload": {"items": [_sample_item()]}})}
            result = lf.lambda_handler(event, None)
            assert result["statusCode"] == 500
            assert "boom" in result["body"]

    @patch("lambda_function.cache_addon_to_dynamo")
    @patch("lambda_function.find_product_doc_id", return_value="doc-123")
    @patch("lambda_function.get_opensearch_client")
    def test_base64_encoded_event(self, mock_get_client, mock_find, mock_cache, mock_os_client):
        import lambda_function as lf
        mock_get_client.return_value = mock_os_client
        body = {"operation": "u", "payload": {"items": [_sample_item()]}}
        encoded = base64.b64encode(json.dumps(body).encode()).decode()
        event = {"body": encoded, "isBase64Encoded": True}
        result = lf.lambda_handler(event, None)
        assert result["statusCode"] == 200

    @patch("lambda_function.cache_addon_to_dynamo")
    @patch("lambda_function.find_product_doc_id", return_value="doc-123")
    @patch("lambda_function.get_opensearch_client")
    def test_dict_body_event(self, mock_get_client, mock_find, mock_cache, mock_os_client):
        import lambda_function as lf
        mock_get_client.return_value = mock_os_client
        event = {"body": {"operation": "u", "payload": {"items": [_sample_item()]}}}
        result = lf.lambda_handler(event, None)
        assert result["statusCode"] == 200

    @patch("lambda_function.cache_addon_to_dynamo")
    @patch("lambda_function.find_product_doc_id", return_value="doc-123")
    @patch("lambda_function.get_opensearch_client")
    def test_default_operation_is_upsert(self, mock_get_client, mock_find, mock_cache, mock_os_client):
        """When operation is omitted, it defaults to 'u'."""
        import lambda_function as lf
        mock_get_client.return_value = mock_os_client
        body = {"payload": {"items": [_sample_item()]}}
        result = lf.lambda_handler(_make_function_url_event(body), None)
        assert result["statusCode"] == 200
        assert result["body"]["results"][0]["status"] == "enriched"


# ===========================================================================
# 11. now_ms
# ===========================================================================

class TestNowMs:
    def test_returns_integer(self):
        import lambda_function as lf
        assert isinstance(lf.now_ms(), int)

    def test_is_recent_timestamp(self):
        import lambda_function as lf
        ts = lf.now_ms()
        assert ts > 1_700_000_000_000


# ===========================================================================
# 12. get_opensearch_client
# ===========================================================================

class TestGetOpenSearchClient:
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

    @patch("lambda_function.OPENSEARCH_AUTH", "basic")
    @patch("lambda_function.OpenSearch")
    def test_basic_auth_creates_client(self, mock_os_cls):
        import lambda_function as lf
        lf.opensearch_client = None
        mock_os_cls.return_value = MagicMock()
        client = lf.get_opensearch_client()
        assert client is not None
        call_kwargs = mock_os_cls.call_args[1]
        assert call_kwargs["verify_certs"] is False
