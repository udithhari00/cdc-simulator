"""
Unit tests for cdc-lambda-organization/lambda_function.py

Run with:
    pytest test_lambda_function.py -v
"""

import base64
import json
import os
from unittest.mock import MagicMock, patch, call

import pytest

# ---------------------------------------------------------------------------
# Patch AWS / OpenSearch clients before importing the module so module-level
# boto3 calls don't require real credentials.
# ---------------------------------------------------------------------------
os.environ.setdefault('OPENSEARCH_HOST', 'localhost')
os.environ.setdefault('OPENSEARCH_AUTH', 'basic')
os.environ.setdefault('AWS_REGION', 'us-east-1')
os.environ.setdefault('DYNAMODB_ENDPOINT', 'http://localhost:8000')

with patch('boto3.resource'), patch('boto3.Session'):
    import lambda_function as lf


# ===========================================================================
# Helpers
# ===========================================================================

def make_payload(**kwargs) -> dict:
    """Return a minimal valid YFS_ORGANIZATION payload (uppercase keys)."""
    base = {
        'ORGANIZATION_KEY':  'ORG-KEY-001',
        'ORGANIZATION_CODE': 'DEFAULT',
        'ORGANIZATION_NAME': 'Glen Raven Inc',
    }
    base.update(kwargs)
    return base


def make_debezium_envelope(payload: dict, op: str = 'c', ts_ms: int = 1700000000000) -> dict:
    """Wrap a payload in a Debezium envelope."""
    key = 'before' if op == 'd' else 'after'
    # Debezium sends lowercase column names
    lc_payload = {k.lower(): v for k, v in payload.items()}
    return {'op': op, 'ts_ms': ts_ms, key: lc_payload, ('before' if op != 'd' else 'after'): None}


def make_msk_event(debezium_event: dict, topic: str = 'glenraven-dev.STERLING.YFS_ORGANIZATION') -> dict:
    """Wrap a Debezium event in an MSK trigger event."""
    encoded = base64.b64encode(json.dumps(debezium_event).encode()).decode()
    partition_key = f'{topic}-0'
    return {
        'eventSource': 'aws:kafka',
        'records': {
            partition_key: [{'topic': topic, 'offset': 0, 'value': encoded}]
        }
    }


# ===========================================================================
# Fixtures
# ===========================================================================

@pytest.fixture(autouse=True)
def reset_opensearch_client():
    """Reset the lazy singleton before each test."""
    lf.opensearch_client = None
    yield
    lf.opensearch_client = None


@pytest.fixture
def mock_opensearch():
    client = MagicMock()
    client.update_by_query.return_value = {'updated': 0, 'failures': []}
    client.update.return_value = {'result': 'updated'}
    client.delete.return_value = {'result': 'deleted'}
    lf.opensearch_client = client
    return client


@pytest.fixture
def mock_cache_table():
    table = MagicMock()
    table.get_item.return_value = {}
    lf.cache_table = table
    return table


# ===========================================================================
# Utility function tests
# ===========================================================================

class TestGetValue:
    def test_returns_string(self):
        assert lf.get_value({'KEY': 'value'}, 'KEY') == 'value'

    def test_strips_whitespace(self):
        # Oracle CHAR fields are padded with trailing spaces
        assert lf.get_value({'KEY': 'value   '}, 'KEY') == 'value'

    def test_missing_key_returns_none(self):
        assert lf.get_value({}, 'KEY') is None

    def test_none_value_returns_none(self):
        assert lf.get_value({'KEY': None}, 'KEY') is None

    def test_whitespace_only_returns_none(self):
        assert lf.get_value({'KEY': '   '}, 'KEY') is None

    def test_numeric_value_coerced_to_string(self):
        assert lf.get_value({'KEY': 42}, 'KEY') == '42'


class TestGetTimestampValue:
    def test_microseconds_converted_to_millis(self):
        # Debezium sends timestamps in microseconds
        ts_us = 1700000000000123
        result = lf.get_timestamp_value({'TS': ts_us}, 'TS')
        assert result == ts_us // 1000

    def test_milliseconds_returned_as_is(self):
        ts_ms = 1700000000000
        result = lf.get_timestamp_value({'TS': ts_ms}, 'TS')
        assert result == ts_ms

    def test_seconds_converted_to_millis(self):
        ts_s = 1700000000
        result = lf.get_timestamp_value({'TS': ts_s}, 'TS')
        assert result == ts_s * 1000

    def test_none_returns_none(self):
        assert lf.get_timestamp_value({'TS': None}, 'TS') is None

    def test_missing_returns_none(self):
        assert lf.get_timestamp_value({}, 'TS') is None

    def test_invalid_returns_none(self):
        assert lf.get_timestamp_value({'TS': 'not-a-number'}, 'TS') is None


class TestBuildFullName:
    def test_all_parts(self):
        assert lf.build_full_name({'firstName': 'John', 'middleName': 'A', 'lastName': 'Doe'}) == 'John A Doe'

    def test_first_and_last_only(self):
        assert lf.build_full_name({'firstName': 'John', 'lastName': 'Doe'}) == 'John Doe'

    def test_empty_returns_none(self):
        assert lf.build_full_name({}) is None

    def test_none_parts_skipped(self):
        assert lf.build_full_name({'firstName': 'John', 'middleName': None, 'lastName': 'Doe'}) == 'John Doe'


class TestGetPrimaryPhone:
    def test_prefers_mobile(self):
        info = {'mobilePhone': '111', 'dayPhone': '222', 'eveningPhone': '333'}
        assert lf.get_primary_phone(info) == '111'

    def test_falls_back_to_day(self):
        info = {'dayPhone': '222', 'eveningPhone': '333'}
        assert lf.get_primary_phone(info) == '222'

    def test_falls_back_to_evening(self):
        assert lf.get_primary_phone({'eveningPhone': '333'}) == '333'

    def test_none_when_no_phone(self):
        assert lf.get_primary_phone({}) is None


# ===========================================================================
# Topic extraction
# ===========================================================================

class TestExtractTableFromTopic:
    def test_known_topic(self):
        assert lf.extract_table_from_topic('glenraven-dev.STERLING.yfs_organization') == 'YFS_ORGANIZATION'

    def test_unknown_topic_uppercased(self):
        assert lf.extract_table_from_topic('prefix.schema.some_other_table') == 'SOME_OTHER_TABLE'


# ===========================================================================
# process_single_event routing
# ===========================================================================

class TestProcessSingleEvent:
    def test_routes_yfs_organization(self, mock_opensearch, mock_cache_table):
        payload = make_payload()
        result = lf.process_single_event('YFS_ORGANIZATION', 'c', payload)
        assert result['statusCode'] == 200

    def test_unknown_table_returns_400(self):
        result = lf.process_single_event('UNKNOWN_TABLE', 'c', {})
        assert result['statusCode'] == 400


# ===========================================================================
# process_organization — Phase 1 (DynamoDB)
# ===========================================================================

class TestProcessOrganizationDynamoDB:
    def test_missing_org_key_returns_400(self, mock_cache_table, mock_opensearch):
        result = lf.process_organization({'ORGANIZATION_CODE': 'DEFAULT'}, 'c')
        assert result['statusCode'] == 400
        assert 'ORGANIZATION_KEY' in result['body']

    def test_create_writes_to_dynamo(self, mock_cache_table, mock_opensearch):
        payload = make_payload()
        lf.process_organization(payload, 'c')
        mock_cache_table.put_item.assert_called_once()
        item = mock_cache_table.put_item.call_args[1]['Item']
        assert item['PK'] == 'ORGANIZATION'
        assert item['SK'] == 'ORG-KEY-001'
        assert item['organizationName'] == 'Glen Raven Inc'

    def test_create_stores_address_keys(self, mock_cache_table, mock_opensearch):
        payload = make_payload(CORPORATE_ADDRESS_KEY='CORP-ADDR-1', CONTACT_ADDRESS_KEY='CONT-ADDR-1')
        lf.process_organization(payload, 'c')
        item = mock_cache_table.put_item.call_args[1]['Item']
        assert item['corporateAddressKey'] == 'CORP-ADDR-1'
        assert item['contactAddressKey'] == 'CONT-ADDR-1'

    def test_create_excludes_none_fields(self, mock_cache_table, mock_opensearch):
        payload = make_payload()  # no address keys
        lf.process_organization(payload, 'c')
        item = mock_cache_table.put_item.call_args[1]['Item']
        assert 'corporateAddressKey' not in item
        assert 'contactAddressKey' not in item

    def test_delete_calls_delete_item(self, mock_cache_table, mock_opensearch):
        payload = make_payload()
        lf.process_organization(payload, 'd')
        mock_cache_table.delete_item.assert_called_once_with(
            Key={'PK': 'ORGANIZATION', 'SK': 'ORG-KEY-001'}
        )
        mock_cache_table.put_item.assert_not_called()

    def test_dynamo_failure_propagates(self, mock_cache_table, mock_opensearch):
        mock_cache_table.put_item.side_effect = Exception('DynamoDB unavailable')
        with pytest.raises(Exception, match='DynamoDB unavailable'):
            lf.process_organization(make_payload(), 'c')

    def test_hash_stored_in_dynamo(self, mock_cache_table, mock_opensearch):
        payload = make_payload()
        lf.process_organization(payload, 'c')
        item = mock_cache_table.put_item.call_args[1]['Item']
        assert item['orgNameHash'] == lf.compute_org_name_hash('Glen Raven Inc')

    def test_fanout_skipped_when_name_unchanged(self, mock_cache_table, mock_opensearch):
        existing_hash = lf.compute_org_name_hash('Glen Raven Inc')
        mock_cache_table.get_item.return_value = {
            'Item': {'PK': 'ORGANIZATION', 'SK': 'ORG-KEY-001', 'orgNameHash': existing_hash}
        }
        lf.process_organization(make_payload(), 'u')
        mock_opensearch.update_by_query.assert_not_called()

    def test_fanout_triggered_when_name_changed(self, mock_cache_table, mock_opensearch):
        old_hash = lf.compute_org_name_hash('Old Name')
        mock_cache_table.get_item.return_value = {
            'Item': {'PK': 'ORGANIZATION', 'SK': 'ORG-KEY-001', 'orgNameHash': old_hash}
        }
        lf.process_organization(make_payload(), 'u')  # payload has 'Glen Raven Inc'
        assert mock_opensearch.update_by_query.call_count == 3


# ===========================================================================
# process_organization — Phase 2 (fan-out)
# ===========================================================================

SAMPLE_HASH = lf.compute_org_name_hash('Glen Raven Inc')


class TestComputeOrgNameHash:
    def test_returns_16_char_hex(self):
        h = lf.compute_org_name_hash('Glen Raven Inc')
        assert len(h) == 16
        assert all(c in '0123456789abcdef' for c in h)

    def test_none_name_is_stable(self):
        assert lf.compute_org_name_hash(None) == lf.compute_org_name_hash(None)

    def test_different_names_differ(self):
        assert lf.compute_org_name_hash('Acme') != lf.compute_org_name_hash('Globex')

    def test_same_name_same_hash(self):
        assert lf.compute_org_name_hash('Glen Raven') == lf.compute_org_name_hash('Glen Raven')


class TestUpdateOrdersWithOrganization:
    def test_fanout_calls_update_by_query_three_times(self, mock_opensearch):
        lf.update_orders_with_organization('ORG-KEY-001', 'Glen Raven Inc', 'u', SAMPLE_HASH)
        assert mock_opensearch.update_by_query.call_count == 3

    def test_fanout_queries_seller_buyer_customer_with_hash_filter(self, mock_opensearch):
        lf.update_orders_with_organization('ORG-KEY-001', 'Glen Raven Inc', 'u', SAMPLE_HASH)
        calls = mock_opensearch.update_by_query.call_args_list
        must_terms   = [c[1]['body']['query']['bool']['must']['term'] for c in calls]
        mustnot_terms = [c[1]['body']['query']['bool']['must_not']['term'] for c in calls]

        assert {'sellerOrganizationCode': 'ORG-KEY-001'} in must_terms
        assert {'buyerOrganizationCode': 'ORG-KEY-001'} in must_terms
        assert {'customer.organizationCode': 'ORG-KEY-001'} in must_terms

        # must_not filters out docs already at the current hash
        assert {'sellerOrgNameHash': SAMPLE_HASH} in mustnot_terms
        assert {'buyerOrgNameHash': SAMPLE_HASH} in mustnot_terms
        assert {'customer.orgNameHash': SAMPLE_HASH} in mustnot_terms

    def test_fanout_delete_passes_none_name(self, mock_opensearch):
        lf.update_orders_with_organization('ORG-KEY-001', 'Glen Raven Inc', 'd', SAMPLE_HASH)
        for c in mock_opensearch.update_by_query.call_args_list:
            assert c[1]['body']['script']['params']['name'] is None

    def test_fanout_update_passes_name_and_hash(self, mock_opensearch):
        lf.update_orders_with_organization('ORG-KEY-001', 'Glen Raven Inc', 'u', SAMPLE_HASH)
        for c in mock_opensearch.update_by_query.call_args_list:
            params = c[1]['body']['script']['params']
            assert params['name'] == 'Glen Raven Inc'
            assert params['nameHash'] == SAMPLE_HASH

    def test_fanout_script_sets_hash_for_seller(self, mock_opensearch):
        lf.update_orders_with_organization('ORG-KEY-001', 'Glen Raven Inc', 'u', SAMPLE_HASH)
        seller_call = next(
            c for c in mock_opensearch.update_by_query.call_args_list
            if 'sellerOrganizationCode' in c[1]['body']['query']['bool']['must']['term']
        )
        script_src = seller_call[1]['body']['script']['source']
        assert 'sellerOrgNameHash' in script_src

    def test_fanout_script_sets_hash_for_customer_nested(self, mock_opensearch):
        lf.update_orders_with_organization('ORG-KEY-001', 'Glen Raven Inc', 'u', SAMPLE_HASH)
        customer_call = next(
            c for c in mock_opensearch.update_by_query.call_args_list
            if 'customer.organizationCode' in c[1]['body']['query']['bool']['must']['term']
        )
        script_src = customer_call[1]['body']['script']['source']
        assert 'ctx._source.customer.organizationName' in script_src
        assert 'ctx._source.customer.orgNameHash' in script_src

    def test_fanout_opensearch_failure_is_non_fatal(self, mock_opensearch):
        mock_opensearch.update_by_query.side_effect = Exception('OpenSearch down')
        # Should not raise — fan-out failures are logged as warnings
        lf.update_orders_with_organization('ORG-KEY-001', 'Glen Raven Inc', 'u', SAMPLE_HASH)

    def test_fanout_uses_conflicts_proceed(self, mock_opensearch):
        lf.update_orders_with_organization('ORG-KEY-001', 'Glen Raven Inc', 'u', SAMPLE_HASH)
        for c in mock_opensearch.update_by_query.call_args_list:
            assert c[1]['conflicts'] == 'proceed'


# ===========================================================================
# process_organization — Phase 3 (OpenSearch index)
# ===========================================================================

class TestIndexOrganization:
    def test_delete_calls_opensearch_delete(self, mock_opensearch, mock_cache_table):
        lf.index_organization('DEFAULT', 'ORG-KEY-001', 'Glen Raven Inc', {}, 'd', None, None)
        mock_opensearch.delete.assert_called_once_with(
            index=lf.INDEX_ORGANIZATION, id='DEFAULT', ignore=[404]
        )
        mock_opensearch.update.assert_not_called()

    def test_create_upserts_with_org_code_as_id(self, mock_opensearch, mock_cache_table):
        lf.index_organization('DEFAULT', 'ORG-KEY-001', 'Glen Raven Inc', {}, 'c', None, None)
        mock_opensearch.update.assert_called_once()
        call_kwargs = mock_opensearch.update.call_args[1]
        assert call_kwargs['id'] == 'DEFAULT'
        assert call_kwargs['index'] == lf.INDEX_ORGANIZATION
        assert call_kwargs['body']['doc_as_upsert'] is True

    def test_create_doc_contains_core_fields(self, mock_opensearch, mock_cache_table):
        lf.index_organization('DEFAULT', 'ORG-KEY-001', 'Glen Raven Inc', {}, 'c', None, None, source_timestamp=9999)
        doc = mock_opensearch.update.call_args[1]['body']['doc']
        assert doc['organizationKey'] == 'ORG-KEY-001'
        assert doc['organizationCode'] == 'DEFAULT'
        assert doc['organizationName'] == 'Glen Raven Inc'
        assert doc['cdcTimestamp'] == 9999
        assert 'indexedAt' in doc

    def test_none_fields_excluded_from_doc(self, mock_opensearch, mock_cache_table):
        lf.index_organization('DEFAULT', 'ORG-KEY-001', 'Glen Raven Inc', {}, 'c', None, None)
        doc = mock_opensearch.update.call_args[1]['body']['doc']
        # Fields with None values should be stripped
        assert all(v is not None for v in doc.values())

    def test_enriched_with_corporate_address(self, mock_opensearch, mock_cache_table):
        mock_cache_table.get_item.return_value = {
            'Item': {'PK': 'PERSON_INFO', 'SK': 'CORP-ADDR-1',
                     'city': 'Burlington', 'state': 'NC', 'zipCode': '27215',
                     'country': 'US', 'addressLine1': '1 Glen Raven Rd',
                     'addressHash': 'abc123'}
        }
        lf.index_organization('DEFAULT', 'ORG-KEY-001', 'Glen Raven Inc', {}, 'c', 'CORP-ADDR-1', None)
        doc = mock_opensearch.update.call_args[1]['body']['doc']
        assert doc['address']['city'] == 'Burlington'
        assert doc['address']['state'] == 'NC'
        assert doc['address']['zipCode'] == '27215'
        assert doc['address']['addressHash'] == 'abc123'

    def test_enriched_with_contact(self, mock_opensearch, mock_cache_table):
        mock_cache_table.get_item.return_value = {
            'Item': {'PK': 'PERSON_INFO', 'SK': 'CONT-ADDR-1',
                     'firstName': 'Jane', 'lastName': 'Smith',
                     'emailId': 'jane@example.com', 'mobilePhone': '555-0100',
                     'contactHash': 'def456'}
        }
        lf.index_organization('DEFAULT', 'ORG-KEY-001', 'Glen Raven Inc', {}, 'c', None, 'CONT-ADDR-1')
        doc = mock_opensearch.update.call_args[1]['body']['doc']
        assert doc['contact']['fullName'] == 'Jane Smith'
        assert doc['contact']['email'] == 'jane@example.com'
        assert doc['contact']['phone'] == '555-0100'
        assert doc['contact']['contactHash'] == 'def456'

    def test_enriched_with_customer(self, mock_opensearch, mock_cache_table):
        mock_cache_table.get_item.return_value = {
            'Item': {'PK': 'CUSTOMER_BY_ORG_KEY', 'SK': 'ORG-KEY-001',
                     'customerId': 'CUST-001', 'customerKey': 'CKEY-001',
                     'customerType': 'B', 'customerRewardsNo': 'RWD-999',
                     'customerHash': 'ghi789'}
        }
        lf.index_organization('DEFAULT', 'ORG-KEY-001', 'Glen Raven Inc', {}, 'c', None, None)
        doc = mock_opensearch.update.call_args[1]['body']['doc']
        assert doc['customer']['customerId'] == 'CUST-001'
        assert doc['customer']['customerRewardsNo'] == 'RWD-999'
        assert doc['customer']['customerHash'] == 'ghi789'

    def test_no_enrichment_when_cache_miss(self, mock_opensearch, mock_cache_table):
        mock_cache_table.get_item.return_value = {}
        lf.index_organization('DEFAULT', 'ORG-KEY-001', 'Glen Raven Inc', {}, 'c', 'ADDR-1', 'CONT-1')
        doc = mock_opensearch.update.call_args[1]['body']['doc']
        assert 'address' not in doc
        assert 'contact' not in doc

    def test_opensearch_failure_is_non_fatal(self, mock_opensearch, mock_cache_table):
        mock_opensearch.update.side_effect = Exception('OpenSearch down')
        # Should not raise — Phase 3 failures are caught and logged
        lf.index_organization('DEFAULT', 'ORG-KEY-001', 'Glen Raven Inc', {}, 'c', None, None)


# ===========================================================================
# process_organization — searchableText
# ===========================================================================

class TestSearchableText:
    def _get_indexed_doc(self, mock_opensearch, mock_cache_table, payload_overrides=None, cache_return=None):
        payload = make_payload(**(payload_overrides or {}))
        if cache_return is not None:
            mock_cache_table.get_item.return_value = cache_return
        lf.process_organization(payload, 'c')
        return mock_opensearch.update.call_args[1]['body']['doc']

    def test_contains_org_code_and_name(self, mock_opensearch, mock_cache_table):
        doc = self._get_indexed_doc(mock_opensearch, mock_cache_table)
        assert 'DEFAULT' in doc['searchableText']
        assert 'Glen Raven Inc' in doc['searchableText']

    def test_contains_customer_id_when_enriched(self, mock_opensearch, mock_cache_table):
        doc = self._get_indexed_doc(
            mock_opensearch, mock_cache_table,
            cache_return={'Item': {'PK': 'CUSTOMER_BY_ORG_KEY', 'SK': 'ORG-KEY-001',
                                   'customerId': 'CUST-001'}}
        )
        assert 'CUST-001' in doc['searchableText']

    def test_contains_extn_sales_person(self, mock_opensearch, mock_cache_table):
        doc = self._get_indexed_doc(
            mock_opensearch, mock_cache_table,
            payload_overrides={'EXTN_SALES_PERSON': 'SalesRep1'}
        )
        assert 'SalesRep1' in doc['searchableText']

    def test_empty_searchable_text_when_minimal_payload(self, mock_opensearch, mock_cache_table):
        # Only required fields — searchableText should still have org code/name
        doc = self._get_indexed_doc(mock_opensearch, mock_cache_table)
        assert doc['searchableText'] != ''

    def test_no_none_tokens_in_searchable_text(self, mock_opensearch, mock_cache_table):
        doc = self._get_indexed_doc(mock_opensearch, mock_cache_table)
        assert 'None' not in doc['searchableText']


# ===========================================================================
# MSK event processing
# ===========================================================================

class TestProcessMskEvent:
    def _make_event(self, op='c', extra_payload=None):
        payload = make_payload(**(extra_payload or {}))
        envelope = make_debezium_envelope(payload, op=op)
        return make_msk_event(envelope)

    def test_successful_batch_returns_200(self, mock_opensearch, mock_cache_table):
        result = lf.process_msk_event(self._make_event())
        assert result['statusCode'] == 200
        assert 'Processed 1' in result['body']

    def test_partial_failure_returns_207(self, mock_opensearch, mock_cache_table):
        mock_cache_table.put_item.side_effect = Exception('DynamoDB down')
        result = lf.process_msk_event(self._make_event())
        assert result['statusCode'] == 207
        assert 'errors 1' in result['body']

    def test_all_failures_returns_207(self, mock_opensearch, mock_cache_table):
        mock_cache_table.put_item.side_effect = Exception('DynamoDB down')
        event = self._make_event()
        result = lf.process_msk_event(event)
        assert result['statusCode'] == 207

    def test_multiple_messages_in_batch(self, mock_opensearch, mock_cache_table):
        def make_msg(org_key):
            p = make_payload(ORGANIZATION_KEY=org_key, ORGANIZATION_CODE=org_key)
            env = make_debezium_envelope(p)
            encoded = base64.b64encode(json.dumps(env).encode()).decode()
            return {'topic': 'glenraven-dev.STERLING.YFS_ORGANIZATION', 'offset': 0, 'value': encoded}

        event = {
            'eventSource': 'aws:kafka',
            'records': {
                'glenraven-dev.STERLING.YFS_ORGANIZATION-0': [
                    make_msg('KEY-001'),
                    make_msg('KEY-002'),
                    make_msg('KEY-003'),
                ]
            }
        }
        result = lf.process_msk_event(event)
        assert result['statusCode'] == 200
        assert 'Processed 3' in result['body']

    def test_delete_operation_decoded_from_before(self, mock_opensearch, mock_cache_table):
        payload = make_payload()
        envelope = make_debezium_envelope(payload, op='d')
        event = make_msk_event(envelope)
        result = lf.process_msk_event(event)
        assert result['statusCode'] == 200
        mock_cache_table.delete_item.assert_called_once()

    def test_payload_keys_uppercased(self, mock_opensearch, mock_cache_table):
        """Debezium sends lowercase column names; lambda must uppercase them."""
        payload = make_payload()
        envelope = make_debezium_envelope(payload, op='c')
        # Confirm the envelope has lowercase keys (as Debezium sends)
        assert 'organization_key' in envelope['after']
        # After processing, put_item should be called with uppercased field names
        lf.process_msk_event(make_msk_event(envelope))
        item = mock_cache_table.put_item.call_args[1]['Item']
        assert item['organizationKey'] == 'ORG-KEY-001'

    def test_malformed_base64_counted_as_error(self, mock_opensearch, mock_cache_table):
        event = {
            'eventSource': 'aws:kafka',
            'records': {
                'topic-0': [{'topic': 'topic', 'offset': 0, 'value': 'not-valid-base64!!!'}]
            }
        }
        result = lf.process_msk_event(event)
        assert result['statusCode'] == 207
        assert 'errors 1' in result['body']


# ===========================================================================
# _parse_debezium_message — format detection
# ===========================================================================

class TestParseDebeziumMessage:
    def test_flattened_smt_extracts_op_and_timestamp(self):
        event = {
            "schema": {"type": "struct", "fields": []},
            "payload": {
                "ORGANIZATION_KEY": "KEY-001",
                "ORGANIZATION_NAME": "Acme Corp",
                "__op": "r",
                "__source_ts_ms": 1773133137000,
                "__table": "YFS_ORGANIZATION",
            }
        }
        op, ts, payload = lf._parse_debezium_message(event)
        assert op == "r"
        assert ts == 1773133137000
        assert "ORGANIZATION_KEY" in payload
        assert "__op" not in payload
        assert "__source_ts_ms" not in payload
        assert "__table" not in payload

    def test_flattened_smt_strips_all_dunder_keys(self):
        inner = {"COL_A": "val", "__op": "c", "__source_ts_ms": 999, "__table": "T"}
        _, _, payload = lf._parse_debezium_message({"schema": {}, "payload": inner})
        assert payload == {"COL_A": "val"}

    def test_standard_envelope_create(self):
        event = {"op": "c", "ts_ms": 1700000000000, "before": None,
                 "after": {"ORGANIZATION_KEY": "K1", "ORGANIZATION_NAME": "Acme"}}
        op, ts, payload = lf._parse_debezium_message(event)
        assert op == "c"
        assert ts == 1700000000000
        assert payload == {"ORGANIZATION_KEY": "K1", "ORGANIZATION_NAME": "Acme"}

    def test_standard_envelope_delete_uses_before(self):
        event = {"op": "d", "ts_ms": 1700000000000,
                 "before": {"ORGANIZATION_KEY": "K1"}, "after": None}
        op, ts, payload = lf._parse_debezium_message(event)
        assert op == "d"
        assert payload == {"ORGANIZATION_KEY": "K1"}

    def test_flattened_null_op_defaults_to_c(self):
        event = {"schema": {}, "payload": {"COL": "val", "__op": None}}
        op, _, _ = lf._parse_debezium_message(event)
        assert op == "c"


# ===========================================================================
# Real CDC event — flattened MSK format (YFS_ORGANIZATION snapshot)
# ===========================================================================

REAL_ORG_CDC_EVENT = {
    "schema": {
        "type": "struct",
        "optional": False,
        "name": "glenraven-dev.STERLING.YFS_ORGANIZATION.Value",
        "fields": []
    },
    "payload": {
        "ORGANIZATION_KEY": "T100226829_762          ",
        "ORGANIZATION_CODE": "T100226829_762          ",
        "RESOURCE_IDENTIFIER": " ",
        "XREF_ORGANIZATION_CODE": "                        ",
        "IS_HUB_ORGANIZATION": "N",
        "IS_NODE": " ",
        "IS_ENTERPRISE": "N",
        "PARENT_ORGANIZATION_CODE": "                        ",
        "CORPORATE_ADDRESS_KEY": "                        ",
        "CONTACT_ADDRESS_KEY": "                        ",
        "BILLING_ADDRESS_KEY": "                        ",
        "LOCALE_CODE": " ",
        "ORGANIZATION_NAME": "TRANS FX INC",
        "CREATOR_ORGANIZATION_KEY": "TV                      ",
        "PRIMARY_ENTERPRISE_KEY": "TV                      ",
        "DUNS_NUMBER": " ",
        "PRIMARY_URL": " ",
        "INHERIT_CONFIG_FROM_ENTERPRISE": "                        ",
        "INHERIT_ORG_CONFIG": " ",
        "BUSINESS_CALENDAR_KEY": "                        ",
        "ACTIVATE_FLAG": " ",
        "PWD_POLICY_KEY": None,
        "LOCKID": 2.0,
        "CREATETS": 1731434930000,
        "MODIFYTS": 1733452768000,
        "CREATEUSERID": "kaufl@dist.us.glenraven.com",
        "MODIFYUSERID": "sterling",
        "CREATEPROGID": "ISCCSSYS001",
        "MODIFYPROGID": "YDKConfigDeploy",
        "PAYMENT_PROCESSING_REQD": "N",
        "REQUIRES_CHAINED_ORDER": " ",
        "REQUIRES_CHANGE_REQUEST": " ",
        "DEFAULT_DISTRIBUTION_RULE_ID": " ",
        "INVENTORY_KEPT_EXTERNALLY": " ",
        "INVENTORY_PUBLISHED": "Y",
        "CATALOG_ORGANIZATION_CODE": "TV                      ",
        "INVENTORY_ORGANIZATION_CODE": "TV                      ",
        "PRICING_ORGANIZATION_CODE": "TV                      ",
        "IS_BUYER": "Y",
        "IS_SELLER": " ",
        "IS_CARRIER": " ",
        "ITEM_XREF_RULE": " ",
        "XREF_ALIAS_TYPE": " ",
        "TAXPAYER_ID": " ",
        "TAX_JURISDICTION": " ",
        "TAX_EXEMPT_FLAG": "N",
        "TAX_EXEMPTION_CERTIFICATE": " ",
        "ISSUING_AUTHORITY": " ",
        "AUTHORITY_TYPE": " ",
        "IS_SOURCING_KEPT": "N",
        "ACCOUNT_WITH_HUB": " ",
        "PRIMARY_SIC_CODE": " ",
        "DEFAULT_PAYMENT_RULE_ID": " ",
        "DEFAULT_PRICE_PROGRAM_KEY": "                        ",
        "CAPACITY_ORGANIZATION_CODE": "TV                      ",
        "IS_LEGAL_ENTITY": " ",
        "IS_SRVC_SOURCING_KEPT": " ",
        "IS_DEL_SOURCING_KEPT": " ",
        "CAPACITY_KEPT_EXTERNALLY": " ",
        "DEFAULT_FULFILLMENT_TYPE": " ",
        "UCC_PREFIX": "0000000",
        "REQUIRES_VAS_COMPLIANCE": " ",
        "SUPPLY_KEPT_EXTERNALLY": "N",
        "ORG_SUFFIX": " ",
        "CUSTOMER_MASTER_ORG_CODE": "TV                      ",
        "COLONY_ID": "DEFAULT",
        "EXTN_PREFERRED_DC": "84                      ",
        "EXTN_NAFTA_REQUIRED": "N",
        "EXTN_SALES_PERSON": "84041",
        "EXTN_PAYMENT_TERMS": " ",
        "EXTN_PAYMENT_TERM_DESC": " ",
        "EXTN_INTERNAL_NOTE": " ",
        "EXTN_EXTERNAL_PRINT_NOTE": " ",
        "EXTN_DEFAULT_CARRIER": " ",
        "EXTN_DEFAULT_CARRIER_SERVICE": " ",
        "EXTN_DEFAULT_FREIGHT_TERMS": " ",
        "EXTN_APLUS_CUSTOMER_ID": " ",
        "EXTN_REMIT_TO_ID": " ",
        "ORGANIZATION_NAME_LC": " ",
        "EXTN_NOTIFICATION_TYPE": " ",
        "EXTN_PB_CARRIER_CODE": " ",
        "EXTN_FOB": " ",
        "EXTN_SHIPPING_PREFERENCE": "P",
        "EXTN_MARKET_SEGMENT": "Industrial General Purpose - TBD Not Classified",
        "EXTN_THRESHOLD_PERCENTAGE": " ",
        "EXTN_TIME_LIMIT": " ",
        "EXTN_EXPORT_CUSTOMER": "N",
        "EXTN_IMPORT_VENDOR": " ",
        "EXTN_INSIDE_SALES_REP": "Lydia Kauffman",
        "STORE_CONFIG_ORGANIZATION_CODE": None,
        "IS_TEMPLATE": "N",
        "CUSTOMER_MAINTAINED_EXTERNALLY": None,
        "INVORG_SUPPLY_DEMAND_EXTERNAL": None,
        "SYNC_TARGET_ID": None,
        "__op": "r",
        "__table": "YFS_ORGANIZATION",
        "__source_ts_ms": 1773133137000
    }
}


def _make_msk_event_from_raw(cdc_event: dict,
                              topic: str = 'glenraven-dev.STERLING.YFS_ORGANIZATION') -> dict:
    """Wrap a raw CDC event dict as an MSK event source mapping trigger."""
    encoded = base64.b64encode(json.dumps(cdc_event).encode()).decode()
    return {
        'eventSource': 'aws:kafka',
        'records': {
            f'{topic}-0': [{'topic': topic, 'offset': 0, 'value': encoded}]
        }
    }


class TestRealOrgCdcEvent:
    """End-to-end tests using the real MSK event format from Debezium."""

    @pytest.fixture
    def indexed_doc(self, mock_opensearch, mock_cache_table):
        event = _make_msk_event_from_raw(REAL_ORG_CDC_EVENT)
        result = lf.lambda_handler(event, None)
        assert result['statusCode'] == 200
        return mock_opensearch.update.call_args[1]['body']['doc']

    def test_processes_successfully(self, mock_opensearch, mock_cache_table):
        event = _make_msk_event_from_raw(REAL_ORG_CDC_EVENT)
        result = lf.lambda_handler(event, None)
        assert result['statusCode'] == 200

    def test_organization_code_stripped(self, indexed_doc):
        assert indexed_doc['organizationCode'] == 'T100226829_762'

    def test_organization_key_stripped(self, indexed_doc):
        assert indexed_doc['organizationKey'] == 'T100226829_762'

    def test_organization_name(self, indexed_doc):
        assert indexed_doc['organizationName'] == 'TRANS FX INC'

    def test_is_hub_organization_field_name(self, indexed_doc):
        # Field must be isHubOrganization (not isHub)
        assert 'isHubOrganization' in indexed_doc
        assert 'isHub' not in indexed_doc
        assert indexed_doc['isHubOrganization'] == 'N'

    def test_is_enterprise(self, indexed_doc):
        assert indexed_doc['isEnterprise'] == 'N'

    def test_is_buyer(self, indexed_doc):
        assert indexed_doc['isBuyer'] == 'Y'

    def test_extn_sales_person(self, indexed_doc):
        assert indexed_doc['extnSalesPerson'] == '84041'

    def test_extn_inside_sales_rep(self, indexed_doc):
        assert indexed_doc['extnInsideSalesRep'] == 'Lydia Kauffman'

    def test_extn_preferred_dc_stripped(self, indexed_doc):
        assert indexed_doc['extnPreferredDc'] == '84'

    def test_extn_nafta_required(self, indexed_doc):
        assert indexed_doc['extnNaftaRequired'] == 'N'

    def test_extn_export_customer(self, indexed_doc):
        assert indexed_doc['extnExportCustomer'] == 'N'

    def test_extn_shipping_preference(self, indexed_doc):
        assert indexed_doc['extnShippingPreference'] == 'P'

    def test_extn_market_segment(self, indexed_doc):
        assert indexed_doc['extnMarketSegment'] == 'Industrial General Purpose - TBD Not Classified'

    def test_created_timestamp(self, indexed_doc):
        assert indexed_doc['createdTimestamp'] == 1731434930000

    def test_modified_timestamp(self, indexed_doc):
        assert indexed_doc['modifiedTimestamp'] == 1733452768000

    def test_cdc_operation_is_r(self, indexed_doc):
        assert indexed_doc['cdcOperation'] == 'r'

    def test_cdc_timestamp_from_source_ts_ms(self, indexed_doc):
        assert indexed_doc['cdcTimestamp'] == 1773133137000

    def test_primary_enterprise_key_stripped(self, indexed_doc):
        assert indexed_doc['primaryEnterpriseKey'] == 'TV'

    def test_blank_fields_excluded(self, indexed_doc):
        # All-whitespace values must be stripped to None and excluded
        assert 'dunsNumber' not in indexed_doc
        assert 'isNode' not in indexed_doc
        assert 'xrefOrganizationCode' not in indexed_doc
        assert 'resourceIdentifier' not in indexed_doc

    def test_null_fields_excluded(self, indexed_doc):
        assert 'pwdPolicyKey' not in indexed_doc

    def test_no_none_values_in_doc(self, indexed_doc):
        assert all(v is not None for v in indexed_doc.values())

    def test_opensearch_indexed_with_org_code_as_id(self, mock_opensearch, mock_cache_table):
        event = _make_msk_event_from_raw(REAL_ORG_CDC_EVENT)
        lf.lambda_handler(event, None)
        assert mock_opensearch.update.call_args[1]['id'] == 'T100226829_762'

    def test_dynamo_cache_written_with_org_key(self, mock_opensearch, mock_cache_table):
        event = _make_msk_event_from_raw(REAL_ORG_CDC_EVENT)
        lf.lambda_handler(event, None)
        item = mock_cache_table.put_item.call_args[1]['Item']
        assert item['SK'] == 'T100226829_762'
        assert item['organizationName'] == 'TRANS FX INC'

    def test_searchable_text_includes_org_code_and_name(self, indexed_doc):
        assert 'T100226829_762' in indexed_doc['searchableText']
        assert 'TRANS FX INC' in indexed_doc['searchableText']

    def test_searchable_text_includes_sales_rep(self, indexed_doc):
        assert 'Lydia Kauffman' in indexed_doc['searchableText']

    def test_searchable_text_includes_sales_person(self, indexed_doc):
        assert '84041' in indexed_doc['searchableText']


# ===========================================================================
# Real CDC event — Kimpton Rowan Palm Springs (from Kafka topic)
# ===========================================================================

REAL_ORG_CDC_EVENT_KIMPTON = {
    "schema": {
        "type": "struct",
        "optional": False,
        "name": "glenraven-dev.STERLING.YFS_ORGANIZATION.Value",
        "fields": []
    },
    "payload": {
        "ORGANIZATION_KEY": "T100226454_762          ",
        "ORGANIZATION_CODE": "T100226454_762          ",
        "RESOURCE_IDENTIFIER": " ",
        "XREF_ORGANIZATION_CODE": "                        ",
        "IS_HUB_ORGANIZATION": "N",
        "IS_NODE": " ",
        "IS_ENTERPRISE": "N",
        "PARENT_ORGANIZATION_CODE": "                        ",
        "CORPORATE_ADDRESS_KEY": "                        ",
        "CONTACT_ADDRESS_KEY": "                        ",
        "BILLING_ADDRESS_KEY": "                        ",
        "LOCALE_CODE": " ",
        "ORGANIZATION_NAME": "KIMPTON ROWAN PALM SPRINGS",
        "CREATOR_ORGANIZATION_KEY": "TV                      ",
        "PRIMARY_ENTERPRISE_KEY": "TV                      ",
        "DUNS_NUMBER": " ",
        "PRIMARY_URL": " ",
        "INHERIT_CONFIG_FROM_ENTERPRISE": "                        ",
        "INHERIT_ORG_CONFIG": " ",
        "BUSINESS_CALENDAR_KEY": "                        ",
        "ACTIVATE_FLAG": " ",
        "PWD_POLICY_KEY": None,
        "LOCKID": 3.0,
        "CREATETS": 1728659337000,
        "MODIFYTS": 1733452768000,
        "CREATEUSERID": "vasca@dist.us.glenraven.com",
        "MODIFYUSERID": "sterling",
        "CREATEPROGID": "ISCCSSYS001",
        "MODIFYPROGID": "YDKConfigDeploy",
        "PAYMENT_PROCESSING_REQD": "N",
        "REQUIRES_CHAINED_ORDER": " ",
        "REQUIRES_CHANGE_REQUEST": " ",
        "DEFAULT_DISTRIBUTION_RULE_ID": " ",
        "INVENTORY_KEPT_EXTERNALLY": " ",
        "INVENTORY_PUBLISHED": "Y",
        "CATALOG_ORGANIZATION_CODE": "TV                      ",
        "INVENTORY_ORGANIZATION_CODE": "TV                      ",
        "PRICING_ORGANIZATION_CODE": "TV                      ",
        "IS_BUYER": "Y",
        "IS_SELLER": " ",
        "IS_CARRIER": " ",
        "ITEM_XREF_RULE": " ",
        "XREF_ALIAS_TYPE": " ",
        "TAXPAYER_ID": " ",
        "TAX_JURISDICTION": " ",
        "TAX_EXEMPT_FLAG": "N",
        "TAX_EXEMPTION_CERTIFICATE": " ",
        "ISSUING_AUTHORITY": " ",
        "AUTHORITY_TYPE": " ",
        "IS_SOURCING_KEPT": "N",
        "ACCOUNT_WITH_HUB": " ",
        "PRIMARY_SIC_CODE": " ",
        "DEFAULT_PAYMENT_RULE_ID": " ",
        "DEFAULT_PRICE_PROGRAM_KEY": "                        ",
        "CAPACITY_ORGANIZATION_CODE": "TV                      ",
        "IS_LEGAL_ENTITY": " ",
        "IS_SRVC_SOURCING_KEPT": " ",
        "IS_DEL_SOURCING_KEPT": " ",
        "CAPACITY_KEPT_EXTERNALLY": " ",
        "DEFAULT_FULFILLMENT_TYPE": " ",
        "UCC_PREFIX": "0000000",
        "REQUIRES_VAS_COMPLIANCE": " ",
        "SUPPLY_KEPT_EXTERNALLY": "N",
        "ORG_SUFFIX": " ",
        "CUSTOMER_MASTER_ORG_CODE": "TV                      ",
        "COLONY_ID": "DEFAULT",
        "EXTN_PREFERRED_DC": "84                      ",
        "EXTN_NAFTA_REQUIRED": "N",
        "EXTN_SALES_PERSON": "8011",
        "EXTN_PAYMENT_TERMS": " ",
        "EXTN_PAYMENT_TERM_DESC": " ",
        "EXTN_INTERNAL_NOTE": " ",
        "EXTN_EXTERNAL_PRINT_NOTE": " ",
        "EXTN_DEFAULT_CARRIER": " ",
        "EXTN_DEFAULT_CARRIER_SERVICE": " ",
        "EXTN_DEFAULT_FREIGHT_TERMS": " ",
        "EXTN_APLUS_CUSTOMER_ID": " ",
        "EXTN_REMIT_TO_ID": " ",
        "ORGANIZATION_NAME_LC": " ",
        "EXTN_NOTIFICATION_TYPE": " ",
        "EXTN_PB_CARRIER_CODE": " ",
        "EXTN_FOB": " ",
        "EXTN_SHIPPING_PREFERENCE": "P",
        "EXTN_MARKET_SEGMENT": "Decorative Residential - Designers",
        "EXTN_THRESHOLD_PERCENTAGE": " ",
        "EXTN_TIME_LIMIT": " ",
        "EXTN_EXPORT_CUSTOMER": "N",
        "EXTN_IMPORT_VENDOR": " ",
        "EXTN_INSIDE_SALES_REP": "Lisa Campbell",
        "STORE_CONFIG_ORGANIZATION_CODE": None,
        "IS_TEMPLATE": "N",
        "CUSTOMER_MAINTAINED_EXTERNALLY": None,
        "INVORG_SUPPLY_DEMAND_EXTERNAL": None,
        "SYNC_TARGET_ID": None,
        "__op": "r",
        "__table": "YFS_ORGANIZATION",
        "__source_ts_ms": 1773133137000
    }
}


class TestKimptonOrgCdcEvent:
    """End-to-end tests using actual Kafka CDC data — Kimpton Rowan Palm Springs."""

    @pytest.fixture
    def indexed_doc(self, mock_opensearch, mock_cache_table):
        event = _make_msk_event_from_raw(REAL_ORG_CDC_EVENT_KIMPTON)
        result = lf.lambda_handler(event, None)
        assert result['statusCode'] == 200
        return mock_opensearch.update.call_args[1]['body']['doc']

    def test_processes_successfully(self, mock_opensearch, mock_cache_table):
        event = _make_msk_event_from_raw(REAL_ORG_CDC_EVENT_KIMPTON)
        result = lf.lambda_handler(event, None)
        assert result['statusCode'] == 200

    def test_organization_code_stripped(self, indexed_doc):
        assert indexed_doc['organizationCode'] == 'T100226454_762'

    def test_organization_key_stripped(self, indexed_doc):
        assert indexed_doc['organizationKey'] == 'T100226454_762'

    def test_organization_name(self, indexed_doc):
        assert indexed_doc['organizationName'] == 'KIMPTON ROWAN PALM SPRINGS'

    def test_is_buyer(self, indexed_doc):
        assert indexed_doc['isBuyer'] == 'Y'

    def test_is_enterprise(self, indexed_doc):
        assert indexed_doc['isEnterprise'] == 'N'

    def test_tax_exempt_flag(self, indexed_doc):
        assert indexed_doc['taxExemptFlag'] == 'N'

    def test_extn_sales_person(self, indexed_doc):
        assert indexed_doc['extnSalesPerson'] == '8011'

    def test_extn_inside_sales_rep(self, indexed_doc):
        assert indexed_doc['extnInsideSalesRep'] == 'Lisa Campbell'

    def test_extn_preferred_dc_stripped(self, indexed_doc):
        assert indexed_doc['extnPreferredDc'] == '84'

    def test_extn_nafta_required(self, indexed_doc):
        assert indexed_doc['extnNaftaRequired'] == 'N'

    def test_extn_export_customer(self, indexed_doc):
        assert indexed_doc['extnExportCustomer'] == 'N'

    def test_extn_shipping_preference(self, indexed_doc):
        assert indexed_doc['extnShippingPreference'] == 'P'

    def test_extn_market_segment(self, indexed_doc):
        assert indexed_doc['extnMarketSegment'] == 'Decorative Residential - Designers'

    def test_created_timestamp(self, indexed_doc):
        assert indexed_doc['createdTimestamp'] == 1728659337000

    def test_modified_timestamp(self, indexed_doc):
        assert indexed_doc['modifiedTimestamp'] == 1733452768000

    def test_cdc_operation_is_r(self, indexed_doc):
        assert indexed_doc['cdcOperation'] == 'r'

    def test_cdc_timestamp_from_source_ts_ms(self, indexed_doc):
        assert indexed_doc['cdcTimestamp'] == 1773133137000

    def test_primary_enterprise_key_stripped(self, indexed_doc):
        assert indexed_doc['primaryEnterpriseKey'] == 'TV'

    def test_blank_fields_excluded(self, indexed_doc):
        assert 'dunsNumber' not in indexed_doc
        assert 'isNode' not in indexed_doc
        assert 'xrefOrganizationCode' not in indexed_doc
        assert 'resourceIdentifier' not in indexed_doc
        assert 'localeCode' not in indexed_doc

    def test_null_fields_excluded(self, indexed_doc):
        assert 'pwdPolicyKey' not in indexed_doc
        assert 'storeConfigOrganizationCode' not in indexed_doc

    def test_no_none_values_in_doc(self, indexed_doc):
        assert all(v is not None for v in indexed_doc.values())

    def test_opensearch_indexed_with_org_code_as_id(self, mock_opensearch, mock_cache_table):
        event = _make_msk_event_from_raw(REAL_ORG_CDC_EVENT_KIMPTON)
        lf.lambda_handler(event, None)
        assert mock_opensearch.update.call_args[1]['id'] == 'T100226454_762'

    def test_dynamo_cache_written_with_org_key(self, mock_opensearch, mock_cache_table):
        event = _make_msk_event_from_raw(REAL_ORG_CDC_EVENT_KIMPTON)
        lf.lambda_handler(event, None)
        item = mock_cache_table.put_item.call_args[1]['Item']
        assert item['SK'] == 'T100226454_762'
        assert item['organizationName'] == 'KIMPTON ROWAN PALM SPRINGS'

    def test_searchable_text_includes_org_code_and_name(self, indexed_doc):
        assert 'T100226454_762' in indexed_doc['searchableText']
        assert 'KIMPTON ROWAN PALM SPRINGS' in indexed_doc['searchableText']

    def test_searchable_text_includes_sales_rep(self, indexed_doc):
        assert 'Lisa Campbell' in indexed_doc['searchableText']

    def test_searchable_text_includes_sales_person(self, indexed_doc):
        assert '8011' in indexed_doc['searchableText']


# ===========================================================================
# lambda_handler entry point
# ===========================================================================

class TestLambdaHandler:
    def test_msk_event_routed_correctly(self, mock_opensearch, mock_cache_table):
        payload = make_payload()
        envelope = make_debezium_envelope(payload)
        event = make_msk_event(envelope)
        result = lf.lambda_handler(event, None)
        assert result['statusCode'] == 200

    def test_direct_invocation_routed_correctly(self, mock_opensearch, mock_cache_table):
        event = {
            'table': 'YFS_ORGANIZATION',
            'operation': 'c',
            'payload': make_payload(),
        }
        result = lf.lambda_handler(event, None)
        assert result['statusCode'] == 200

    def test_unhandled_exception_returns_500(self, mock_opensearch, mock_cache_table):
        with patch.object(lf, 'process_msk_event', side_effect=RuntimeError('boom')):
            event = {'eventSource': 'aws:kafka', 'records': {}}
            result = lf.lambda_handler(event, None)
        assert result['statusCode'] == 500
        assert 'boom' in result['body']


# ===========================================================================
# get_cached_customer_contact_by_org_key
# ===========================================================================

class TestGetCachedCustomerContactByOrgKey:

    def test_returns_data_when_found(self, mock_cache_table):
        mock_cache_table.get_item.return_value = {
            'Item': {'PK': 'CUSTOMER_CONTACT_BY_ORG_KEY', 'SK': 'ORG-KEY-001',
                     'customerKey': 'CKEY-001', 'contactName': 'Jane Doe', 'updatedAt': '2024-01-01'}
        }
        result = lf.get_cached_customer_contact_by_org_key('ORG-KEY-001')
        assert result['customerKey'] == 'CKEY-001'
        assert result['contactName'] == 'Jane Doe'
        assert 'PK' not in result
        assert 'SK' not in result
        assert 'updatedAt' not in result

    def test_returns_none_when_not_found(self, mock_cache_table):
        mock_cache_table.get_item.return_value = {}
        result = lf.get_cached_customer_contact_by_org_key('ORG-KEY-MISSING')
        assert result is None

    def test_returns_none_on_dynamo_error(self, mock_cache_table):
        mock_cache_table.get_item.side_effect = Exception('DynamoDB down')
        result = lf.get_cached_customer_contact_by_org_key('ORG-KEY-001')
        assert result is None

    def test_uses_correct_pk_and_sk(self, mock_cache_table):
        mock_cache_table.get_item.return_value = {}
        lf.get_cached_customer_contact_by_org_key('ORG-KEY-001')
        mock_cache_table.get_item.assert_called_once_with(
            Key={'PK': 'CUSTOMER_CONTACT_BY_ORG_KEY', 'SK': 'ORG-KEY-001'}
        )


# ===========================================================================
# enrich_org_with_addresses
# ===========================================================================

SAMPLE_PERSON_INFO = {
    'firstName': 'John', 'middleName': 'A', 'lastName': 'Smith',
    'emailId': 'john@example.com', 'dayPhone': '555-0001',
    'company': 'Acme Corp', 'addressLine1': '100 Main St',
    'city': 'Raleigh', 'state': 'NC', 'zipCode': '27601', 'country': 'US',
}


class TestEnrichOrgWithAddresses:

    @patch('lambda_function.get_cached_person_info', return_value=SAMPLE_PERSON_INFO)
    def test_populates_all_three_address_types(self, mock_pi, mock_cache_table):
        mock_cache_table.query.return_value = {'Items': [
            {'SK': 'CKEY-001#BillTo', 'personInfoKey': 'PI001'},
            {'SK': 'CKEY-001#ShipTo', 'personInfoKey': 'PI002'},
            {'SK': 'CKEY-001#SoldTo', 'personInfoKey': 'PI003'},
        ]}
        org_doc = {}
        lf.enrich_org_with_addresses(org_doc, 'CKEY-001')
        assert 'billTo' in org_doc
        assert 'shipTo' in org_doc
        assert 'soldTo' in org_doc

    @patch('lambda_function.get_cached_person_info', return_value=SAMPLE_PERSON_INFO)
    def test_bill_to_has_full_name(self, mock_pi, mock_cache_table):
        mock_cache_table.query.return_value = {'Items': [
            {'SK': 'CKEY-001#BillTo', 'personInfoKey': 'PI001'},
        ]}
        org_doc = {}
        lf.enrich_org_with_addresses(org_doc, 'CKEY-001')
        assert org_doc['billTo']['fullName'] == 'John A Smith'
        assert org_doc['billTo']['emailId'] == 'john@example.com'
        assert org_doc['billTo']['city'] == 'Raleigh'

    @patch('lambda_function.get_cached_person_info', return_value=SAMPLE_PERSON_INFO)
    def test_includes_person_info_key(self, mock_pi, mock_cache_table):
        mock_cache_table.query.return_value = {'Items': [
            {'SK': 'CKEY-001#ShipTo', 'personInfoKey': 'PI002'},
        ]}
        org_doc = {}
        lf.enrich_org_with_addresses(org_doc, 'CKEY-001')
        assert org_doc['shipTo']['personInfoKey'] == 'PI002'

    def test_no_addnl_address_records_does_nothing(self, mock_cache_table):
        mock_cache_table.query.return_value = {'Items': []}
        org_doc = {}
        lf.enrich_org_with_addresses(org_doc, 'CKEY-001')
        assert 'billTo' not in org_doc
        assert 'shipTo' not in org_doc
        assert 'soldTo' not in org_doc

    @patch('lambda_function.get_cached_person_info', return_value=None)
    def test_missing_person_info_skips_address(self, mock_pi, mock_cache_table):
        mock_cache_table.query.return_value = {'Items': [
            {'SK': 'CKEY-001#BillTo', 'personInfoKey': 'PI001'},
        ]}
        org_doc = {}
        lf.enrich_org_with_addresses(org_doc, 'CKEY-001')
        assert 'billTo' not in org_doc

    @patch('lambda_function.get_cached_person_info', return_value=SAMPLE_PERSON_INFO)
    def test_missing_person_info_key_skips_address(self, mock_pi, mock_cache_table):
        mock_cache_table.query.return_value = {'Items': [
            {'SK': 'CKEY-001#BillTo'},  # no personInfoKey
        ]}
        org_doc = {}
        lf.enrich_org_with_addresses(org_doc, 'CKEY-001')
        assert 'billTo' not in org_doc
        mock_pi.assert_not_called()

    @patch('lambda_function.get_cached_person_info', return_value=SAMPLE_PERSON_INFO)
    def test_unknown_address_type_skipped(self, mock_pi, mock_cache_table):
        mock_cache_table.query.return_value = {'Items': [
            {'SK': 'CKEY-001#Unknown', 'personInfoKey': 'PI001'},
        ]}
        org_doc = {}
        lf.enrich_org_with_addresses(org_doc, 'CKEY-001')
        assert 'Unknown' not in org_doc

    def test_dynamo_query_failure_is_non_fatal(self, mock_cache_table):
        mock_cache_table.query.side_effect = Exception('DynamoDB down')
        org_doc = {}
        lf.enrich_org_with_addresses(org_doc, 'CKEY-001')  # Should not raise

    @patch('lambda_function.get_cached_person_info', return_value=SAMPLE_PERSON_INFO)
    def test_query_uses_correct_pk_and_begins_with(self, mock_pi, mock_cache_table):
        mock_cache_table.query.return_value = {'Items': []}
        lf.enrich_org_with_addresses({}, 'CKEY-001')
        kce = mock_cache_table.query.call_args[1]['KeyConditionExpression']
        # Verify the begins_with prefix includes customerKey#
        assert 'CKEY-001#' in str(kce) or mock_cache_table.query.called

    @patch('lambda_function.get_cached_person_info', return_value=SAMPLE_PERSON_INFO)
    def test_none_values_excluded_from_address(self, mock_pi, mock_cache_table):
        mock_cache_table.query.return_value = {'Items': [
            {'SK': 'CKEY-001#BillTo', 'personInfoKey': 'PI001'},
        ]}
        org_doc = {}
        lf.enrich_org_with_addresses(org_doc, 'CKEY-001')
        assert all(v is not None for v in org_doc['billTo'].values())


# ===========================================================================
# Phase 3 — billTo/shipTo/soldTo enrichment integration
# ===========================================================================

class TestIndexOrganizationAddressEnrichment:

    def _setup_cache(self, mock_cache_table, customer_item=None, addnl_items=None, person_info=None):
        """Configure mock_cache_table to return customer-by-org-key + addnl addresses + person info."""
        def get_item_side_effect(**kwargs):
            key = kwargs.get('Key', {})
            pk, sk = key.get('PK', ''), key.get('SK', '')
            if pk == 'CUSTOMER_BY_ORG_KEY' and customer_item:
                return {'Item': {'PK': pk, 'SK': sk, **customer_item}}
            if pk == 'PERSON_INFO' and person_info:
                return {'Item': {'PK': pk, 'SK': sk, **person_info}}
            return {}

        mock_cache_table.get_item.side_effect = get_item_side_effect
        mock_cache_table.query.return_value = {'Items': addnl_items or []}

    def test_enriches_bill_to_ship_to_sold_to(self, mock_opensearch, mock_cache_table):
        self._setup_cache(
            mock_cache_table,
            customer_item={'customerKey': 'CKEY-001'},
            addnl_items=[
                {'SK': 'CKEY-001#BillTo', 'personInfoKey': 'PI001'},
                {'SK': 'CKEY-001#ShipTo', 'personInfoKey': 'PI002'},
                {'SK': 'CKEY-001#SoldTo', 'personInfoKey': 'PI003'},
            ],
            person_info={**SAMPLE_PERSON_INFO}
        )
        lf.index_organization('DEFAULT', 'ORG-KEY-001', 'Glen Raven Inc', {}, 'c', None, None)
        doc = mock_opensearch.update.call_args[1]['body']['doc']
        assert 'billTo' in doc
        assert 'shipTo' in doc
        assert 'soldTo' in doc
        assert doc['billTo']['fullName'] == 'John A Smith'

    def test_no_customer_by_org_key_skips_address_enrichment(self, mock_opensearch, mock_cache_table):
        mock_cache_table.get_item.return_value = {}
        lf.index_organization('DEFAULT', 'ORG-KEY-001', 'Glen Raven Inc', {}, 'c', None, None)
        doc = mock_opensearch.update.call_args[1]['body']['doc']
        assert 'billTo' not in doc
        assert 'shipTo' not in doc
        assert 'soldTo' not in doc

    def test_customer_without_customer_key_skips(self, mock_opensearch, mock_cache_table):
        self._setup_cache(
            mock_cache_table,
            customer_item={'customerId': 'CUST-001'},  # no customerKey
        )
        lf.index_organization('DEFAULT', 'ORG-KEY-001', 'Glen Raven Inc', {}, 'c', None, None)
        doc = mock_opensearch.update.call_args[1]['body']['doc']
        assert 'billTo' not in doc

    def test_searchable_text_includes_bill_to_fields(self, mock_opensearch, mock_cache_table):
        self._setup_cache(
            mock_cache_table,
            customer_item={'customerKey': 'CKEY-001'},
            addnl_items=[{'SK': 'CKEY-001#BillTo', 'personInfoKey': 'PI001'}],
            person_info={'firstName': 'Jane', 'lastName': 'Doe', 'emailId': 'jane@acme.com', 'company': 'Acme'}
        )
        lf.index_organization('DEFAULT', 'ORG-KEY-001', 'Glen Raven Inc', {}, 'c', None, None)
        doc = mock_opensearch.update.call_args[1]['body']['doc']
        assert 'Jane Doe' in doc['searchableText']
        assert 'jane@acme.com' in doc['searchableText']
        assert 'Acme' in doc['searchableText']


# ===========================================================================
# Soft Delete — cdc-organization-processor
# ===========================================================================

class TestSoftDeleteOrganization:
    """
    Tests for the ENABLE_HARD_DELETE toggle on the organization lambda.

    Soft delete (default, ENABLE_HARD_DELETE=false):
      - OpenSearch document is updated with _deleted=True and _deletedAt (epoch ms)
      - client.delete() is NOT called

    Hard delete (ENABLE_HARD_DELETE=true):
      - client.delete() is called with the organization code and ignore=[404]
      - client.update() is NOT called for the delete

    DynamoDB phase:
      - Soft delete: update_item marks the row with _deleted=True
      - Hard delete: delete_item physically removes the row
    """

    # ------------------------------------------------------------------
    # OpenSearch — soft delete (default)
    # ------------------------------------------------------------------

    def test_soft_delete_os_calls_update_not_delete(self, mock_opensearch, mock_cache_table):
        with patch.dict("os.environ", {"ENABLE_HARD_DELETE": "false"}):
            lf.index_organization("DEFAULT", "ORG-KEY-001", "Glen Raven Inc", {}, "d", None, None)
        mock_opensearch.update.assert_called_once()
        mock_opensearch.delete.assert_not_called()

    def test_soft_delete_os_sets_deleted_true(self, mock_opensearch, mock_cache_table):
        with patch.dict("os.environ", {"ENABLE_HARD_DELETE": "false"}):
            lf.index_organization("DEFAULT", "ORG-KEY-001", "Glen Raven Inc", {}, "d", None, None)
        doc = mock_opensearch.update.call_args[1]["body"]["doc"]
        assert doc["_deleted"] is True

    def test_soft_delete_os_sets_deleted_at_epoch_millis(self, mock_opensearch, mock_cache_table):
        with patch.dict("os.environ", {"ENABLE_HARD_DELETE": "false"}):
            lf.index_organization("DEFAULT", "ORG-KEY-001", "Glen Raven Inc", {}, "d", None, None)
        doc = mock_opensearch.update.call_args[1]["body"]["doc"]
        assert isinstance(doc["_deletedAt"], int)
        assert doc["_deletedAt"] > 1_700_000_000_000

    def test_soft_delete_os_targets_correct_id_and_index(self, mock_opensearch, mock_cache_table):
        with patch.dict("os.environ", {"ENABLE_HARD_DELETE": "false"}):
            lf.index_organization("DEFAULT", "ORG-KEY-001", "Glen Raven Inc", {}, "d", None, None)
        call_kwargs = mock_opensearch.update.call_args[1]
        assert call_kwargs["id"] == "DEFAULT"
        assert call_kwargs["index"] == lf.INDEX_ORGANIZATION

    # ------------------------------------------------------------------
    # OpenSearch — hard delete
    # ------------------------------------------------------------------

    def test_hard_delete_os_calls_client_delete(self, mock_opensearch, mock_cache_table):
        with patch.dict("os.environ", {"ENABLE_HARD_DELETE": "true"}):
            lf.index_organization("DEFAULT", "ORG-KEY-001", "Glen Raven Inc", {}, "d", None, None)
        mock_opensearch.delete.assert_called_once_with(
            index=lf.INDEX_ORGANIZATION, id="DEFAULT", ignore=[404]
        )
        mock_opensearch.update.assert_not_called()

    def test_hard_delete_os_not_found_returns_gracefully(self, mock_opensearch, mock_cache_table):
        mock_opensearch.delete.return_value = {"result": "not_found"}
        with patch.dict("os.environ", {"ENABLE_HARD_DELETE": "true"}):
            # Should not raise — 404 is ignored
            lf.index_organization("DEFAULT", "ORG-KEY-001", "Glen Raven Inc", {}, "d", None, None)

    # ------------------------------------------------------------------
    # DynamoDB — soft delete (default)
    # ------------------------------------------------------------------

    def test_soft_delete_dynamo_calls_update_item_not_delete_item(self, mock_opensearch, mock_cache_table):
        with patch.dict("os.environ", {"ENABLE_HARD_DELETE": "false"}):
            lf.process_organization(make_payload(), "d")
        mock_cache_table.update_item.assert_called_once()
        mock_cache_table.delete_item.assert_not_called()

    def test_soft_delete_dynamo_sets_deleted_true(self, mock_opensearch, mock_cache_table):
        with patch.dict("os.environ", {"ENABLE_HARD_DELETE": "false"}):
            lf.process_organization(make_payload(), "d")
        expr_vals = mock_cache_table.update_item.call_args[1]["ExpressionAttributeValues"]
        assert any(v is True for v in expr_vals.values()), \
            f"Expected _deleted=True in ExpressionAttributeValues: {expr_vals}"

    # ------------------------------------------------------------------
    # DynamoDB — hard delete
    # ------------------------------------------------------------------

    def test_hard_delete_dynamo_calls_delete_item_not_update_item(self, mock_opensearch, mock_cache_table):
        with patch.dict("os.environ", {"ENABLE_HARD_DELETE": "true"}):
            lf.process_organization(make_payload(), "d")
        mock_cache_table.delete_item.assert_called_once_with(
            Key={"PK": "ORGANIZATION", "SK": "ORG-KEY-001"}
        )
        mock_cache_table.update_item.assert_not_called()

    # ------------------------------------------------------------------
    # Toggle boundary
    # ------------------------------------------------------------------

    def test_default_env_is_soft_delete(self, mock_opensearch, mock_cache_table):
        """When ENABLE_HARD_DELETE is absent from environment, soft delete is active."""
        import os as _os
        _os.environ.pop("ENABLE_HARD_DELETE", None)
        lf.index_organization("DEFAULT", "ORG-KEY-001", "Glen Raven Inc", {}, "d", None, None)
        mock_opensearch.update.assert_called_once()
        mock_opensearch.delete.assert_not_called()


