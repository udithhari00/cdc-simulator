#!/usr/bin/env python3
"""
init_environment.py
===================
Auto-initializes:
1. DynamoDB table `enrichment_cache` (PK/SK) in DynamoDB Local
2. OpenSearch 3.x indices (orders, order-lines, organizations, products) using schemas in index/*.json
"""

import json
import logging
import os
import sys
import time
from pathlib import Path

import boto3
from botocore.exceptions import ClientError, EndpointConnectionError
from opensearchpy import OpenSearch, RequestsHttpConnection

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("bootstrap")

# Config from environment
DYNAMODB_ENDPOINT = os.environ.get("DYNAMODB_ENDPOINT", "http://dynamodb-local:8000")
DYNAMODB_TABLE = os.environ.get("DYNAMODB_TABLE", "enrichment_cache")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

OPENSEARCH_HOST = os.environ.get("OPENSEARCH_HOST", "opensearch")
OPENSEARCH_PORT = int(os.environ.get("OPENSEARCH_PORT", "9200"))
OPENSEARCH_SCHEME = os.environ.get("OPENSEARCH_SCHEME", "http")
OPENSEARCH_USERNAME = os.environ.get("OPENSEARCH_USERNAME", "admin")
OPENSEARCH_PASSWORD = os.environ.get("OPENSEARCH_PASSWORD", "admin")
OPENSEARCH_AUTH = os.environ.get("OPENSEARCH_AUTH", "basic")

INDEX_DIR = os.environ.get("INDEX_DIR", "/app/index")


def wait_for_dynamodb(max_retries=30, delay=2):
    logger.info(f"Connecting to DynamoDB at {DYNAMODB_ENDPOINT}...")
    client = boto3.client(
        "dynamodb",
        region_name=AWS_REGION,
        endpoint_url=DYNAMODB_ENDPOINT,
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "local"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "local"),
    )
    for i in range(1, max_retries + 1):
        try:
            client.list_tables()
            logger.info("DynamoDB is ready.")
            return client
        except (ClientError, EndpointConnectionError, Exception) as e:
            logger.info(f"Waiting for DynamoDB (attempt {i}/{max_retries}): {e}")
            time.sleep(delay)
    raise RuntimeError("Timed out waiting for DynamoDB")


def create_dynamodb_table(client):
    try:
        tables = client.list_tables().get("TableNames", [])
        if DYNAMODB_TABLE in tables:
            logger.info(f"DynamoDB table '{DYNAMODB_TABLE}' already exists.")
            return

        logger.info(f"Creating DynamoDB table '{DYNAMODB_TABLE}'...")
        client.create_table(
            TableName=DYNAMODB_TABLE,
            KeySchema=[
                {"AttributeName": "PK", "KeyType": "HASH"},
                {"AttributeName": "SK", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "PK", "AttributeType": "S"},
                {"AttributeName": "SK", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        logger.info(f"Table '{DYNAMODB_TABLE}' created successfully.")
    except Exception as e:
        logger.error(f"Error creating DynamoDB table: {e}")
        raise


def wait_for_opensearch(max_retries=30, delay=2):
    logger.info(f"Connecting to OpenSearch at {OPENSEARCH_SCHEME}://{OPENSEARCH_HOST}:{OPENSEARCH_PORT}...")
    client = OpenSearch(
        hosts=[{"host": OPENSEARCH_HOST, "port": OPENSEARCH_PORT}],
        http_auth=(OPENSEARCH_USERNAME, OPENSEARCH_PASSWORD) if OPENSEARCH_AUTH == "basic" else None,
        use_ssl=(OPENSEARCH_SCHEME == "https"),
        verify_certs=False,
        connection_class=RequestsHttpConnection,
        timeout=120,
    )
    for i in range(1, max_retries + 1):
        try:
            info = client.info()
            version = info.get("version", {}).get("number", "unknown")
            logger.info(f"OpenSearch is ready (version: {version}).")
            return client
        except Exception as e:
            logger.info(f"Waiting for OpenSearch (attempt {i}/{max_retries}): {e}")
            time.sleep(delay)
    raise RuntimeError("Timed out waiting for OpenSearch")


def initialize_opensearch_indices(client):
    index_files = {
        "orders": "orders.json",
        "order-lines": "order-lines.json",
        "organizations": "organizations.json",
        "products": "products.json",
    }

    base_path = Path(INDEX_DIR)
    if not base_path.exists():
        # Fallback to local repo index path if running outside docker
        base_path = Path(__file__).resolve().parent.parent.parent / "index"

    logger.info(f"Loading index definitions from {base_path}...")

    for index_name, filename in index_files.items():
        file_path = base_path / filename
        if not file_path.exists():
            logger.warning(f"Index schema file not found: {file_path}, skipping.")
            continue

        try:
            if client.indices.exists(index=index_name):
                logger.info(f"OpenSearch index '{index_name}' already exists.")
                continue

            with open(file_path, "r", encoding="utf-8") as f:
                body = json.load(f)

            # In single-node local environment, set number_of_replicas to 0 for green health
            if "settings" in body and "index" in body["settings"]:
                body["settings"]["index"]["number_of_replicas"] = 0

            logger.info(f"Creating OpenSearch index '{index_name}' from {filename}...")
            client.indices.create(index=index_name, body=body)
            logger.info(f"Index '{index_name}' created successfully.")
        except Exception as e:
            logger.error(f"Failed to create index '{index_name}': {e}")
            raise


def main():
    logger.info("=== STARTING ENVIRONMENT BOOTSTRAP ===")
    ddb_client = wait_for_dynamodb()
    create_dynamodb_table(ddb_client)

    os_client = wait_for_opensearch()
    initialize_opensearch_indices(os_client)
    logger.info("=== ENVIRONMENT BOOTSTRAP COMPLETED SUCCESSFULLY ===")


if __name__ == "__main__":
    main()

