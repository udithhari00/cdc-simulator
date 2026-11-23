import logging
import os
import threading
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional

import boto3
from fastapi import FastAPI, HTTPException, UploadFile, File, Form
from opensearchpy import OpenSearch, RequestsHttpConnection
from pydantic import BaseModel, Field

from engine.base import CDCEvent
from engine.kafka_producer import LocalKafkaPublisher
from engine.adapters.mock_generator import MockGeneratorAdapter
from engine.adapters.file_adapter import FileSourceAdapter
from worker.consumer import CDCConsumerWorker

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("simulator_api")

# Global instances
publisher: Optional[LocalKafkaPublisher] = None
consumer_worker: Optional[CDCConsumerWorker] = None
consumer_thread: Optional[threading.Thread] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global publisher, consumer_worker, consumer_thread

    logger.info("Initializing Kafka Publisher...")
    publisher = LocalKafkaPublisher()

    logger.info("Starting background CDC Consumer Worker thread...")
    consumer_worker = CDCConsumerWorker()
    consumer_thread = threading.Thread(target=consumer_worker.start, daemon=True, name="KafkaConsumerWorkerThread")
    consumer_thread.start()

    yield

    logger.info("Shutting down background services...")
    if consumer_worker:
        consumer_worker.stop()
    if publisher:
        publisher.close()


app = FastAPI(
    title="OpenSearch CDC Ingestion Simulator",
    description="Pluggable simulation platform for streaming events through Kafka and CDC Lambdas to OpenSearch 3.x",
    version="1.0.0",
    lifespan=lifespan,
)


class ScenarioRequest(BaseModel):
    scenario: str = Field(
        default="orders",
        description="Scenario / index name: 'orders' (or 'order_lifecycle'), 'products' (or 'catalog'), 'organizations', 'bulk_orders'",
    )
    count: int = Field(default=5, description="Number of entities/orders to generate")
    line_count: int = Field(default=3, description="Order lines per order (for 'orders')")
    include_shipment: bool = Field(default=True, description="Whether to include shipments and containers")
    include_hold: bool = Field(default=False, description="Whether to include hold events")


class SingleEventRequest(BaseModel):
    table: str = Field(description="Table name, e.g. YFS_ORDER_HEADER, PRODUCT, etc.")
    operation: str = Field(default="c", description="Operation: 'c', 'u', 'd', 'r'")
    payload: Dict[str, Any] = Field(description="JSON row payload")
    key_field: Optional[str] = Field(default=None, description="Primary key field name")
    key_value: Optional[str] = Field(default=None, description="Primary key value")


@app.get("/health", tags=["System"])
def health_check():
    status = {"api": "healthy", "kafka": "unknown", "dynamodb": "unknown", "opensearch": "unknown"}

    # Kafka check
    try:
        if publisher and publisher.get_producer().bootstrap_connected():
            status["kafka"] = "healthy"
        else:
            status["kafka"] = "connecting"
    except Exception as e:
        status["kafka"] = f"unhealthy: {str(e)}"

    # DynamoDB check
    try:
        ddb = boto3.client(
            "dynamodb",
            region_name=os.environ.get("AWS_REGION", "us-east-1"),
            endpoint_url=os.environ.get("DYNAMODB_ENDPOINT", "http://dynamodb-local:8000"),
            aws_access_key_id="local",
            aws_secret_access_key="local",
        )
        tables = ddb.list_tables().get("TableNames", [])
        status["dynamodb"] = f"healthy (tables: {tables})"
    except Exception as e:
        status["dynamodb"] = f"unhealthy: {str(e)}"

    # OpenSearch check
    try:
        os_client = OpenSearch(
            hosts=[{"host": os.environ.get("OPENSEARCH_HOST", "opensearch"), "port": int(os.environ.get("OPENSEARCH_PORT", "9200"))}],
            http_auth=(os.environ.get("OPENSEARCH_USERNAME", "admin"), os.environ.get("OPENSEARCH_PASSWORD", "admin")),
            use_ssl=(os.environ.get("OPENSEARCH_SCHEME", "http") == "https"),
            verify_certs=False,
            connection_class=RequestsHttpConnection,
            timeout=5,
        )
        info = os_client.info()
        status["opensearch"] = f"healthy (version: {info.get('version', {}).get('number', 'unknown')})"
    except Exception as e:
        status["opensearch"] = f"unhealthy: {str(e)}"

    return status


@app.post("/api/v1/simulate/scenario", tags=["Simulation"])
def run_simulation_scenario(req: ScenarioRequest):
    """
    Executes a high-level simulation scenario, publishing generated events directly to Kafka.
    """
    if not publisher:
        raise HTTPException(status_code=500, detail="Kafka Publisher not initialized")

    generator = MockGeneratorAdapter()
    published_events = []

    try:
        events = generator.produce_events(
            scenario=req.scenario,
            count=req.count,
            line_count=req.line_count,
            include_shipment=req.include_shipment,
            include_hold=req.include_hold,
        )

        for event in events:
            res = publisher.publish_event(event, flush=False)
            published_events.append(res)

        publisher.get_producer().flush()

    except Exception as e:
        logger.error(f"Error during simulation execution: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

    return {
        "scenario": req.scenario,
        "total_published": len(published_events),
        "details": published_events[:50],  # preview first 50
    }


@app.post("/api/v1/ingest/event", tags=["Ingestion"])
def ingest_single_event(req: SingleEventRequest):
    """
    Ingest a single arbitrary event directly to Kafka, routed to its corresponding Lambda processor.
    """
    if not publisher:
        raise HTTPException(status_code=500, detail="Kafka Publisher not initialized")

    event = CDCEvent(
        table=req.table,
        operation=req.operation,
        payload=req.payload,
        key_field=req.key_field,
        key_value=req.key_value,
    )
    res = publisher.publish_event(event)
    return {"status": "success", "record": res}


@app.post("/api/v1/ingest/file", tags=["Ingestion"])
async def ingest_file(
    file: UploadFile = File(...),
    table: str = Form(...),
    operation: str = Form("c"),
    key_field: Optional[str] = Form(None),
):
    """
    Upload and ingest a CSV, JSON, or JSONL file into Kafka.
    """
    if not publisher:
        raise HTTPException(status_code=500, detail="Kafka Publisher not initialized")

    temp_path = f"/tmp/{file.filename}"
    with open(temp_path, "wb") as f:
        content = await file.read()
        f.write(content)

    adapter = FileSourceAdapter()
    published_count = 0
    try:
        for event in adapter.produce_events(file_path=temp_path, table=table, operation=operation, key_field=key_field):
            publisher.publish_event(event, flush=False)
            published_count += 1

        publisher.get_producer().flush()
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)

    return {"status": "success", "file": file.filename, "table": table, "published_count": published_count}


@app.get("/api/v1/metrics", tags=["System"])
def get_metrics():
    """
    Provides real-time document counts across OpenSearch indices and DynamoDB table.
    """
    results = {"indices": {}, "dynamodb_items": 0}

    # OpenSearch counts
    try:
        os_client = OpenSearch(
            hosts=[{"host": os.environ.get("OPENSEARCH_HOST", "opensearch"), "port": int(os.environ.get("OPENSEARCH_PORT", "9200"))}],
            http_auth=(os.environ.get("OPENSEARCH_USERNAME", "admin"), os.environ.get("OPENSEARCH_PASSWORD", "admin")),
            use_ssl=(os.environ.get("OPENSEARCH_SCHEME", "http") == "https"),
            verify_certs=False,
            connection_class=RequestsHttpConnection,
            timeout=5,
        )
        for idx in ["orders", "order-lines", "organizations", "products"]:
            try:
                res = os_client.count(index=idx)
                results["indices"][idx] = res.get("count", 0)
            except Exception:
                results["indices"][idx] = "Index not found or not initialized"
    except Exception as e:
        results["indices"] = f"OpenSearch connection error: {e}"

    # DynamoDB count
    try:
        ddb = boto3.client(
            "dynamodb",
            region_name=os.environ.get("AWS_REGION", "us-east-1"),
            endpoint_url=os.environ.get("DYNAMODB_ENDPOINT", "http://dynamodb-local:8000"),
            aws_access_key_id="local",
            aws_secret_access_key="local",
        )
        desc = ddb.describe_table(TableName=os.environ.get("DYNAMODB_TABLE", "enrichment_cache"))
        results["dynamodb_items"] = desc["Table"]["ItemCount"]
    except Exception as e:
        results["dynamodb_items"] = f"DynamoDB error: {e}"

    return results

