import json
import logging
import os
from pathlib import Path
from typing import Dict, Optional

import yaml
from kafka import KafkaProducer

from .base import CDCEvent

logger = logging.getLogger("kafka_producer")


class LocalKafkaPublisher:
    def __init__(self, bootstrap_servers: Optional[str] = None, rules_path: Optional[str] = None):
        self.bootstrap_servers = bootstrap_servers or os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        self.rules_path = rules_path or os.environ.get(
            "ROUTING_RULES_PATH", str(Path(__file__).resolve().parent.parent / "routing_rules.yaml")
        )
        self.table_to_topic: Dict[str, str] = {}
        self.table_to_pk: Dict[str, str] = {}
        self.topic_prefix = "local-cdc"
        self.schema = "STERLING"
        self._load_routing_rules()
        self._producer: Optional[KafkaProducer] = None

    def _load_routing_rules(self):
        p = Path(self.rules_path)
        if not p.exists():
            logger.warning(f"Routing rules file not found at {p}. Using default conventions.")
            return

        with open(p, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}

        self.topic_prefix = data.get("topic_prefix", "local-cdc")
        self.schema = data.get("schema", "STERLING")

        for mapping in data.get("mappings", []):
            table = mapping["table"].upper()
            topic = mapping.get("topic") or f"{self.topic_prefix}.{self.schema}.{table}"
            self.table_to_topic[table] = topic
            if "primary_key" in mapping:
                self.table_to_pk[table] = mapping["primary_key"]

        logger.info(f"Loaded {len(self.table_to_topic)} table-to-topic routing rules.")

    def get_topic_for_table(self, table: str) -> str:
        table_upper = table.upper()
        if table_upper in self.table_to_topic:
            return self.table_to_topic[table_upper]
        # Dynamic fallback for arbitrary unmapped tables
        return f"{self.topic_prefix}.{self.schema}.{table_upper}"

    def get_producer(self) -> KafkaProducer:
        if self._producer is not None:
            return self._producer

        logger.info(f"Connecting to Kafka at {self.bootstrap_servers}...")
        self._producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers.split(","),
            key_serializer=lambda k: json.dumps(k).encode("utf-8") if k is not None else None,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            retries=5,
            request_timeout_ms=10000,
        )
        logger.info("Kafka producer successfully connected.")
        return self._producer

    def publish_event(self, event: CDCEvent, flush: bool = True) -> dict:
        producer = self.get_producer()
        topic = self.get_topic_for_table(event.table)

        # Debezium-standard envelope format
        message_value = {
            "op": event.operation,
            "ts_ms": event.source_timestamp,
            "before": event.payload if event.operation == "d" else None,
            "after": event.payload if event.operation != "d" else None,
        }

        # Resolve message key
        pk_field = event.key_field or self.table_to_pk.get(event.table)
        key_val = event.key_value
        if not key_val and pk_field and pk_field in event.payload:
            key_val = event.payload[pk_field]

        message_key = {"schema": None, "payload": {pk_field or "ID": key_val}} if key_val else None

        future = producer.send(topic, key=message_key, value=message_value)
        if flush:
            producer.flush()
        record_metadata = future.get(timeout=10)

        return {
            "topic": record_metadata.topic,
            "partition": record_metadata.partition,
            "offset": record_metadata.offset,
            "table": event.table,
            "operation": event.operation,
            "key": key_val,
        }

    def close(self):
        if self._producer:
            self._producer.flush()
            self._producer.close()
            self._producer = None

