import base64
import json
import logging
import os
import signal
import sys
import time
from pathlib import Path
from typing import Any, Dict, Optional

import yaml
from kafka import KafkaConsumer

from .lambda_loader import LambdaRegistry

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("cdc_consumer")


class CDCConsumerWorker:
    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        rules_path: Optional[str] = None,
        group_id: str = "cdc-local-worker-group",
    ):
        self.bootstrap_servers = bootstrap_servers or os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        self.rules_path = rules_path or os.environ.get(
            "ROUTING_RULES_PATH", str(Path(__file__).resolve().parent.parent / "routing_rules.yaml")
        )
        self.group_id = group_id
        self.topic_map: Dict[str, Dict[str, Any]] = {}
        self.registry = LambdaRegistry()
        self.consumer: Optional[KafkaConsumer] = None
        self.running = False
        self._load_rules()

    def _load_rules(self):
        p = Path(self.rules_path)
        if not p.exists():
            raise FileNotFoundError(f"Routing rules file not found: {p}")

        with open(p, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}

        prefix = data.get("topic_prefix", "local-cdc")
        schema = data.get("schema", "STERLING")

        for m in data.get("mappings", []):
            table = m["table"].upper()
            topic = m.get("topic") or f"{prefix}.{schema}.{table}"
            processor = m.get("processor")
            self.topic_map[topic] = {
                "table": table,
                "processor": processor,
                "primary_key": m.get("primary_key"),
                "target_index": m.get("target_index"),
            }

        logger.info(f"Loaded {len(self.topic_map)} topic subscriptions.")

    def start(self):
        topics = list(self.topic_map.keys())
        if not topics:
            logger.warning("No topics configured to subscribe to.")
            return

        logger.info(f"Connecting to Kafka {self.bootstrap_servers}...")
        retries = 30
        while retries > 0:
            try:
                self.consumer = KafkaConsumer(
                    *topics,
                    bootstrap_servers=self.bootstrap_servers.split(","),
                    group_id=self.group_id,
                    auto_offset_reset="earliest",
                    enable_auto_commit=True,
                    auto_commit_interval_ms=1000,
                    consumer_timeout_ms=1000,
                    value_deserializer=None,  # raw bytes to encode to base64
                    key_deserializer=None,
                )
                logger.info(f"Subscribed to {len(topics)} topics successfully: {topics}")
                break
            except Exception as e:
                logger.warning(f"Kafka connection attempt failed ({e}). Retrying in 2 seconds...")
                time.sleep(2)
                retries -= 1

        if not self.consumer:
            raise RuntimeError("Could not connect to Kafka after multiple retries.")

        self.running = True
        logger.info("CDC Consumer Worker is now actively polling for messages...")

        while self.running:
            try:
                # poll returns dict: {TopicPartition: [ConsumerRecord]}
                records = self.consumer.poll(timeout_ms=1000)
                if not records:
                    continue

                # Collect all messages and sort by timestamp to preserve multi-table CDC transaction order
                all_messages = []
                for tp, messages in records.items():
                    all_messages.extend(messages)
                all_messages.sort(key=lambda m: getattr(m, "timestamp", 0) or 0)

                for msg in all_messages:
                    self._process_message(msg)

            except Exception as e:
                logger.error(f"Error in consumer polling loop: {e}", exc_info=True)
                time.sleep(1)

    def _process_message(self, msg):
        topic = msg.topic
        topic_info = self.topic_map.get(topic)
        if not topic_info:
            logger.warning(f"Received message from unmapped topic: {topic}")
            return

        processor_name = topic_info.get("processor")
        if not processor_name:
            logger.warning(f"No processor registered for topic: {topic}")
            return

        try:
            lambda_module = self.registry.get_processor(processor_name)
        except Exception as e:
            logger.error(f"Failed to load processor {processor_name}: {e}")
            return

        # Prepare MSK-compatible event structure
        val_bytes = msg.value if msg.value is not None else b""
        val_b64 = base64.b64encode(val_bytes).decode("ascii")

        key_b64 = base64.b64encode(msg.key).decode("ascii") if msg.key is not None else None

        tp_key = f"{topic}-{msg.partition}"
        msk_event = {
            "eventSource": "aws:kafka",
            "records": {
                tp_key: [
                    {
                        "topic": topic,
                        "partition": msg.partition,
                        "offset": msg.offset,
                        "timestamp": msg.timestamp,
                        "timestampType": "CREATE_TIME",
                        "key": key_b64,
                        "value": val_b64,
                    }
                ]
            },
        }

        try:
            # Special handling for product processors that accept direct JSON payload via URL
            if processor_name == "cdc-product-processor":
                raw_json = json.loads(val_bytes.decode("utf-8"))
                operation = raw_json.get("op", "c")
                payload = raw_json.get("after") or raw_json.get("payload") or raw_json
                res = lambda_module.lambda_handler({"body": {"operation": operation, "payload": payload}}, None)
            elif processor_name == "cdc-product-addon-processor":
                raw_json = json.loads(val_bytes.decode("utf-8"))
                operation = raw_json.get("op", "u")
                payload = raw_json.get("after") or raw_json.get("payload") or raw_json
                res = lambda_module.lambda_handler({"body": {"operation": operation, "payload": {"items": [payload]}}}, None)
            else:
                res = lambda_module.lambda_handler(msk_event, None)

            logger.info(f"Processed msg | topic={topic} partition={msg.partition} offset={msg.offset} res={res}")
        except Exception as e:
            logger.error(f"Execution failed in {processor_name} for topic={topic}: {e}", exc_info=True)

    def stop(self):
        logger.info("Stopping CDC Consumer Worker...")
        self.running = False
        if self.consumer:
            self.consumer.close()
            self.consumer = None
        logger.info("CDC Consumer Worker stopped.")


def main():
    worker = CDCConsumerWorker()

    def handle_exit(signum, frame):
        worker.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_exit)
    signal.signal(signal.SIGTERM, handle_exit)

    worker.start()


if __name__ == "__main__":
    main()

