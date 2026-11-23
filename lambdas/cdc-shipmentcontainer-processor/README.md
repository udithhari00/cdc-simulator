# cdc-shipmentcontainer-processor

CDC Lambda for **YFS_SHIPMENT_CONTAINER** — processes shipment container CDC events from Kafka (MSK).

## What it does

1. **Phase 1** — Caches container data to DynamoDB:
   - Forward row: `PK=YFS_SHIPMENT_CONTAINER, SK=<SHIPMENT_CONTAINER_KEY>`
   - Reverse row: `PK=YFS_SHIPMENT_CONTAINER_BY_SHIPMENT, SK=<SHIPMENT_KEY>#<SHIPMENT_CONTAINER_KEY>`
2. **Phase 2** — Re-derives the `TRACKING_NO` cache for all order lines linked to this container's shipment

## DynamoDB Access Patterns

| Operation | PK | SK |
|---|---|---|
| Write | `YFS_SHIPMENT_CONTAINER` | `<SHIPMENT_CONTAINER_KEY>` |
| Write | `YFS_SHIPMENT_CONTAINER_BY_SHIPMENT` | `<SHIPMENT_KEY>#<SHIPMENT_CONTAINER_KEY>` |
| Query | `YFS_SHIPMENT_LINE_BY_SHIPMENT` | `begins_with(<SHIPMENT_KEY>#)` |
| Query | `YFS_SHIPMENT_LINE_BY_ORDER_LINE` | `begins_with(<ORDER_LINE_KEY>#)` |
| Read | `YFS_SHIPMENT` | `<SHIPMENT_KEY>` |
| Write | `TRACKING_NO` | `<ORDER_LINE_KEY>` |

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `DYNAMODB_TABLE` | `enrichment_cache` | DynamoDB table name |
| `DYNAMODB_ENDPOINT` | _(none)_ | Local DynamoDB endpoint |
| `AWS_REGION` | `us-east-1` | AWS region |
| `ENABLE_HARD_DELETE` | `false` | Use hard deletes instead of soft deletes |
| `LOG_LEVEL` | `INFO` | Logging level |

## Direct Invocation (Test)

```json
{
  "table": "YFS_SHIPMENT_CONTAINER",
  "operation": "c",
  "payload": {
    "SHIPMENT_CONTAINER_KEY": "SC-001",
    "SHIPMENT_KEY": "SHP-001",
    "TRACKING_NO": "1Z999AA10123456784",
    "CONTAINER_NO": "CTN-001",
    "CONTAINER_TYPE": "CASE"
  }
}
```

## Running Tests

```bash
pytest test_lambda_function.py -v --cov=lambda_function --cov-report=term-missing
```
