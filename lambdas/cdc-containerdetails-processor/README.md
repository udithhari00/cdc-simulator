# cdc-containerdetails-processor

CDC Lambda for **YFS_CONTAINER_DETAILS** — processes container details CDC events from Kafka (MSK).

## What it does

1. **Phase 1** — Caches container details to DynamoDB:
   - Forward row: `PK=YFS_CONTAINER_DETAILS, SK=<SHIPMENT_CONTAINER_DETAILS_KEY>`
   - Reverse row: `PK=YFS_CONTAINER_DETAILS_BY_SHIPMENT_LINE, SK=<SHIPMENT_LINE_KEY>#<SHIPMENT_CONTAINER_DETAILS_KEY>`
2. **Phase 2** — Re-derives the `TRACKING_NO` cache for the affected order line by walking the relationship: Order Line -> Shipment Line -> Container Details -> Child Container -> Parent Container

## DynamoDB Access Patterns

| Operation | PK | SK |
|---|---|---|
| Write | `YFS_CONTAINER_DETAILS` | `<SHIPMENT_CONTAINER_DETAILS_KEY>` |
| Write | `YFS_CONTAINER_DETAILS_BY_SHIPMENT_LINE` | `<SHIPMENT_LINE_KEY>#<SHIPMENT_CONTAINER_DETAILS_KEY>` |
| Read | `YFS_SHIPMENT_LINE` | `<SHIPMENT_LINE_KEY>` |
| Query | `YFS_SHIPMENT_LINE_BY_ORDER_LINE` | `begins_with(<ORDER_LINE_KEY>#)` |
| Read | `YFS_SHIPMENT_CONTAINER` | `<SHIPMENT_CONTAINER_KEY>` |
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
  "table": "YFS_CONTAINER_DETAILS",
  "operation": "c",
  "payload": {
    "SHIPMENT_CONTAINER_DETAILS_KEY": "SCD-001",
    "SHIPMENT_LINE_KEY": "SL-001",
    "SHIPMENT_CONTAINER_KEY": "SC-001",
    "QUANTITY": 10
  }
}
```

## Running Tests

```bash
pytest test_lambda_function.py -v --cov=lambda_function --cov-report=term-missing
```
