#!/bin/bash
set -e

export PYTHONPATH=/app:/app/local-simulator:/app/lambdas:$PYTHONPATH

echo "=== Running Environment Initialization ==="
python /app/local-simulator/bootstrap/init_environment.py

echo "=== Starting Ingestion Simulator Service & Consumer Worker ==="
exec uvicorn api.main:app --app-dir /app/local-simulator --host 0.0.0.0 --port 8001
