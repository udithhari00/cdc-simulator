FROM python:3.11-slim

WORKDIR /app

# System dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install python dependencies
COPY local-simulator/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY local-simulator /app/local-simulator
COPY index /app/index
COPY lambdas /app/lambdas

# Set executable permission on entrypoint
RUN chmod +x /app/local-simulator/entrypoint.sh

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app:/app/local-simulator:/app/lambdas

EXPOSE 8001

ENTRYPOINT ["/bin/bash", "/app/local-simulator/entrypoint.sh"]

