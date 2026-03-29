# Real-Time CDC Project Guide

This project demonstrates real-time Change Data Capture (CDC) from PostgreSQL to Kafka using Debezium, with both a Python consumer and a web dashboard for live event visualization.

## Architecture

### Components

- **PostgreSQL (`cdc-postgres`)**  
  Source database containing `ecommerce` tables and seed data.
- **Kafka (`cdc-kafka`)**  
  Event backbone where CDC messages are published to topics like `cdc.customers`, `cdc.orders`, etc.
- **Debezium Connect (`cdc-debezium`)**  
  Captures row-level changes from PostgreSQL and publishes CDC events to Kafka.
- **Kafka UI (`cdc-kafka-ui`)**  
  UI to inspect topics, messages, and Kafka Connect.
- **Python CDC Consumer (`cdc-consumer`)**  
  Subscribes to CDC topics and processes events with console/log output.
- **Dashboard (`cdc-dashboard`)**  
  Node.js + WebSocket app that streams CDC events to the browser in real time.

### Data Flow

1. App/data generator writes to PostgreSQL tables in schema `ecommerce`.
2. Debezium reads PostgreSQL WAL via logical replication (`pgoutput`).
3. Debezium emits CDC events into Kafka topics (prefixed with `cdc.`).
4. Consumer and dashboard subscribe to Kafka topics and process/render events.
5. Kafka UI lets you inspect topics and connector state.

## Port Mapping

- PostgreSQL: `localhost:5432`
- Kafka broker (host listener): `localhost:29092`
- Debezium Connect REST API: `localhost:8083`
- Kafka UI: `http://localhost:8080`
- Dashboard: `http://localhost:3000`

## Credentials and Important Defaults

- PostgreSQL database: `enterprise_db`
- PostgreSQL user: `cdc_user`
- PostgreSQL password: `cdc_password`
- Debezium connector config file: `debezium/postgres-connector.json`
- Docker network: `codewithyu`

## Prerequisites

- Docker Desktop (or Docker Engine + Compose plugin)
- Python 3.9+ (for local data generator)

## Quick Start Commands

Run from project root (`Real_time_CDC`).

### 1) Start all containers

```bash
docker compose -f docker-compose.yml up -d --build
```

### 2) Check services

```bash
docker compose -f docker-compose.yml ps
```

### 3) Register Debezium PostgreSQL connector

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  --data @debezium/postgres-connector.json
```

### 4) Verify connector status

```bash
curl http://localhost:8083/connectors/postgres-cdc-connector/status
```

### 5) Run data generator locally (produces DB changes)

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r scripts/requirements.txt
python scripts/generate-data.py --host localhost --port 5432 -i 2
```

### 6) Open UIs

- Kafka UI: `http://localhost:8080`
- CDC Dashboard: `http://localhost:3000`

## Useful Operational Commands

### View container logs

```bash
docker compose -f docker-compose.yml logs -f kafka
docker compose -f docker-compose.yml logs -f debezium
docker compose -f docker-compose.yml logs -f cdc-consumer
docker compose -f docker-compose.yml logs -f dashboard
```

### List Kafka topics from Kafka container

```bash
docker exec -it cdc-kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --list
```

### Check connector list

```bash
curl http://localhost:8083/connectors
```

### Restart services

```bash
docker compose -f docker-compose.yml restart kafka debezium cdc-consumer dashboard
```

### Stop everything

```bash
docker compose -f docker-compose.yml down
```

### Stop and remove volumes (fresh reset)

```bash
docker compose -f docker-compose.yml down -v
```

## Optional Local Runs (without Docker for app parts)

### Dashboard (Node.js)

```bash
cd dashboard
npm install
npm run start
```

### Consumer (Python)

```bash
cd consumer
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
KAFKA_BOOTSTRAP_SERVERS=localhost:29092 python consumer.py
```

## Troubleshooting

- If Debezium has no topics, ensure connector is registered and `postgres` is healthy.
- If dashboard shows no events, confirm data generator is running and Kafka has `cdc.*` topics.
- If local scripts fail to connect to Kafka, use `localhost:29092` (host listener), not `kafka:9092`.
- If schema/table errors occur, check that `postgres/init.sql` initialized successfully.

## Project Structure

- `docker-compose.yml` - Service orchestration
- `postgres/init.sql` - Schema, tables, seed, and CDC prerequisites
- `postgres/postgresql.conf` and `postgres/pg_hba.conf` - PostgreSQL replication/auth tuning
- `debezium/postgres-connector.json` - Connector definition
- `consumer/` - Python Kafka consumer app
- `dashboard/` - Node.js dashboard app (HTTP + WebSocket)
- `scripts/generate-data.py` - Synthetic data generator for CDC demo
