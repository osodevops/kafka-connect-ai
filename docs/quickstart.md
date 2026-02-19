# kafka-connect-ai Quick Start Guide

Get a kafka-connect-ai connector running in under 10 minutes using Docker Compose.

## Prerequisites

- Docker and Docker Compose
- An LLM API key (Anthropic or OpenAI)
- The kafka-connect-ai uber JAR (built or downloaded)

## 1. Build the Uber JAR

```bash
mvn clean package -pl kafka-connect-ai-connect -am -DskipTests
```

The uber JAR is produced at `kafka-connect-ai-connect/target/kafka-connect-ai-connect-0.1.0-SNAPSHOT-all.jar`.

## 2. Start the Stack

From the repository root:

```bash
cd docker
docker compose up -d
```

This starts:

| Service | Port | Purpose |
|---------|------|---------|
| Kafka (KRaft) | 9092 | Message broker |
| Schema Registry | 8081 | Schema management |
| Kafka Connect | 8083 | Connector runtime (with kafka-connect-ai plugin) |
| PostgreSQL | 5432 | Database for JDBC examples |
| Redis Stack | 6379, 8001 | Semantic cache (optional) |

Wait for all services to become healthy:

```bash
docker compose ps
```

## 3. Verify kafka-connect-ai is Loaded

```bash
curl -s http://localhost:8083/connector-plugins | python3 -m json.tool
```

You should see both connector classes:

```json
[
  {
    "class": "sh.oso.connect.ai.connect.source.AiSourceConnector",
    "type": "source",
    "version": "0.1.0"
  },
  {
    "class": "sh.oso.connect.ai.connect.sink.AiSinkConnector",
    "type": "sink",
    "version": "0.1.0"
  }
]
```

## 4. Deploy a Source Connector

### Example: HTTP API to Kafka

Ingest data from a REST API, transform it with an LLM, and write to a Kafka topic.

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "weather-source",
    "config": {
      "connector.class": "sh.oso.connect.ai.connect.source.AiSourceConnector",
      "tasks.max": "1",
      "connect.ai.source.adapter": "http",
      "connect.ai.topic": "weather-events",
      "connect.ai.batch.size": "50",

      "http.source.url": "https://api.open-meteo.com/v1/forecast?latitude=51.5&longitude=-0.1&hourly=temperature_2m",
      "http.source.method": "GET",
      "http.source.headers": "Accept: application/json",
      "http.source.poll.interval.ms": "300000",
      "http.source.response.content.path": "hourly",

      "ai.llm.provider": "anthropic",
      "ai.llm.api.key": "YOUR_API_KEY",
      "ai.llm.model": "claude-sonnet-4-20250514",
      "ai.agent.system.prompt": "Transform weather data into structured events with fields: timestamp, temperature_celsius, location.",
      "ai.agent.target.schema": "{\"type\":\"object\",\"properties\":{\"timestamp\":{\"type\":\"string\"},\"temperature_celsius\":{\"type\":\"number\"},\"location\":{\"type\":\"string\"}},\"required\":[\"timestamp\",\"temperature_celsius\",\"location\"]}"
    }
  }'
```

### Example: PostgreSQL CDC to Kafka

Capture changes from a PostgreSQL table using incremental timestamp queries.

```bash
# Create a sample table first
docker exec kcai-postgres psql -U kcai -d kcai -c "
CREATE TABLE orders (
  id SERIAL PRIMARY KEY,
  customer TEXT NOT NULL,
  amount NUMERIC(10,2) NOT NULL,
  status TEXT DEFAULT 'pending',
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW()
);
INSERT INTO orders (customer, amount, status) VALUES
  ('Acme Corp', 1500.00, 'shipped'),
  ('Widgets Inc', 320.50, 'pending'),
  ('Tech Labs', 8900.00, 'delivered');
"

# Deploy the connector
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "orders-source",
    "config": {
      "connector.class": "sh.oso.connect.ai.connect.source.AiSourceConnector",
      "tasks.max": "1",
      "connect.ai.source.adapter": "jdbc",
      "connect.ai.topic": "order-events",

      "jdbc.url": "jdbc:postgresql://postgres:5432/kcai",
      "jdbc.user": "kcai",
      "jdbc.password": "kcai",
      "jdbc.table": "orders",
      "jdbc.query.mode": "timestamp",
      "jdbc.timestamp.column": "updated_at",
      "jdbc.poll.interval.ms": "5000",

      "ai.llm.provider": "anthropic",
      "ai.llm.api.key": "YOUR_API_KEY",
      "ai.llm.model": "claude-sonnet-4-20250514",
      "ai.agent.system.prompt": "Normalise order records into events with fields: order_id, customer_name, amount_usd, order_status, event_timestamp."
    }
  }'
```

## 5. Deploy a Sink Connector

### Example: Kafka to HTTP API

Forward transformed records from a Kafka topic to an external API.

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "webhook-sink",
    "config": {
      "connector.class": "sh.oso.connect.ai.connect.sink.AiSinkConnector",
      "tasks.max": "1",
      "connect.ai.sink.adapter": "http",
      "topics": "order-events",

      "http.sink.url": "https://webhook.site/your-unique-id",
      "http.sink.method": "POST",
      "http.sink.headers": "Content-Type: application/json",
      "http.sink.batch.size": "10",
      "http.sink.retry.max": "3",

      "ai.llm.provider": "anthropic",
      "ai.llm.api.key": "YOUR_API_KEY",
      "ai.llm.model": "claude-sonnet-4-20250514",
      "ai.agent.system.prompt": "Transform order events into webhook payloads with fields: event_type, payload, timestamp."
    }
  }'
```

## 6. Monitor Connector Status

```bash
# List all connectors
curl -s http://localhost:8083/connectors | python3 -m json.tool

# Check connector status
curl -s http://localhost:8083/connectors/weather-source/status | python3 -m json.tool

# View connector config
curl -s http://localhost:8083/connectors/weather-source/config | python3 -m json.tool
```

## 7. Consume Transformed Records

```bash
docker exec kcai-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic weather-events \
  --from-beginning \
  --max-messages 5
```

## 8. Pause, Resume, and Delete

```bash
# Pause
curl -X PUT http://localhost:8083/connectors/weather-source/pause

# Resume
curl -X PUT http://localhost:8083/connectors/weather-source/resume

# Delete
curl -X DELETE http://localhost:8083/connectors/weather-source
```

## 9. Tear Down

```bash
cd docker
docker compose down -v
```

## Next Steps

- [Configuration Reference](configuration.md) — full list of all config properties
- [Use Cases](use-cases.md) — common integration patterns
- [Deployment Guide](deployment.md) — production deployment options
- [Troubleshooting](troubleshooting.md) — common issues and fixes
