# kafka-connect-ai Use Cases

Common integration patterns and real-world scenarios for the kafka-connect-ai connector.

---

## 1. REST API Ingestion with Transformation

**Scenario:** Poll a third-party REST API, transform the response into a canonical schema, and publish to Kafka.

**Adapters:** HTTP Source

```json
{
  "name": "github-events-source",
  "config": {
    "connector.class": "sh.oso.connect.ai.connect.source.AiSourceConnector",
    "tasks.max": "1",
    "connect.ai.source.adapter": "http",
    "connect.ai.topic": "github-events",

    "http.source.url": "https://api.github.com/repos/osodevops/kafka-connect-ai/events",
    "http.source.method": "GET",
    "http.source.headers": "Accept: application/vnd.github.v3+json",
    "http.source.poll.interval.ms": "120000",
    "http.auth.type": "bearer",
    "http.auth.bearer.token": "ghp_...",
    "http.pagination.type": "link_header",

    "ai.llm.provider": "anthropic",
    "ai.llm.api.key": "sk-ant-...",
    "ai.llm.model": "claude-sonnet-4-20250514",
    "ai.agent.system.prompt": "Extract GitHub events into: event_id, event_type, actor_login, repo_name, created_at. Discard webhook metadata.",
    "ai.agent.target.schema": "{\"type\":\"object\",\"properties\":{\"event_id\":{\"type\":\"string\"},\"event_type\":{\"type\":\"string\"},\"actor_login\":{\"type\":\"string\"},\"repo_name\":{\"type\":\"string\"},\"created_at\":{\"type\":\"string\"}},\"required\":[\"event_id\",\"event_type\",\"actor_login\",\"repo_name\",\"created_at\"]}"
  }
}
```

**Key features used:** Bearer authentication, Link header pagination, schema enforcement.

---

## 2. Database CDC to Kafka

**Scenario:** Capture changes from a PostgreSQL table and stream normalised events to Kafka.

**Adapters:** JDBC Source

```json
{
  "name": "customer-cdc",
  "config": {
    "connector.class": "sh.oso.connect.ai.connect.source.AiSourceConnector",
    "tasks.max": "1",
    "connect.ai.source.adapter": "jdbc",
    "connect.ai.topic": "customer-events",

    "jdbc.url": "jdbc:postgresql://db-primary:5432/app",
    "jdbc.user": "cdc_reader",
    "jdbc.password": "...",
    "jdbc.table": "customers",
    "jdbc.query.mode": "timestamp+incrementing",
    "jdbc.timestamp.column": "updated_at",
    "jdbc.incrementing.column": "id",
    "jdbc.poll.interval.ms": "5000",

    "ai.llm.provider": "anthropic",
    "ai.llm.api.key": "sk-ant-...",
    "ai.llm.model": "claude-sonnet-4-20250514",
    "ai.agent.system.prompt": "Transform customer database rows into event payloads with: customer_id, full_name, email, tier (derive from total_spend: <1000=bronze, <10000=silver, >=10000=gold), last_updated_iso."
  }
}
```

**Key features used:** Timestamp+incrementing query mode for gapless CDC, LLM-derived computed fields.

---

## 3. Kafka-to-Kafka Transformation (ETL Bridge)

**Scenario:** Consume from one Kafka cluster, transform records, and produce to another topic on the Connect cluster.

**Adapters:** Kafka Source

```json
{
  "name": "legacy-bridge",
  "config": {
    "connector.class": "sh.oso.connect.ai.connect.source.AiSourceConnector",
    "tasks.max": "2",
    "connect.ai.source.adapter": "kafka",
    "connect.ai.topic": "normalised-events",

    "kafka.source.bootstrap.servers": "legacy-kafka-1:9092,legacy-kafka-2:9092",
    "kafka.source.topics": "raw-events-v1,raw-events-v2",
    "kafka.source.group.id": "connect-ai-bridge",
    "kafka.source.consumer.auto.offset.reset": "earliest",

    "ai.llm.provider": "openai",
    "ai.llm.api.key": "sk-...",
    "ai.llm.model": "gpt-4o-mini",
    "ai.agent.system.prompt": "Merge legacy event formats v1 and v2 into a unified schema: event_id, event_type, source_system, payload (nested object), occurred_at (ISO 8601).",
    "ai.agent.target.schema": "{\"type\":\"object\",\"properties\":{\"event_id\":{\"type\":\"string\"},\"event_type\":{\"type\":\"string\"},\"source_system\":{\"type\":\"string\"},\"payload\":{\"type\":\"object\"},\"occurred_at\":{\"type\":\"string\"}},\"required\":[\"event_id\",\"event_type\",\"source_system\",\"occurred_at\"]}"
  }
}
```

**Key features used:** Cross-cluster consumption, consumer pass-through config, multi-topic subscription.

---

## 4. Kafka to External HTTP Webhook

**Scenario:** Forward processed events from a Kafka topic to a third-party webhook or API.

**Adapters:** HTTP Sink

```json
{
  "name": "slack-webhook-sink",
  "config": {
    "connector.class": "sh.oso.connect.ai.connect.sink.AiSinkConnector",
    "tasks.max": "1",
    "connect.ai.sink.adapter": "http",
    "topics": "alert-events",

    "http.sink.url": "https://hooks.slack.com/services/T00/B00/xxxx",
    "http.sink.method": "POST",
    "http.sink.headers": "Content-Type: application/json",
    "http.sink.batch.size": "1",
    "http.sink.retry.max": "3",
    "http.rate.limit.rps": "1.0",

    "ai.llm.provider": "anthropic",
    "ai.llm.api.key": "sk-ant-...",
    "ai.llm.model": "claude-haiku-4-5-20251001",
    "ai.agent.system.prompt": "Transform alert events into Slack message format: {\"text\": \"<summary of the alert>\"}"
  }
}
```

**Key features used:** HTTP sink with rate limiting, fast model for simple transformations.

---

## 5. Kafka to Database Materialisation

**Scenario:** Consume transformed events and materialise them into a PostgreSQL table.

**Adapters:** JDBC Sink

```json
{
  "name": "orders-materialise",
  "config": {
    "connector.class": "sh.oso.connect.ai.connect.sink.AiSinkConnector",
    "tasks.max": "1",
    "connect.ai.sink.adapter": "jdbc",
    "topics": "order-events",

    "jdbc.url": "jdbc:postgresql://analytics-db:5432/warehouse",
    "jdbc.user": "writer",
    "jdbc.password": "...",
    "jdbc.table": "dim_orders",
    "jdbc.sink.insert.mode": "upsert",
    "jdbc.sink.pk.columns": "order_id",
    "jdbc.sink.auto.ddl": "true",
    "jdbc.batch.size": "500",

    "ai.llm.provider": "anthropic",
    "ai.llm.api.key": "sk-ant-...",
    "ai.llm.model": "claude-sonnet-4-20250514",
    "ai.agent.system.prompt": "Transform order events into dimension table rows: order_id, customer_name, total_amount, currency, status, region (derive from shipping_address), created_date, updated_date."
  }
}
```

**Key features used:** Upsert mode with primary key, auto-DDL, batch writes.

---

## 6. Multi-API Aggregation

**Scenario:** Ingest from multiple APIs using separate connectors, merge into a unified topic.

Deploy multiple source connectors writing to the same topic:

```bash
# Connector 1: Weather data
curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d '{
  "name": "weather-ingest",
  "config": {
    "connector.class": "sh.oso.connect.ai.connect.source.AiSourceConnector",
    "connect.ai.source.adapter": "http",
    "connect.ai.topic": "environmental-events",
    "http.source.url": "https://api.weather.example.com/current",
    "http.source.poll.interval.ms": "300000",
    "ai.llm.provider": "anthropic",
    "ai.llm.api.key": "sk-ant-...",
    "ai.agent.system.prompt": "Normalise into: source=weather, metric_name, metric_value, unit, location, timestamp."
  }
}'

# Connector 2: Air quality data
curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d '{
  "name": "airquality-ingest",
  "config": {
    "connector.class": "sh.oso.connect.ai.connect.source.AiSourceConnector",
    "connect.ai.source.adapter": "http",
    "connect.ai.topic": "environmental-events",
    "http.source.url": "https://api.airquality.example.com/latest",
    "http.source.poll.interval.ms": "600000",
    "ai.llm.provider": "anthropic",
    "ai.llm.api.key": "sk-ant-...",
    "ai.agent.system.prompt": "Normalise into: source=airquality, metric_name, metric_value, unit, location, timestamp."
  }
}'
```

**Key features used:** Multiple connectors writing to the same topic, consistent schema via system prompt.

---

## 7. Cost-Optimised High-Throughput Pipeline

**Scenario:** Process large volumes with minimal LLM cost using model routing, caching, and deterministic transforms.

```json
{
  "name": "high-throughput-ingest",
  "config": {
    "connector.class": "sh.oso.connect.ai.connect.source.AiSourceConnector",
    "tasks.max": "4",
    "connect.ai.source.adapter": "jdbc",
    "connect.ai.topic": "processed-records",
    "connect.ai.batch.size": "200",

    "jdbc.url": "jdbc:postgresql://db:5432/source",
    "jdbc.table": "events",
    "jdbc.query.mode": "timestamp",
    "jdbc.timestamp.column": "created_at",
    "jdbc.poll.interval.ms": "2000",

    "ai.llm.provider": "anthropic",
    "ai.llm.api.key": "sk-ant-...",
    "ai.llm.model": "claude-sonnet-4-20250514",
    "ai.agent.system.prompt": "Transform events into canonical format with: id, type, data, timestamp.",

    "ai.batch.size": "50",
    "ai.batch.max.wait.ms": "1000",
    "ai.batch.parallel.calls": "4",
    "ai.call.timeout.seconds": "60",

    "ai.router.enabled": "true",
    "ai.router.deterministic.patterns": "field_rename,type_cast,timestamp_format",
    "ai.llm.model.fast": "claude-haiku-4-5-20251001",
    "ai.llm.model.powerful": "claude-opus-4-6",

    "ai.cache.enabled": "true",
    "ai.cache.redis.url": "redis://redis:6379",
    "ai.cache.similarity.threshold": "0.95",
    "ai.cache.ttl.seconds": "3600"
  }
}
```

**Cost optimisation layers:**

1. **Tier 0 (deterministic):** Simple field renames and type casts bypass the LLM entirely — zero cost
2. **Tier 1 (fast model):** Simple records use Claude Haiku — lowest per-token cost
3. **Semantic cache:** Duplicate/similar records return cached results — no LLM call
4. **Batch parallelism:** 4 concurrent LLM calls reduce wall-clock time
5. **Prompt caching:** Anthropic caches the system prompt — reduced input token cost

---

## 8. Schema Discovery for Unknown APIs

**Scenario:** Connect to an API with undocumented or evolving schemas. Let the LLM infer the output schema.

```json
{
  "name": "unknown-api",
  "config": {
    "connector.class": "sh.oso.connect.ai.connect.source.AiSourceConnector",
    "connect.ai.source.adapter": "http",
    "connect.ai.topic": "discovered-events",

    "http.source.url": "https://api.undocumented.example.com/data",
    "http.source.poll.interval.ms": "60000",

    "ai.llm.provider": "anthropic",
    "ai.llm.api.key": "sk-ant-...",
    "ai.llm.model": "claude-sonnet-4-20250514",
    "ai.agent.system.prompt": "Transform the raw data into well-structured events. Identify and extract the key entities, relationships, and timestamps.",
    "ai.schema.discovery": "true"
  }
}
```

The `ai.schema.discovery=true` flag triggers the SchemaDiscoveryAgent at startup. It fetches a sample of records, sends them to the LLM, and infers a JSON Schema that is then used as the target schema for all subsequent transformations.

---

## 9. OAuth2-Protected API with Cursor Pagination

**Scenario:** Ingest from an enterprise API requiring OAuth2 client credentials and cursor-based pagination.

```json
{
  "name": "salesforce-ingest",
  "config": {
    "connector.class": "sh.oso.connect.ai.connect.source.AiSourceConnector",
    "connect.ai.source.adapter": "http",
    "connect.ai.topic": "crm-records",

    "http.source.url": "https://api.crm.example.com/v2/contacts",
    "http.source.method": "GET",
    "http.source.poll.interval.ms": "300000",
    "http.source.response.content.path": "data",

    "http.auth.type": "oauth2",
    "http.auth.oauth2.token.url": "https://auth.crm.example.com/oauth/token",
    "http.auth.oauth2.client.id": "connect-ai-integration",
    "http.auth.oauth2.client.secret": "...",
    "http.auth.oauth2.scope": "contacts:read",

    "http.pagination.type": "cursor",
    "http.pagination.cursor.field": "meta.next_cursor",
    "http.pagination.cursor.param": "cursor",

    "ai.llm.provider": "anthropic",
    "ai.llm.api.key": "sk-ant-...",
    "ai.llm.model": "claude-sonnet-4-20250514",
    "ai.agent.system.prompt": "Normalise CRM contact records into: contact_id, first_name, last_name, email, company, lifecycle_stage, last_activity_date."
  }
}
```

**Key features used:** OAuth2 client credentials flow, cursor pagination, nested content path extraction.
