# Nexus Configuration Reference

Complete reference for all Nexus connector configuration properties.

## Overview

Nexus configuration is passed as key-value pairs when creating a connector via the Kafka Connect REST API. Properties are grouped by function:

| Prefix | Domain |
|--------|--------|
| `nexus.*` | Core connector (adapter type, topic, batch size, DLQ) |
| `ai.llm.*` | LLM provider, model, API key, temperature, tokens |
| `ai.agent.*` | Agent behaviour (prompts, schema, caching, retries) |
| `ai.batch.*` | Batch accumulator and parallel execution |
| `ai.router.*` | Model router and deterministic transforms |
| `ai.cache.*` | Semantic cache (Redis) |
| `http.source.*` / `http.sink.*` | HTTP adapter |
| `http.auth.*` | HTTP authentication strategies |
| `http.pagination.*` | HTTP pagination strategies |
| `jdbc.*` | JDBC adapter |
| `kafka.source.*` | Kafka-to-Kafka adapter |

---

## Core Connector Properties

### Source Connector

| Property | Type | Default | Required | Description |
|----------|------|---------|----------|-------------|
| `nexus.source.adapter` | String | — | Yes | Source adapter type: `http`, `jdbc`, or `kafka` |
| `nexus.topic` | String | — | Yes | Target Kafka topic for produced records |
| `nexus.batch.size` | Int | `100` | No | Maximum records per fetch batch |
| `nexus.dlq.topic` | String | `""` | No | Dead-letter queue topic for records that fail transformation |
| `tasks.max` | Int | `1` | No | Number of parallel tasks (standard Kafka Connect property) |

### Sink Connector

| Property | Type | Default | Required | Description |
|----------|------|---------|----------|-------------|
| `nexus.sink.adapter` | String | — | Yes | Sink adapter type: `http` or `jdbc` |
| `topics` | String | — | Yes | Kafka topic(s) to consume from (standard Kafka Connect property) |
| `nexus.dlq.topic` | String | `""` | No | Dead-letter queue topic for failed records |
| `tasks.max` | Int | `1` | No | Number of parallel tasks |

---

## AI / LLM Properties

### LLM Provider

| Property | Type | Default | Required | Description |
|----------|------|---------|----------|-------------|
| `ai.llm.provider` | String | `"anthropic"` | Yes | LLM provider: `anthropic` or `openai` |
| `ai.llm.api.key` | Password | `""` | Yes | API key for the LLM provider |
| `ai.llm.model` | String | `"claude-sonnet-4-20250514"` | No | Model identifier for transformations |
| `ai.llm.base.url` | String | `""` | No | Override base URL for LLM API (proxies, testing) |
| `ai.llm.endpoint` | String | `""` | No | Alias for `ai.llm.base.url` (fallback) |
| `ai.llm.temperature` | Double | `0.0` | No | Sampling temperature (0.0 = deterministic, 1.0 = creative) |
| `ai.llm.max.tokens` | Int | `4096` | No | Maximum output tokens per LLM response |

### Agent Behaviour

| Property | Type | Default | Required | Description |
|----------|------|---------|----------|-------------|
| `ai.agent.system.prompt` | String | `""` | Yes | System prompt describing the transformation task |
| `ai.agent.target.schema` | String | `""` | No | JSON Schema string for validating LLM output structure |
| `ai.agent.enable.prompt.caching` | Boolean | `true` | No | Enable Anthropic prompt caching for repeated system prompts |
| `ai.schema.discovery` | Boolean | `false` | No | LLM-based schema inference from sample records at startup (source only) |
| `ai.structured.output` | Boolean | `true` | No | Pass JSON Schema to LLM as structured output constraint |
| `ai.max.retries` | Int | `3` | No | Max LLM retry attempts on malformed output |
| `ai.retry.backoff.ms` | Long | `1000` | No | Base backoff in ms between retries (exponentially doubled) |

### Batch Accumulator

Controls dual-trigger flush (size or time, whichever comes first). Source connector only.

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `ai.batch.size` | Int | `50` | Records that trigger a batch flush |
| `ai.batch.max.wait.ms` | Long | `2000` | Max wait time in ms before flushing regardless of size |

### Parallel LLM Execution

Split batches into sub-batches and process them concurrently.

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `ai.batch.parallel.calls` | Int | `1` | Number of concurrent LLM calls (>1 enables sub-batching) |
| `ai.call.timeout.seconds` | Int | `30` | Per-sub-batch LLM call timeout in seconds |

### Model Router

Automatically routes records to different model tiers based on complexity.

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `ai.router.enabled` | Boolean | `false` | Enable automatic model tier routing |
| `ai.llm.model.fast` | String | `"claude-haiku-4-5-20251001"` | Tier 1: simple flat records (<=5 fields, depth <=1) |
| `ai.llm.model.default` | String | (value of `ai.llm.model`) | Tier 2: moderate complexity |
| `ai.llm.model.powerful` | String | `"claude-opus-4-6"` | Tier 3: complex records (>20 fields, depth >3) |
| `ai.router.deterministic.patterns` | String | `""` | Tier 0 deterministic transforms (comma-separated): `field_rename`, `type_cast`, `timestamp_format` |

**Tier routing logic:**

| Tier | Condition | Action |
|------|-----------|--------|
| 0 | Deterministic patterns configured + flat records | Bypass LLM entirely |
| 1 | Avg fields <= 5, max depth <= 1 | Use fast model |
| 2 | Moderate complexity (default) | Use default model |
| 3 | Avg fields > 20, depth > 3, or unparseable | Use powerful model |

### Semantic Cache

Redis-backed vector similarity cache for LLM call deduplication. Requires Redis Stack with RediSearch.

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `ai.cache.enabled` | Boolean | `false` | Enable semantic cache |
| `ai.cache.redis.url` | String | `"redis://localhost:6379"` | Redis connection URL |
| `ai.cache.similarity.threshold` | Double | `0.95` | Minimum cosine similarity for a cache hit (0.0–1.0) |
| `ai.cache.ttl.seconds` | Int | `3600` | TTL for cached entries |
| `ai.cache.embedding.model` | String | `"text-embedding-3-small"` | OpenAI embedding model for vector generation |

**Note:** The semantic cache uses OpenAI embeddings regardless of which LLM provider you choose. An OpenAI API key is required (currently reuses `ai.llm.api.key`; ensure it's an OpenAI key or configure the cache embedding separately).

---

## HTTP Adapter Properties

### HTTP Source

Applicable when `nexus.source.adapter=http`.

| Property | Type | Default | Required | Description |
|----------|------|---------|----------|-------------|
| `http.source.url` | String | — | Yes | HTTP endpoint URL to poll |
| `http.source.method` | String | `"GET"` | No | HTTP method: `GET`, `POST`, `PUT` |
| `http.source.headers` | String | `""` | No | Comma-separated `Name: Value` pairs |
| `http.source.poll.interval.ms` | Long | `60000` | No | Polling interval in ms |
| `http.source.response.content.path` | String | `""` | No | Dot-delimited JSON path to extract records (e.g. `data.items`) |
| `http.source.timeout.ms` | Long | `30000` | No | HTTP connection/request timeout in ms |
| `http.rate.limit.rps` | Double | `10.0` | No | Token-bucket rate limit (requests/second) |

### HTTP Sink

Applicable when `nexus.sink.adapter=http`.

| Property | Type | Default | Required | Description |
|----------|------|---------|----------|-------------|
| `http.sink.url` | String | — | Yes | Target HTTP endpoint (supports `{field}` template substitution) |
| `http.sink.method` | String | `"POST"` | No | HTTP method: `POST`, `PUT`, `PATCH` |
| `http.sink.headers` | String | `""` | No | Comma-separated `Name: Value` pairs |
| `http.sink.timeout.ms` | Long | `30000` | No | HTTP connection/request timeout in ms |
| `http.sink.batch.size` | Int | `1` | No | Records per HTTP request batch |
| `http.sink.retry.max` | Int | `3` | No | Max retries on 429/5xx (exponential backoff, honours `Retry-After`) |
| `http.rate.limit.rps` | Double | `10.0` | No | Token-bucket rate limit (requests/second) |

### HTTP Authentication

Set via `http.auth.type`. Authentication config is shared between source and sink.

#### `http.auth.type=none` (default)

No additional properties.

#### `http.auth.type=basic`

| Property | Required | Description |
|----------|----------|-------------|
| `http.auth.basic.username` | Yes | HTTP Basic Auth username |
| `http.auth.basic.password` | Yes | HTTP Basic Auth password |

#### `http.auth.type=bearer`

| Property | Required | Description |
|----------|----------|-------------|
| `http.auth.bearer.token` | Yes | Static bearer token |

#### `http.auth.type=apikey`

| Property | Required | Default | Description |
|----------|----------|---------|-------------|
| `http.auth.apikey.header` | No | `"X-API-Key"` | Header name for the API key |
| `http.auth.apikey.value` | Yes | — | The API key value |

#### `http.auth.type=oauth2`

Client credentials flow.

| Property | Required | Default | Description |
|----------|----------|---------|-------------|
| `http.auth.oauth2.token.url` | Yes | — | Token endpoint URL |
| `http.auth.oauth2.client.id` | Yes | — | Client ID |
| `http.auth.oauth2.client.secret` | Yes | — | Client secret |
| `http.auth.oauth2.scope` | No | `""` | OAuth2 scope |

### HTTP Pagination

Set via `http.pagination.type`. Source adapter only.

#### `http.pagination.type=none` (default)

No pagination.

#### `http.pagination.type=cursor`

| Property | Default | Description |
|----------|---------|-------------|
| `http.pagination.cursor.field` | `"next_cursor"` | Response field containing the next cursor |
| `http.pagination.cursor.param` | `"cursor"` | Query parameter for the cursor |

#### `http.pagination.type=offset`

| Property | Default | Description |
|----------|---------|-------------|
| `http.pagination.offset.param` | `"offset"` | Query parameter for offset |
| `http.pagination.limit.param` | `"limit"` | Query parameter for page size |
| `http.pagination.limit.value` | `"100"` | Items per page |

#### `http.pagination.type=page_number`

| Property | Default | Description |
|----------|---------|-------------|
| `http.pagination.page.param` | `"page"` | Query parameter for page number |
| `http.pagination.page.start` | `"1"` | Starting page number |

#### `http.pagination.type=link_header`

Follows RFC 5988 `Link: <url>; rel="next"` headers automatically. No additional config.

---

## JDBC Adapter Properties

### JDBC Source

Applicable when `nexus.source.adapter=jdbc`.

| Property | Type | Default | Required | Description |
|----------|------|---------|----------|-------------|
| `jdbc.url` | String | — | Yes | JDBC connection URL (e.g. `jdbc:postgresql://host:5432/db`) |
| `jdbc.user` | String | `""` | No | Database username |
| `jdbc.password` | Password | `""` | No | Database password |
| `jdbc.table` | String | — | Yes* | Source table name (*not required if `jdbc.query` is set) |
| `jdbc.tables` | String | `""` | No | Comma-separated list for multi-table mode |
| `jdbc.query` | String | `""` | No | Custom SQL query (overrides `jdbc.table`) |
| `jdbc.driver.class` | String | `""` | No | JDBC driver class (auto-discovered if omitted) |
| `jdbc.query.mode` | String | `"bulk"` | No | Query mode: `bulk`, `timestamp`, `incrementing`, `timestamp+incrementing` |
| `jdbc.timestamp.column` | String | `"updated_at"` | No | Column for timestamp-based incremental fetching |
| `jdbc.incrementing.column` | String | `"id"` | No | Column for incrementing-ID-based fetching |
| `jdbc.poll.interval.ms` | Long | `5000` | No | Polling interval in ms |

**Query modes:**

| Mode | Behaviour |
|------|-----------|
| `bulk` | SELECT * with no filtering — full table scan each poll |
| `timestamp` | WHERE timestamp_column > last_seen ORDER BY timestamp_column |
| `incrementing` | WHERE id_column > last_seen ORDER BY id_column |
| `timestamp+incrementing` | Combines both for gapless incremental capture |

### JDBC Sink

Applicable when `nexus.sink.adapter=jdbc`.

| Property | Type | Default | Required | Description |
|----------|------|---------|----------|-------------|
| `jdbc.url` | String | — | Yes | JDBC connection URL |
| `jdbc.user` | String | `""` | No | Database username |
| `jdbc.password` | Password | `""` | No | Database password |
| `jdbc.table` | String | — | Yes | Target table for writes |
| `jdbc.driver.class` | String | `""` | No | JDBC driver class |
| `jdbc.sink.pk.columns` | String | `""` | No* | Primary key columns for upsert (*required when insert mode is `upsert`) |
| `jdbc.sink.insert.mode` | String | `"insert"` | No | Write mode: `insert` or `upsert` (ON CONFLICT DO UPDATE) |
| `jdbc.sink.auto.ddl` | Boolean | `false` | No | Auto-create target table if it doesn't exist |
| `jdbc.batch.size` | Int | `1000` | No | Rows per JDBC batch write |

---

## Kafka-to-Kafka Adapter Properties

Applicable when `nexus.source.adapter=kafka`. Uses an embedded KafkaConsumer to read from an upstream cluster.

| Property | Type | Default | Required | Description |
|----------|------|---------|----------|-------------|
| `kafka.source.bootstrap.servers` | String | `""` | Yes | Bootstrap servers for the upstream Kafka cluster |
| `kafka.source.topics` | String | `""` | Yes* | Comma-separated source topics (*or use `topics.regex`) |
| `kafka.source.topics.regex` | String | `""` | No | Regex for topic subscription (alternative to explicit list) |
| `kafka.source.group.id` | String | `"nexus-k2k-consumer"` | No | Consumer group ID |
| `kafka.source.poll.timeout.ms` | Long | `1000` | No | Consumer poll timeout in ms |
| `kafka.source.security.protocol` | String | `"PLAINTEXT"` | No | Security protocol: `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, `SASL_SSL` |

### Consumer Pass-Through

Any property prefixed with `kafka.source.consumer.` is stripped of the prefix and passed directly to the underlying `KafkaConsumer`. This allows configuring any Kafka consumer property:

```json
{
  "kafka.source.consumer.auto.offset.reset": "earliest",
  "kafka.source.consumer.max.poll.records": "500",
  "kafka.source.consumer.session.timeout.ms": "30000"
}
```

---

## Example: Full Source Connector Config

```json
{
  "name": "api-ingest",
  "config": {
    "connector.class": "sh.oso.nexus.connect.source.NexusSourceConnector",
    "tasks.max": "2",

    "nexus.source.adapter": "http",
    "nexus.topic": "api-events",
    "nexus.batch.size": "100",
    "nexus.dlq.topic": "api-events-dlq",

    "http.source.url": "https://api.example.com/v1/events",
    "http.source.method": "GET",
    "http.source.headers": "Accept: application/json",
    "http.source.poll.interval.ms": "60000",
    "http.source.response.content.path": "data.events",
    "http.auth.type": "bearer",
    "http.auth.bearer.token": "sk-...",
    "http.pagination.type": "cursor",
    "http.pagination.cursor.field": "meta.next",
    "http.pagination.cursor.param": "after",

    "ai.llm.provider": "anthropic",
    "ai.llm.api.key": "sk-ant-...",
    "ai.llm.model": "claude-sonnet-4-20250514",
    "ai.llm.temperature": "0.0",
    "ai.agent.system.prompt": "Transform API events into canonical format with: event_id, event_type, timestamp, payload.",
    "ai.agent.target.schema": "{\"type\":\"object\",\"properties\":{\"event_id\":{\"type\":\"string\"},\"event_type\":{\"type\":\"string\"},\"timestamp\":{\"type\":\"string\"},\"payload\":{\"type\":\"object\"}},\"required\":[\"event_id\",\"event_type\",\"timestamp\"]}",
    "ai.agent.enable.prompt.caching": "true",
    "ai.structured.output": "true",

    "ai.batch.size": "25",
    "ai.batch.max.wait.ms": "3000",
    "ai.batch.parallel.calls": "3",
    "ai.call.timeout.seconds": "60",

    "ai.router.enabled": "true",
    "ai.llm.model.fast": "claude-haiku-4-5-20251001",
    "ai.llm.model.powerful": "claude-opus-4-6",

    "ai.cache.enabled": "true",
    "ai.cache.redis.url": "redis://redis:6379",
    "ai.cache.similarity.threshold": "0.95",
    "ai.cache.ttl.seconds": "7200"
  }
}
```
