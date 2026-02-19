# Product Requirements Document (PRD)
# AI-Powered Universal Kafka Connector
## Project Codename: **Nexus**

**Version:** 1.0  
**Author:** OSO Engineering  
**Date:** 19 February 2026  
**Status:** Draft  

---

## 1. Executive Summary

Nexus is an AI-powered universal Kafka Connect connector that replaces the need for hundreds of bespoke connector plugins with a single, intelligent connector. It uses pluggable transport adapters (HTTP/REST, JDBC, Kafka, File, Queue) combined with LLM-backed AI agents to dynamically understand, transform, and route data between any source and any sink system.

Instead of writing custom connector code for each integration, operators describe their pipeline in natural language via a system prompt. The AI agent handles schema discovery, data transformation, format conversion, and intelligent routing — while the connector inherits Kafka Connect's battle-tested distributed execution, fault tolerance, and offset management.

**Value Proposition:** *"One connector to replace 200. Describe your pipeline, the AI handles the rest."*

---

## 2. Problem Statement

### 2.1 Current State

The Kafka Connect ecosystem has 250+ connectors on Confluent Hub, each representing a separate codebase with:

- Separate configuration DSLs (30+ properties per connector)
- Separate maintenance and version lifecycles
- Separate failure modes and debugging procedures
- Separate schema mapping logic
- No cross-connector intelligence or adaptability

### 2.2 Pain Points

| Pain Point | Impact |
|-----------|--------|
| **Long tail of integrations** | ~40% of enterprise APIs have no pre-built connector; teams build custom connectors from scratch |
| **Configuration complexity** | JDBC source alone has 4 query modes, 20+ config properties, brittle schema mapping |
| **Schema rigidity** | Source schema changes break connectors; manual intervention required |
| **Connector sprawl** | Large deployments manage 50+ different connector plugins, each with different upgrade cycles |
| **Development cost** | Building a production connector takes 4-8 weeks of engineering time |

### 2.3 Target Outcome

Reduce integration time from weeks to minutes. A single connector plugin that can connect to any REST API, any database, or any Kafka cluster — configured via natural language prompts and connection strings rather than bespoke code.

---

## 3. Architecture

### 3.1 Core Design Pattern: MM2-Inspired Embedded Consumer

The architecture is directly inspired by MirrorMaker 2's `MirrorSourceConnector` and Confluent Replicator. Both use the pattern of a **Source Connector with an embedded consumer** inside the `SourceTask`, where:

1. `SourceTask.start()` → creates the embedded client (consumer, HTTP client, JDBC connection)
2. `SourceTask.poll()` → fetches raw data via adapter → passes through AI agent → returns `List<SourceRecord>`
3. Connect framework's built-in producer → writes transformed records to the target topic/cluster
4. Offset management → handled by Connect's offset storage, identical to MM2

For the **sink** direction, a standard `SinkTask` receives records from Connect's consumer, passes them through the AI agent, and writes to the destination via a sink adapter.

### 3.2 System Architecture Diagram

```
┌──────────────────────────────────────────────────────────────────────┐
│                    Kafka Connect Worker (Distributed Mode)           │
│                                                                      │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │              NexusSourceConnector extends SourceConnector       │  │
│  │                                                                 │  │
│  │  ┌───────────────┐   ┌──────────────┐   ┌─────────────────┐  │  │
│  │  │ SourceAdapter  │──▶│   AI Agent    │──▶│  SourceRecord   │  │  │
│  │  │ (pluggable)    │   │   Pipeline    │   │  Output         │  │  │
│  │  │                │   │              │   │  → Connect       │  │  │
│  │  │ • HTTP         │   │ • Semantic    │   │    Producer      │  │  │
│  │  │ • JDBC         │   │   Cache      │   │                  │  │  │
│  │  │ • Kafka        │   │ • Router     │   │                  │  │  │
│  │  │ • File         │   │ • LLM Call   │   │                  │  │  │
│  │  │ • Queue        │   │ • Schema     │   │                  │  │  │
│  │  │                │   │   Enforcer   │   │                  │  │  │
│  │  └───────────────┘   └──────────────┘   └─────────────────┘  │  │
│  └────────────────────────────────────────────────────────────────┘  │
│                                                                      │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │              NexusSinkConnector extends SinkConnector           │  │
│  │                                                                 │  │
│  │  ┌─────────────────┐   ┌──────────────┐   ┌───────────────┐  │  │
│  │  │ SinkRecord batch │──▶│   AI Agent    │──▶│ SinkAdapter   │  │  │
│  │  │ (from Connect    │   │   Pipeline    │   │ (pluggable)   │  │  │
│  │  │  consumer)       │   │              │   │               │  │  │
│  │  │                  │   │ • Semantic    │   │ • HTTP POST   │  │  │
│  │  │                  │   │   Cache      │   │ • JDBC Upsert │  │  │
│  │  │                  │   │ • Router     │   │ • File Write  │  │  │
│  │  │                  │   │ • LLM Call   │   │ • Queue Pub   │  │  │
│  │  │                  │   │ • Formatter  │   │               │  │  │
│  │  └─────────────────┘   └──────────────┘   └───────────────┘  │  │
│  └────────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────┘
```

### 3.3 Component Breakdown

| Component | Responsibility | Language | Notes |
|-----------|---------------|----------|-------|
| `NexusSourceConnector` | Connector lifecycle, task config distribution, adapter discovery | Java 21 | Extends `SourceConnector` |
| `NexusSourceTask` | Poll loop, batch accumulation, agent orchestration, offset tracking | Java 21 | Extends `SourceTask` |
| `NexusSinkConnector` | Connector lifecycle, task config distribution | Java 21 | Extends `SinkConnector` |
| `NexusSinkTask` | Receive batches, agent orchestration, adapter write | Java 21 | Extends `SinkTask` |
| `SourceAdapter` (interface) | Transport-layer abstraction for reading from external systems | Java 21 | SPI-based plugin discovery |
| `SinkAdapter` (interface) | Transport-layer abstraction for writing to external systems | Java 21 | SPI-based plugin discovery |
| `AgentPipeline` | Orchestrates semantic cache → router → LLM call → schema enforcement | Java 21 | Shared by source and sink |
| `BatchAccumulator` | Time + size windowed micro-batching | Java 21 | Lock-free ring buffer |
| `SemanticCache` | Vector similarity cache to bypass LLM for known patterns | Java 21 | Redis-backed |
| `ModelRouter` | Classifies record complexity, routes to appropriate LLM tier | Java 21 | Rule-based + lightweight classifier |

---

## 4. Adapter Specifications

### 4.1 Adapter Interface (Source)

```java
public interface SourceAdapter extends Configurable, AutoCloseable {

    /** Unique adapter type identifier. Used in config: nexus.source.adapter=http */
    String type();

    /** Configuration definition for this adapter. Merged into the connector's ConfigDef. */
    ConfigDef configDef();

    /** Initialise connections, authentication, and clients. Called once per task lifecycle. */
    void start(Map<String, String> config);

    /**
     * Fetch raw data from the source system.
     * Must be non-blocking or bounded by timeout.
     * @param currentOffset Last committed offset (adapter-specific)
     * @param maxRecords Maximum records to fetch in this call
     * @return List of raw records with metadata and offset info
     */
    List<RawRecord> fetch(SourceOffset currentOffset, int maxRecords);

    /** Acknowledge that records up to this offset have been successfully produced to Kafka. */
    void commitOffset(SourceOffset offset);

    /** Health check for monitoring. */
    boolean isHealthy();
}
```

### 4.2 Adapter Interface (Sink)

```java
public interface SinkAdapter extends Configurable, AutoCloseable {

    String type();
    ConfigDef configDef();
    void start(Map<String, String> config);

    /**
     * Write a batch of transformed records to the destination.
     * Must handle retries internally for transient failures.
     * Throw RetriableException for Connect-level retry.
     */
    void write(List<TransformedRecord> records);

    /** Flush any buffered writes. Called by Connect framework before offset commit. */
    void flush();

    boolean isHealthy();
}
```

### 4.3 Adapter Implementations

#### 4.3.1 HTTP/REST Adapter

| Property | Description | Required |
|----------|-------------|----------|
| `nexus.source.http.url` | Base URL for the API endpoint | Yes |
| `nexus.source.http.method` | HTTP method (GET, POST) | No (default: GET) |
| `nexus.source.http.headers` | Static headers as key=value pairs | No |
| `nexus.source.http.auth.type` | Authentication type: `none`, `basic`, `bearer`, `oauth2`, `api_key` | No |
| `nexus.source.http.auth.credentials` | Auth credentials (reference `${secret:name}` for secure storage) | Conditional |
| `nexus.source.http.pagination.type` | Pagination strategy: `cursor`, `offset`, `page`, `link_header`, `none` | No |
| `nexus.source.http.pagination.cursor.path` | JSONPath to next cursor in response | Conditional |
| `nexus.source.http.poll.interval.ms` | Polling interval between API calls | No (default: 60000) |
| `nexus.source.http.timeout.ms` | HTTP request timeout | No (default: 30000) |
| `nexus.source.http.rate.limit.rps` | Max requests per second to respect API rate limits | No |

**Implementation Notes:**
- Use `java.net.http.HttpClient` (Java 21) with virtual thread executor
- Support response formats: JSON, XML, CSV, NDJSON
- Offset tracking: store last cursor/page/timestamp in Connect offset storage
- Sink direction: POST/PUT/PATCH with configurable URL templates (e.g., `/api/records/{id}`)

#### 4.3.2 JDBC Adapter

| Property | Description | Required |
|----------|-------------|----------|
| `nexus.source.jdbc.url` | JDBC connection URL | Yes |
| `nexus.source.jdbc.user` | Database username | Yes |
| `nexus.source.jdbc.password` | Database password (use `${secret:name}`) | Yes |
| `nexus.source.jdbc.driver.class` | JDBC driver class name | No (auto-detect) |
| `nexus.source.jdbc.query` | Custom SQL query (overrides table scan) | No |
| `nexus.source.jdbc.tables` | Comma-separated table names to replicate | Conditional |
| `nexus.source.jdbc.poll.interval.ms` | Polling interval | No (default: 5000) |
| `nexus.source.jdbc.mode` | Query mode: `bulk`, `timestamp`, `incrementing`, `timestamp+incrementing` | No (default: timestamp+incrementing) |
| `nexus.source.jdbc.timestamp.column` | Column name for timestamp-based tracking | Conditional |
| `nexus.source.jdbc.incrementing.column` | Column name for incrementing-based tracking | Conditional |
| `nexus.source.jdbc.batch.size` | Max rows per query | No (default: 1000) |

**Implementation Notes:**
- Use HikariCP connection pool
- Sink direction: auto-generate INSERT/UPSERT SQL based on agent's schema mapping
- Support auto-create tables if target doesn't exist (agent generates DDL)
- Offset tracking: last timestamp/incrementing value in Connect offset storage

#### 4.3.3 Kafka Adapter (K2K — MirrorMaker Style)

| Property | Description | Required |
|----------|-------------|----------|
| `nexus.source.kafka.bootstrap.servers` | Source Kafka cluster bootstrap servers | Yes |
| `nexus.source.kafka.topics` | Comma-separated topic names | Conditional |
| `nexus.source.kafka.topics.regex` | Topic name regex pattern | Conditional |
| `nexus.source.kafka.group.id` | Consumer group ID for the embedded consumer | No (auto-generated) |
| `nexus.source.kafka.security.protocol` | Security protocol (PLAINTEXT, SSL, SASL_SSL, etc.) | No |
| `nexus.source.kafka.consumer.*` | Pass-through consumer configuration | No |

**Implementation Notes:**
- Direct port of MirrorSourceTask's embedded consumer pattern
- Offset tracking via Connect offset storage with wrapped partition/offset keys
- `consumer.assign()` with partitions distributed across tasks by the connector
- Sink direction: not applicable (Connect's producer writes to target cluster)

#### 4.3.4 File Adapter

| Property | Description | Required |
|----------|-------------|----------|
| `nexus.source.file.type` | File system type: `local`, `s3`, `gcs`, `azure_blob`, `sftp` | Yes |
| `nexus.source.file.path` | File path or prefix/glob pattern | Yes |
| `nexus.source.file.format` | Expected format: `json`, `csv`, `xml`, `parquet`, `avro`, `auto` | No (default: auto) |
| `nexus.source.file.credentials` | Cloud credentials or SFTP auth | Conditional |
| `nexus.source.file.poll.interval.ms` | Polling interval for new files | No (default: 10000) |

#### 4.3.5 Queue Adapter

| Property | Description | Required |
|----------|-------------|----------|
| `nexus.source.queue.type` | Queue protocol: `jms`, `amqp`, `mqtt` | Yes |
| `nexus.source.queue.url` | Broker connection URL | Yes |
| `nexus.source.queue.destination` | Queue/topic name on the source broker | Yes |
| `nexus.source.queue.credentials` | Auth credentials | Conditional |

---

## 5. AI Agent Pipeline

### 5.1 Pipeline Architecture

Every record (or batch) passes through the Agent Pipeline, which is a composable chain:

```
Raw Records
    |
    v
+----------------+
| Semantic Cache  |---- HIT ----> Cached Result (skip LLM)
| (Redis Vector)  |
+-------+--------+
        | MISS
        v
+----------------+
| Model Router    |---- DETERMINISTIC ----> Rule Engine (no LLM)
| (Classifier)    |---- SIMPLE -----------> Haiku/GPT-4o-mini
|                 |---- COMPLEX ----------> Sonnet/GPT-4o
|                 |---- AMBIGUOUS --------> Opus/GPT-4
+-------+--------+
        |
        v
+----------------+
| LLM Call        |  <-- Prompt Caching (system prompt cached)
| (Async/VThread) |  <-- Structured Output (JSON Schema enforced)
|                 |  <-- Batch payload (N records per call)
+-------+--------+
        |
        v
+----------------+
| Schema Enforcer |---- Validate output against target schema
|                 |---- Reject malformed -> DLQ
+-------+--------+
        |
        v
+----------------+
| Cache Writer    |---- Store result + embedding in semantic cache
+-------+--------+
        |
        v
  List<SourceRecord> / write via SinkAdapter
```

### 5.2 Agent Configuration

```json
{
  "ai.llm.provider": "anthropic",
  "ai.llm.endpoint": "https://api.anthropic.com/v1/messages",
  "ai.llm.model.default": "claude-sonnet-4-20250514",
  "ai.llm.model.fast": "claude-haiku-4-20250514",
  "ai.llm.model.powerful": "claude-opus-4-20250514",
  "ai.llm.api.key": "${secret:anthropic-api-key}",

  "ai.agent.system.prompt": "You are a data transformation agent...",
  "ai.agent.target.schema": "{...JSON Schema definition...}",
  "ai.agent.target.topic.format": "${source.topic}.transformed",

  "ai.batch.size": 50,
  "ai.batch.max.wait.ms": 2000,
  "ai.batch.parallel.calls": 10,

  "ai.cache.enabled": true,
  "ai.cache.redis.url": "redis://cache:6379",
  "ai.cache.similarity.threshold": 0.95,
  "ai.cache.ttl.seconds": 3600,

  "ai.router.enabled": true,
  "ai.router.deterministic.patterns": "field_rename,type_cast,timestamp_format",

  "ai.structured.output": true,
  "ai.prompt.caching": true,
  "ai.max.retries": 3,
  "ai.retry.backoff.ms": 1000
}
```

### 5.3 Schema Discovery Mode

On first startup (or when `ai.schema.discovery=true`), the agent performs automatic schema inference:

1. Adapter fetches a sample of N records (configurable, default 10)
2. Agent analyses the sample and infers a Kafka Connect `Schema`
3. Schema is persisted to the connector's config (or Schema Registry)
4. Subsequent records are validated against the discovered schema
5. On schema drift detection, agent can auto-evolve or alert

```java
public class SchemaDiscoveryAgent {

    public Schema discoverSchema(List<RawRecord> sample, String systemPrompt) {
        String prompt = "Analyse these " + sample.size() + " sample records "
            + "from the source system. Infer a complete schema with field names, "
            + "data types, optionality, and any nested structures.\n\n"
            + "Source context: " + systemPrompt + "\n\n"
            + "Return a JSON object with fields:\n"
            + "- name: schema name\n"
            + "- fields: array of {name, type, optional, doc}\n"
            + "- type values: STRING, INT32, INT64, FLOAT64, BOOLEAN, "
            + "BYTES, ARRAY, MAP, STRUCT\n\n"
            + "Sample records:\n" + serialise(sample);

        SchemaResponse response = llmClient.call(prompt, SchemaResponse.class);
        return response.toConnectSchema();
    }
}
```

---

## 6. LLM Optimisation Strategies

### 6.1 Micro-Batching

| Parameter | Default | Description |
|-----------|---------|-------------|
| `ai.batch.size` | 50 | Maximum records per LLM call |
| `ai.batch.max.wait.ms` | 2000 | Maximum time to wait before flushing an incomplete batch |

**Implementation:** Dual-trigger `BatchAccumulator` with `ReentrantLock`. Returning `null` from `poll()` tells Connect to re-invoke, allowing accumulation across multiple poll cycles. The embedded consumer's `max.poll.interval.ms` must be set to accommodate worst-case batch accumulation + LLM latency (recommended: 600000ms).

### 6.2 Parallel Async Calls with Virtual Threads

| Parameter | Default | Description |
|-----------|---------|-------------|
| `ai.batch.parallel.calls` | 10 | Max concurrent LLM API calls per task |
| `ai.call.timeout.seconds` | 30 | Per-call timeout |

**Implementation:** Use `Executors.newVirtualThreadPerTaskExecutor()` (Java 21). Each sub-batch is submitted as a virtual thread task. Virtual threads are ~1KB overhead vs ~1MB for platform threads, enabling thousands of concurrent I/O-bound calls. Rate limiting is enforced by a `Semaphore` matching the LLM provider's rate limit.

### 6.3 Prompt Caching

System prompt and schema definition are marked with `cache_control: {"type": "ephemeral"}`. With continuous connector operation, the cache stays warm (5-minute TTL, refreshed per hit). Yields **90% cost reduction** on input tokens and **~30% reduction in time-to-first-token**.

### 6.4 Semantic Caching

| Parameter | Default | Description |
|-----------|---------|-------------|
| `ai.cache.enabled` | false | Enable/disable semantic cache |
| `ai.cache.redis.url` | — | Redis connection URL (with RediSearch module) |
| `ai.cache.similarity.threshold` | 0.95 | Cosine similarity threshold for cache hits |
| `ai.cache.ttl.seconds` | 3600 | Cache entry TTL |
| `ai.cache.embedding.model` | `text-embedding-3-small` | Embedding model for cache keys |

**Implementation:** On each record/batch, compute embedding, search Redis Vector Store, and if hit above threshold, return cached result and skip LLM. On miss, call LLM, store result with embedding. Expected cache hit rate: 50-80% for structured data with repetitive patterns.

### 6.5 Model Routing

| Tier | Model | Cost (per MTok) | Routing Criteria |
|------|-------|-----------------|-----------------|
| Tier 0 | Rule engine (no LLM) | Free | Deterministic transforms: field rename, type cast, timestamp reformat |
| Tier 1 | Claude Haiku / GPT-4o-mini | $0.25-1 | Simple format conversion, known schema mappings |
| Tier 2 | Claude Sonnet / GPT-4o | $3-15 | Complex transforms, nested structures, ambiguous data |
| Tier 3 | Claude Opus / GPT-4 | $15-75 | Multi-step reasoning, cross-record aggregation, unknown schemas |

**Implementation:** `ModelRouter` classifies each record/batch using configurable rules (regex on schema, field count, nesting depth) or a lightweight classifier model. Records matching known patterns in `ai.router.deterministic.patterns` bypass the LLM entirely.

### 6.6 Structured Output Enforcement

All LLM calls use JSON Schema response format. This constrains model decoding to produce only valid JSON matching the target schema. Eliminates parsing failures, retries, and wasted tokens.

### 6.7 Anthropic Message Batches API (Async Mode)

For latency-tolerant pipelines (ETL, analytics), enable async batch processing:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `ai.async.batch.enabled` | false | Use Anthropic Message Batches API |
| `ai.async.batch.size` | 10000 | Records per batch submission |
| `ai.async.batch.poll.interval.ms` | 60000 | Poll interval for batch completion |

Yields **50% cost discount** (stacks with prompt caching for ~75% total savings). Records are accumulated, submitted as a batch, and results are polled. An intermediate Kafka topic holds pending records.

---

## 7. Throughput Estimates

### 7.1 Per-Task Throughput Model

| Configuration | Records/sec/task | Notes |
|--------------|-----------------|-------|
| No optimisation (1 LLM call per record) | 2-5 | Baseline, unusable for production |
| Micro-batching only (50 records/call) | 100-250 | 50x improvement |
| + Parallel calls (10 concurrent) | 1,000-2,500 | Near-linear scaling |
| + Semantic cache (70% hit rate) | 3,000-8,000 | 3x from cache bypass |
| + Model routing (60% deterministic) | 5,000-20,000 | Tier 0 is essentially free |
| + Local model for Tier 1 (vLLM) | 10,000-50,000 | Eliminates API latency for simple records |

### 7.2 Cluster-Level Scaling

With distributed mode and multiple tasks:
- **tasks.max=4**: 4x per-task throughput
- **Multiple connector instances**: Linear horizontal scaling
- **Target**: 50,000-200,000 records/sec at cluster level with full optimisation stack

---

## 8. Offset Management & Exactly-Once Semantics

### 8.1 Source Connector Offsets

Following MM2's pattern, source offsets are wrapped with adapter-specific metadata:

```java
// HTTP adapter offset
Map<String, Object> partition = Map.of("adapter", "http", "url", baseUrl);
Map<String, Object> offset = Map.of("cursor", "abc123", "timestamp", "2026-02-19T08:00:00Z");

// JDBC adapter offset
Map<String, Object> partition = Map.of("adapter", "jdbc", "table", "orders");
Map<String, Object> offset = Map.of("timestamp_column", 1708329600000L, "incrementing_column", 45678L);

// Kafka adapter offset (same as MM2)
Map<String, Object> partition = Map.of("adapter", "kafka", "cluster", sourceClusterAlias, "topic", "orders", "partition", 0);
Map<String, Object> offset = Map.of("offset", 12345L);
```

### 8.2 Exactly-Once Support

- Source: Enable `exactly.once.source.support=enabled` on the Connect worker (Kafka 2.6+)
- Sink: Use `SinkTask.preCommit()` to coordinate adapter flush with offset commit
- LLM non-determinism: On replay after failure, agent may produce different output. Mitigate with:
  - `temperature=0` and `seed` parameter for maximum reproducibility
  - Idempotent writes in sink adapters (upsert by key, not blind insert)
  - Accept eventual consistency for non-critical transforms

---

## 9. Error Handling & Dead Letter Queue

### 9.1 Error Taxonomy

| Error Type | Source | Handling |
|-----------|--------|----------|
| **Adapter transport error** | HTTP 5xx, JDBC connection lost, consumer timeout | Retry with exponential backoff, max 3 attempts |
| **LLM API error** | Rate limit (429), server error (500), timeout | Retry with backoff, respect `Retry-After` header |
| **LLM response error** | Malformed output, schema validation failure | Retry once with clarifying prompt, then DLQ |
| **Agent semantic error** | Agent misunderstands data, produces wrong transformation | DLQ + alert (human review) |
| **Unrecoverable error** | Auth failure, missing config, unsupported format | Fail task, log error, connector status=FAILED |

### 9.2 Dead Letter Queue

```json
{
  "errors.tolerance": "all",
  "errors.deadletterqueue.topic.name": "nexus-dlq",
  "errors.deadletterqueue.topic.replication.factor": 3,
  "errors.deadletterqueue.context.headers.enable": true
}
```

DLQ records include headers with: original topic, partition, offset, error message, agent prompt, agent response (truncated), adapter type, and timestamp.

### 9.3 Errant Record Reporter (Sink)

```java
@Override
public void start(Map<String, String> props) {
    this.reporter = context.errantRecordReporter();
}

@Override
public void put(Collection<SinkRecord> records) {
    for (SinkRecord record : records) {
        try {
            TransformedRecord result = agentPipeline.process(record);
            sinkAdapter.write(List.of(result));
        } catch (AgentProcessingException e) {
            if (reporter != null) {
                reporter.report(record, e);
            }
        }
    }
}
```

---

## 10. Monitoring & Observability

### 10.1 JMX Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `nexus.records.processed.total` | Counter | Total records processed |
| `nexus.records.failed.total` | Counter | Total records sent to DLQ |
| `nexus.llm.calls.total` | Counter | Total LLM API calls made |
| `nexus.llm.calls.latency.ms` | Histogram | LLM call latency distribution |
| `nexus.llm.tokens.input.total` | Counter | Total input tokens consumed |
| `nexus.llm.tokens.output.total` | Counter | Total output tokens consumed |
| `nexus.llm.cost.usd.total` | Counter | Estimated cumulative LLM cost |
| `nexus.cache.hits.total` | Counter | Semantic cache hits |
| `nexus.cache.misses.total` | Counter | Semantic cache misses |
| `nexus.cache.hit.ratio` | Gauge | Rolling cache hit ratio |
| `nexus.router.tier0.total` | Counter | Records handled by rule engine |
| `nexus.router.tier1.total` | Counter | Records routed to fast model |
| `nexus.router.tier2.total` | Counter | Records routed to default model |
| `nexus.adapter.fetch.latency.ms` | Histogram | Source adapter fetch latency |
| `nexus.adapter.write.latency.ms` | Histogram | Sink adapter write latency |
| `nexus.batch.size` | Histogram | Actual batch sizes sent to LLM |

### 10.2 Structured Logging

All log entries include structured context:

```json
{
  "connector": "nexus-stripe-to-postgres",
  "task": 2,
  "adapter": "http",
  "llm_model": "claude-sonnet-4-20250514",
  "batch_size": 47,
  "cache_hit": false,
  "router_tier": 2,
  "latency_ms": 342,
  "tokens_in": 1250,
  "tokens_out": 890
}
```

---

## 11. Security

### 11.1 Credential Management

- All secrets referenced via `${secret:name}` using Connect's `ConfigProvider` SPI
- Supported providers: `FileConfigProvider`, `VaultConfigProvider` (HashiCorp Vault), `AWSSecretsManagerConfigProvider`
- LLM API keys never appear in connector config or logs

### 11.2 Data Privacy

| Feature | Description |
|---------|-------------|
| **PII masking** | Configurable field-level masking before sending to LLM (e.g., mask SSN, email) |
| **Data residency** | Support for region-specific LLM endpoints |
| **Audit logging** | Log what data was sent to LLM (field names only, not values) |
| **Local model option** | vLLM/Ollama endpoint for air-gapped environments where data cannot leave the network |

### 11.3 PII Masking Configuration

```json
{
  "ai.privacy.mask.fields": "ssn,email,phone,credit_card",
  "ai.privacy.mask.patterns": "\\b\\d{3}-\\d{2}-\\d{4}\\b",
  "ai.privacy.mask.replacement": "[MASKED]",
  "ai.privacy.unmask.output": true
}
```

When `unmask.output=true`, original values are restored in the output record after the LLM processes the masked version. The LLM never sees sensitive data.

---

## 12. Configuration Reference

### 12.1 Complete Connector Configuration Example

**HTTP Source to Kafka with AI Transformation:**

```json
{
  "name": "nexus-stripe-source",
  "config": {
    "connector.class": "io.oso.nexus.NexusSourceConnector",
    "tasks.max": 2,

    "nexus.source.adapter": "http",
    "nexus.source.http.url": "https://api.stripe.com/v1/charges",
    "nexus.source.http.auth.type": "bearer",
    "nexus.source.http.auth.credentials": "${secret:stripe-api-key}",
    "nexus.source.http.pagination.type": "cursor",
    "nexus.source.http.pagination.cursor.path": "$.data[-1].id",
    "nexus.source.http.poll.interval.ms": 30000,

    "ai.llm.provider": "anthropic",
    "ai.llm.model.default": "claude-sonnet-4-20250514",
    "ai.llm.api.key": "${secret:anthropic-api-key}",

    "ai.agent.system.prompt": "Extract charge records from Stripe API responses. Each charge should become a separate record with fields: charge_id (string), amount_cents (int), currency (string), customer_id (string), status (string), created_at (ISO-8601 timestamp).",

    "ai.batch.size": 25,
    "ai.batch.max.wait.ms": 5000,
    "ai.cache.enabled": true,
    "ai.cache.redis.url": "redis://cache:6379",
    "ai.structured.output": true,
    "ai.prompt.caching": true,

    "topic": "stripe.charges.normalised",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": false
  }
}
```

**JDBC Sink Connector:**

```json
{
  "name": "nexus-postgres-sink",
  "config": {
    "connector.class": "io.oso.nexus.NexusSinkConnector",
    "tasks.max": 2,
    "topics": "stripe.charges.normalised",

    "nexus.sink.adapter": "jdbc",
    "nexus.sink.jdbc.url": "jdbc:postgresql://warehouse:5432/analytics",
    "nexus.sink.jdbc.user": "${secret:pg-user}",
    "nexus.sink.jdbc.password": "${secret:pg-password}",

    "ai.llm.provider": "anthropic",
    "ai.llm.model.default": "claude-sonnet-4-20250514",
    "ai.llm.api.key": "${secret:anthropic-api-key}",

    "ai.agent.system.prompt": "Map incoming charge records to the PostgreSQL table stripe_charges. Generate UPSERT SQL. Primary key is charge_id. Convert amount_cents to amount_decimal (divide by 100). created_at should be TIMESTAMPTZ.",

    "ai.batch.size": 100,
    "ai.cache.enabled": true,
    "ai.cache.redis.url": "redis://cache:6379",
    "ai.structured.output": true,

    "errors.tolerance": "all",
    "errors.deadletterqueue.topic.name": "nexus-dlq",
    "errors.deadletterqueue.context.headers.enable": true
  }
}
```

---

## 13. What Nexus Does NOT Replace

| System | Why Not | Recommendation |
|--------|---------|---------------|
| **Debezium CDC** | Reads binary transaction logs (MySQL binlog, Postgres WAL) at protocol level. AI cannot interpret binary WAL streams. | Use Debezium as source -> Kafka topic -> Nexus K2K connector for AI-powered transformation/routing |
| **Cluster Linking** | Byte-for-byte replication at Kafka protocol level. No data transformation needed. | Use for pure cluster replication where no intelligence is required |
| **Schema Registry** | Schema governance, compatibility checks, serialisation. Orthogonal to Nexus. | Nexus can integrate with Schema Registry for schema persistence and evolution |
| **Kafka Streams / Flink** | Complex event processing, windowed aggregations, joins across streams. | Use for stateful stream processing; Nexus is for integration, not CEP |

---

## 14. Deployment

### 14.1 Packaging

- Single uber JAR (Maven Shade plugin) deployed to Connect worker's `plugin.path`
- Adapter implementations bundled in the JAR or as separate plugin JARs (extensible via SPI)
- JDBC drivers must be included or provided on the classpath

### 14.2 Docker

```dockerfile
FROM confluentinc/cp-kafka-connect:7.8.0

# Install Nexus connector
COPY nexus-connector-1.0.0-all.jar /usr/share/confluent-hub-components/nexus-connector/

# Include JDBC drivers
COPY drivers/ /usr/share/confluent-hub-components/nexus-connector/lib/

ENV CONNECT_PLUGIN_PATH="/usr/share/confluent-hub-components"
```

### 14.3 Kubernetes (Strimzi)

```yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaConnect
metadata:
  name: nexus-connect
  annotations:
    strimzi.io/use-connector-resources: "true"
spec:
  replicas: 3
  bootstrapServers: target-cluster:9092
  config:
    group.id: nexus-connect
    offset.storage.topic: nexus-connect-offsets
    config.storage.topic: nexus-connect-configs
    status.storage.topic: nexus-connect-status
    exactly.once.source.support: enabled
  build:
    output:
      type: docker
      image: registry.oso.sh/nexus-connect:latest
    plugins:
      - name: nexus-connector
        artifacts:
          - type: jar
            url: https://releases.oso.sh/nexus/nexus-connector-1.0.0-all.jar
```

### 14.4 Deployment Topology

Deploy Connect workers **close to the target** system (same network/region). The source adapter makes remote calls (HTTP, JDBC, Kafka consumer), which are more latency-tolerant than the producer writes to the target.

```
+----------------+         +-----------------------------+
| Source System   |  WAN    | Target Network              |
| (API / DB /     |<--------|                             |
|  Kafka)         |         | Connect Worker Cluster      |
|                 |         | +-- NexusSourceTask          |
|                 |         | |   +-- adapter (remote)     |
|                 |         | +-- producer (local writes)  |
|                 |         |                             |
|                 |         | Target System               |
|                 |         | (Kafka / DB / API)          |
+----------------+         +-----------------------------+
```

---

## 15. Build Plan & Milestones

### Phase 1: Foundation (Weeks 1-4)

| Deliverable | Description |
|-------------|-------------|
| Project scaffolding | Maven multi-module project, CI/CD (GitHub Actions), Java 21 |
| Core interfaces | `SourceAdapter`, `SinkAdapter`, `RawRecord`, `TransformedRecord`, `AgentPipeline` |
| `NexusSourceConnector` + `NexusSourceTask` | Shell implementation with adapter plugin loading |
| `NexusSinkConnector` + `NexusSinkTask` | Shell implementation with adapter plugin loading |
| HTTP Source Adapter | Full implementation with pagination, auth, rate limiting |
| Basic Agent Pipeline | Single LLM call per batch, no caching/routing |
| Integration tests | Embedded Kafka + mock LLM server |

**Exit Criteria:** HTTP Source -> AI Transform -> Kafka Topic working end-to-end in integration tests.

### Phase 2: Adapters & Sink (Weeks 5-8)

| Deliverable | Description |
|-------------|-------------|
| JDBC Source Adapter | All query modes, HikariCP pool, offset tracking |
| JDBC Sink Adapter | Auto-DDL, upsert generation, batch writes |
| HTTP Sink Adapter | POST/PUT with URL templates, retry logic |
| Kafka Source Adapter | MM2-style embedded consumer, offset wrapping |
| Schema Discovery Agent | Auto-infer schema from sample data |
| Structured Output | JSON Schema enforcement on all LLM calls |

**Exit Criteria:** HTTP->Kafka->JDBC pipeline working. Kafka->AI->Kafka (K2K) pipeline working.

### Phase 3: Optimisation (Weeks 9-12)

| Deliverable | Description |
|-------------|-------------|
| Micro-batching | `BatchAccumulator` with dual-trigger flush |
| Virtual thread parallelism | Concurrent LLM calls with semaphore-based rate limiting |
| Prompt caching | `cache_control` integration for Anthropic/OpenAI |
| Semantic caching | Redis Vector Store integration, embedding pipeline |
| Model routing | Rule-based classifier, tiered model selection |
| Metrics & monitoring | Full JMX metrics suite, structured logging |

**Exit Criteria:** >5,000 records/sec/task with full optimisation stack. <$0.01 per 1000 records.

### Phase 4: Production Hardening (Weeks 13-16)

| Deliverable | Description |
|-------------|-------------|
| DLQ integration | Errant record reporter, DLQ topic with context headers |
| Exactly-once support | EOS configuration, idempotent adapter writes |
| PII masking | Field-level masking before LLM, unmasking in output |
| File + Queue adapters | S3, GCS, JMS, AMQP implementations |
| Security | Vault integration, secret providers, audit logging |
| Documentation | User guide, adapter development guide, example configs |
| Confluent Hub packaging | Package for Confluent Hub distribution |

**Exit Criteria:** Production-ready release. All adapters tested with real systems. Documentation complete.

---

## 16. Success Metrics

| Metric | Target | Measurement |
|--------|--------|-------------|
| Integration setup time | <15 minutes (vs hours/weeks today) | Time from `connector.create` to first record in target |
| Throughput (optimised) | >10,000 records/sec/connector | Benchmark with standard payload |
| LLM cost per record | <$0.001 (with full optimisation) | Token tracking + billing API |
| Schema discovery accuracy | >95% correct type inference | Comparison against known schemas |
| Transformation accuracy | >99% correct output | Validation against expected output in test suite |
| Cache hit ratio | >60% for structured data pipelines | Semantic cache metrics |
| Connector uptime | >99.9% | Connect framework health monitoring |
| Community adoption | 500+ GitHub stars in first 6 months | GitHub analytics |

---

## 17. Open Questions & Risks

| # | Question / Risk | Impact | Mitigation |
|---|----------------|--------|------------|
| 1 | LLM non-determinism on replay after failure | Records may differ on reprocess | temperature=0, seed parameter, idempotent writes |
| 2 | LLM API rate limits constrain throughput ceiling | Cannot scale beyond provider limits | Local model tier (vLLM), multiple API keys, Batch API |
| 3 | Cost unpredictability at scale | Token costs hard to forecast | Cost tracking metrics, budget alerts, aggressive caching + routing |
| 4 | Schema drift if agent changes output format | Downstream consumers break | Schema pinning via structured output, Schema Registry integration |
| 5 | Prompt injection via source data | Malicious data could manipulate agent | Input sanitisation, PII masking, role separation in prompts |
| 6 | Semantic cache returns incorrect cached result | Data quality issues | High similarity threshold (0.95+), cache invalidation on schema changes |
| 7 | Complex pagination patterns in HTTP APIs | Not all APIs follow standard patterns | Extensible `PaginationStrategy` interface, agent-assisted pagination discovery |
| 8 | JDBC adapter competing with mature Confluent connector | Feature gap perception | Focus on AI-powered schema discovery and adaptive mapping as differentiators |

---

## 18. Competitive Landscape

| Product | What It Does | How Nexus Differs |
|---------|-------------|-------------------|
| **Confluent JDBC Connector** | Pre-built JDBC source/sink with fixed query modes | Nexus: AI-powered schema discovery, adaptive mapping, natural language config |
| **Confluent HTTP Connector** | HTTP source/sink for specific API patterns | Nexus: Works with *any* API via AI understanding, no per-API code |
| **Debezium** | CDC from database transaction logs | Complementary -- Nexus transforms Debezium output, doesn't replace CDC |
| **StreamNative UniConn** | Universal connectivity for Kafka/Pulsar | No AI -- static connector framework. Nexus adds intelligence layer |
| **Lenses MCP / Kafka AI** | Connects LLMs *to* Kafka | Opposite direction -- Nexus puts AI *inside* the connector pipeline |
| **Airbyte / Fivetran** | Managed ELT with pre-built connectors | Cloud-only, no Kafka-native, per-connector maintenance. Nexus is self-hosted, single plugin |
| **Custom SMTs** | Lightweight per-record transforms | No external API calls, no intelligence, no schema inference |

---

## 19. Licensing & Distribution

| Edition | License | Features |
|---------|---------|----------|
| **Nexus OSS** | Apache 2.0 | Core connector, all adapters, basic agent pipeline (single model, no caching) |
| **Nexus Enterprise** | Commercial (BSL/proprietary) | Semantic caching, model routing, PII masking, schema discovery, metrics dashboard, priority support |

---

## Appendix A: Glossary

| Term | Definition |
|------|-----------|
| **Adapter** | Pluggable transport module that handles reading/writing to a specific external system type |
| **Agent Pipeline** | The processing chain that transforms raw records through cache, router, LLM, and schema enforcement |
| **Semantic Cache** | Vector similarity cache that returns previously computed transformations for similar records |
| **Model Router** | Component that classifies record complexity and routes to the appropriate LLM tier |
| **Micro-batching** | Accumulating multiple records into a single LLM call to amortise latency |
| **Prompt Caching** | LLM provider feature that caches the system prompt prefix, reducing cost and latency |
| **Structured Output** | LLM response mode that constrains output to match a JSON Schema |
| **DLQ** | Dead Letter Queue -- topic for records that failed processing |
| **K2K** | Kafka-to-Kafka replication (the MirrorMaker pattern) |

---

## Appendix B: Technology Stack

| Layer | Technology | Version | Rationale |
|-------|-----------|---------|-----------|
| Language | Java | 21 (LTS) | Native Kafka Connect API, virtual threads |
| Build | Maven | 3.9+ | Shade plugin for uber JAR, multi-module |
| LLM Client | java.net.http.HttpClient | Built-in | Async, virtual thread compatible, no external deps |
| Connection Pool | HikariCP | 5.x | JDBC adapter connection pooling |
| Vector Store | Redis + RediSearch | 7.x | Semantic cache with vector similarity search |
| Embedding | OpenAI text-embedding-3-small | -- | Semantic cache key generation |
| Testing | JUnit 5 + Testcontainers | -- | Integration tests with real Kafka + Redis |
| CI/CD | GitHub Actions | -- | Build, test, release, Confluent Hub publish |
| Containerisation | Docker | -- | Connect worker images |
| Orchestration | Kubernetes + Strimzi | -- | Production deployment |

---

*Document generated: 19 February 2026*
*Next review: Upon Phase 1 completion*
