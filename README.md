<p align="center">
  <h1 align="center">kafka-connect-ai</h1>
  <p align="center">
    AI-powered universal connector for Apache Kafka Connect
  </p>
</p>

<p align="center">
  <a href="https://github.com/osodevops/kafka-connect-ai/actions/workflows/ci.yml">
    <img src="https://github.com/osodevops/kafka-connect-ai/actions/workflows/ci.yml/badge.svg" alt="CI Status">
  </a>
  <a href="https://github.com/osodevops/kafka-connect-ai/blob/main/LICENSE">
    <img src="https://img.shields.io/badge/license-Apache%202.0-blue.svg" alt="License: Apache 2.0">
  </a>
  <a href="https://github.com/osodevops/kafka-connect-ai/releases">
    <img src="https://img.shields.io/github/v/release/osodevops/kafka-connect-ai" alt="Release">
  </a>
</p>

---

**kafka-connect-ai** is a Kafka Connect connector that uses LLMs to transform data between any source and any sink. Instead of writing custom connectors for every integration, you deploy a single uber JAR and describe transformations in natural language. It fetches data from HTTP APIs, databases, or other Kafka clusters, passes records through an AI transformation pipeline, and writes the results to Kafka topics or external systems.

## Features

- **Universal adapters** — HTTP, JDBC (PostgreSQL), and Kafka-to-Kafka source adapters; HTTP and JDBC sink adapters. Pluggable via SPI.
- **LLM-powered transformation** — Describe your transformation in plain English. Supports Anthropic (Claude) and OpenAI models.
- **Schema enforcement** — Define a JSON Schema target and kafka-connect-ai validates every record the LLM produces. Malformed output is retried automatically.
- **Schema discovery** — No schema? kafka-connect-ai infers one from sample data using the LLM at startup.
- **4-tier model routing** — Automatically routes records to the cheapest model that can handle them: deterministic transforms (free), fast model, default model, or powerful model.
- **Semantic caching** — Redis-backed vector similarity cache deduplicates LLM calls for similar records, cutting cost and latency.
- **Batch processing** — Dual-trigger accumulator (size + time) with parallel sub-batch execution across multiple concurrent LLM calls.
- **HTTP strategies** — 5 auth strategies (Basic, Bearer, API Key, OAuth2, None) and 5 pagination strategies (Cursor, Offset, Page Number, Link Header, None).
- **JDBC modes** — 4 query modes (bulk, timestamp, incrementing, timestamp+incrementing), upsert with ON CONFLICT, auto-DDL, and batch writes.
- **Production observability** — 16 JMX metrics via Micrometer (records, LLM calls, latency, tokens, cost, cache hits, router tiers, adapter latency, batch sizes).
- **Prompt caching** — Anthropic prompt caching for repeated system prompts reduces input token costs.
- **Dead letter queue** — Failed records route to a configurable DLQ topic without blocking the pipeline.
- **Structured output** — Anthropic via prompt+prefill, OpenAI via `json_schema` response format.
- **Drop-in deployment** — Single uber JAR with shaded dependencies. Works with any Kafka Connect cluster.

## Installation

### Download from GitHub Releases

```bash
curl -L https://github.com/osodevops/kafka-connect-ai/releases/latest/download/kafka-connect-ai.zip \
  -o kafka-connect-ai.zip
unzip kafka-connect-ai.zip
cp kafka-connect-ai-*/kafka-connect-ai-connect-*-all.jar /path/to/kafka-connect/plugins/
```

Restart your Connect workers. See the [Installation Guide](docs/INSTALL.md) for all options.

### Docker

```bash
docker pull ghcr.io/osodevops/kafka-connect-ai-connect:latest
```

### Build from Source

Requires Java 17+ and Maven 3.9+.

```bash
git clone https://github.com/osodevops/kafka-connect-ai.git
cd kafka-connect-ai
mvn clean package -pl kafka-connect-ai-connect -am -DskipTests
```

The uber JAR is at `kafka-connect-ai-connect/target/kafka-connect-ai-connect-*-all.jar`.

## Quick Start

### 1. Start the Stack

```bash
mvn clean package -pl kafka-connect-ai-connect -am -DskipTests
cd docker && docker compose up -d
```

This starts Kafka (KRaft), Schema Registry, Kafka Connect with kafka-connect-ai, PostgreSQL, and Redis.

### 2. Verify kafka-connect-ai is Loaded

```bash
curl -s http://localhost:8083/connector-plugins | grep ai
```

### 3. Deploy a Connector

**HTTP API to Kafka** — poll a REST API, transform with an LLM, write to a topic:

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "api-source",
    "config": {
      "connector.class": "sh.oso.connect.ai.connect.source.AiSourceConnector",
      "tasks.max": "1",
      "connect.ai.source.adapter": "http",
      "connect.ai.topic": "events",
      "http.source.url": "https://api.example.com/v1/data",
      "http.source.poll.interval.ms": "60000",
      "ai.llm.provider": "anthropic",
      "ai.llm.api.key": "sk-ant-...",
      "ai.llm.model": "claude-sonnet-4-20250514",
      "ai.agent.system.prompt": "Transform into: id, type, timestamp, payload."
    }
  }'
```

**Database CDC to Kafka** — capture changes from PostgreSQL:

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "db-source",
    "config": {
      "connector.class": "sh.oso.connect.ai.connect.source.AiSourceConnector",
      "tasks.max": "1",
      "connect.ai.source.adapter": "jdbc",
      "connect.ai.topic": "db-events",
      "jdbc.url": "jdbc:postgresql://postgres:5432/kcai",
      "jdbc.user": "kcai",
      "jdbc.password": "kcai",
      "jdbc.table": "orders",
      "jdbc.query.mode": "timestamp",
      "jdbc.timestamp.column": "updated_at",
      "ai.llm.provider": "anthropic",
      "ai.llm.api.key": "sk-ant-...",
      "ai.agent.system.prompt": "Normalise order rows into events with: order_id, customer, amount_usd, status."
    }
  }'
```

**Kafka-to-Kafka transformation** — consume, transform, produce:

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "k2k-bridge",
    "config": {
      "connector.class": "sh.oso.connect.ai.connect.source.AiSourceConnector",
      "tasks.max": "1",
      "connect.ai.source.adapter": "kafka",
      "connect.ai.topic": "normalised-events",
      "kafka.source.bootstrap.servers": "upstream-kafka:9092",
      "kafka.source.topics": "raw-events",
      "ai.llm.provider": "anthropic",
      "ai.llm.api.key": "sk-ant-...",
      "ai.agent.system.prompt": "Merge legacy event formats into a unified schema."
    }
  }'
```

See the [Quick Start Guide](docs/quickstart.md) for complete examples including sink connectors.

## Why kafka-connect-ai?

### The Problem

Every new data integration requires a custom Kafka Connect connector — or custom glue code. Schema changes break pipelines. Adding a new source means writing, testing, and maintaining another connector. Transformation logic is scattered across applications.

### The Solution

kafka-connect-ai replaces per-integration connector code with a single, universal connector where transformations are described in natural language:

1. **One connector for everything** — HTTP APIs, databases, Kafka clusters. Source and sink.
2. **Natural language transforms** — Describe what you want in a system prompt. No code.
3. **Schema guarantees** — JSON Schema enforcement validates every record. Invalid output is retried.
4. **Cost optimisation** — 4-tier model routing, semantic caching, deterministic transforms, and prompt caching minimise LLM spend.

### Comparison

| Feature | kafka-connect-ai | Custom Connector | Debezium | MirrorMaker 2 |
|---------|-------|------------------|----------|----------------|
| Sources | HTTP, JDBC, Kafka | One per connector | JDBC (CDC) | Kafka only |
| Sinks | HTTP, JDBC | One per connector | N/A | Kafka only |
| Transformation | LLM (natural language) | Java code | SMTs only | SMTs only |
| Schema enforcement | JSON Schema + LLM retry | Manual | Avro/JSON Schema | None |
| New integration effort | Config change | Weeks of development | Limited to supported DBs | Kafka-to-Kafka only |
| Multi-model routing | 4-tier automatic | N/A | N/A | N/A |
| Semantic caching | Redis vector store | N/A | N/A | N/A |

### When NOT to Use kafka-connect-ai

- **Sub-millisecond latency** — LLM calls add latency (100ms–5s). Use native connectors for latency-critical paths.
- **Deterministic-only transforms** — If your transforms are purely structural (renames, type casts), use Kafka Connect SMTs instead. kafka-connect-ai can do this via Tier 0 deterministic patterns, but SMTs are simpler.
- **Binary data** — kafka-connect-ai works with JSON. For Avro, Protobuf, or binary payloads, use specialised connectors.
- **Full CDC with WAL** — For database replication with transaction ordering, Debezium is purpose-built. kafka-connect-ai uses polling queries.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        kafka-connect-ai                             │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌──────────────────┐    ┌──────────────────┐  ┌──────────────────┐│
│  │  Source Adapters  │    │  AI Pipeline     │  │  Sink Adapters   ││
│  │  (SPI discovery)  │───▶│                  │─▶│  (SPI discovery) ││
│  │                   │    │  ┌────────────┐  │  │                  ││
│  │  • HTTP           │    │  │ Batch      │  │  │  • HTTP          ││
│  │  • JDBC           │    │  │ Accumulator│  │  │  • JDBC          ││
│  │  • Kafka (K2K)    │    │  └─────┬──────┘  │  │                  ││
│  └──────────────────┘    │        │         │  └──────────────────┘│
│                           │  ┌─────▼──────┐  │                     │
│  ┌──────────────────┐    │  │ Model      │  │  ┌──────────────────┐│
│  │  Schema           │    │  │ Router     │  │  │  Metrics         ││
│  │  Discovery Agent  │    │  │ (4-tier)   │  │  │  (16 JMX via     ││
│  │  (startup)        │    │  └─────┬──────┘  │  │   Micrometer)    ││
│  └──────────────────┘    │        │         │  └──────────────────┘│
│                           │  ┌─────▼──────┐  │                     │
│                           │  │ Parallel   │  │  ┌──────────────────┐│
│                           │  │ LLM Exec   │  │  │  Semantic Cache  ││
│                           │  │ (N calls)  │◀─┼─▶│  (Redis +        ││
│                           │  └─────┬──────┘  │  │   embeddings)    ││
│                           │        │         │  └──────────────────┘│
│                           │  ┌─────▼──────┐  │                     │
│                           │  │ Schema     │  │                     │
│                           │  │ Enforcer   │  │                     │
│                           │  └────────────┘  │                     │
│                           └──────────────────┘                     │
│                                                                     │
│  LLM Providers: Anthropic (Claude) │ OpenAI (GPT)                  │
└─────────────────────────────────────────────────────────────────────┘
```

**Data flow:**

1. Source adapter fetches records (HTTP poll, JDBC query, Kafka consume)
2. Batch accumulator groups records by size or time
3. Model router selects the cheapest capable model tier
4. Parallel executor splits batches and calls the LLM concurrently
5. Semantic cache returns cached results for similar records
6. Schema enforcer validates output against the target JSON Schema
7. Records are written to Kafka (source) or external systems (sink)

## Documentation

| Document | Description |
|----------|-------------|
| [Quick Start](docs/quickstart.md) | Get a connector running in 10 minutes |
| [Configuration Reference](docs/configuration.md) | All 60+ configuration properties |
| [Use Cases](docs/use-cases.md) | 9 common integration patterns |
| [Deployment Guide](docs/deployment.md) | Docker, Kubernetes/Strimzi, bare metal |
| [Troubleshooting](docs/troubleshooting.md) | Common issues, debugging, metrics reference |
| [Installation](docs/INSTALL.md) | Installation methods |

## Cost Optimisation

kafka-connect-ai minimises LLM spend through four layers:

| Layer | Mechanism | Savings |
|-------|-----------|---------|
| **Tier 0: Deterministic** | Field renames, type casts, timestamp formatting — no LLM call | 100% |
| **Tier 1: Fast model** | Simple flat records routed to Claude Haiku | ~90% vs default |
| **Semantic cache** | Similar records return cached results from Redis | 100% per cache hit |
| **Prompt caching** | Anthropic caches repeated system prompts | ~50% input tokens |

Enable all layers:

```json
{
  "ai.router.enabled": "true",
  "ai.router.deterministic.patterns": "field_rename,type_cast,timestamp_format",
  "ai.llm.model.fast": "claude-haiku-4-5-20251001",
  "ai.cache.enabled": "true",
  "ai.cache.redis.url": "redis://redis:6379",
  "ai.agent.enable.prompt.caching": "true"
}
```

## Project Structure

```
kafka-connect-ai/
├── kafka-connect-ai-api/                  # Core interfaces and models (no dependencies)
│   └── src/main/java/
│       └── sh/oso/kafka-connect-ai/api/
│           ├── adapter/        # SourceAdapter, SinkAdapter interfaces
│           ├── config/         # Shared config constants
│           ├── error/          # RetryableException, NonRetryableException
│           ├── model/          # RawRecord, TransformedRecord, SourceOffset
│           └── pipeline/       # AgentPipeline interface
├── kafka-connect-ai-adapter-http/         # HTTP source + sink adapter
│   └── src/main/java/
│       └── sh/oso/kafka-connect-ai/adapter/http/
│           ├── auth/           # 5 auth strategies (Basic, Bearer, API Key, OAuth2, None)
│           ├── pagination/     # 5 pagination strategies (Cursor, Offset, Page, Link, None)
│           └── ratelimit/      # Token-bucket rate limiter
├── kafka-connect-ai-adapter-jdbc/         # JDBC source + sink adapter (HikariCP)
│   └── src/main/java/
│       └── sh/oso/kafka-connect-ai/adapter/jdbc/
│           ├── query/          # QueryBuilder, 4 query modes
│           └── sql/            # SqlGenerator, upsert, auto-DDL
├── kafka-connect-ai-adapter-kafka/        # Kafka-to-Kafka source adapter
│   └── src/main/java/
│       └── sh/oso/kafka-connect-ai/adapter/kafka/
├── kafka-connect-ai-connect/              # Connectors, pipeline, LLM, cache, metrics, uber JAR
│   └── src/main/java/
│       └── sh/oso/kafka-connect-ai/connect/
│           ├── cache/          # SemanticCache, EmbeddingClient
│           ├── config/         # AiSourceConfig, AiSinkConfig
│           ├── llm/            # AnthropicClient, OpenAiClient, LlmClientFactory
│           ├── metrics/        # AiConnectMetrics (16 JMX metrics), LogContext
│           ├── pipeline/       # BasicAgentPipeline, BatchAccumulator, ModelRouter,
│           │                   # ParallelLlmExecutor, SchemaEnforcer, SchemaDiscoveryAgent,
│           │                   # DeterministicTransformer
│           ├── sink/           # AiSinkConnector, AiSinkTask
│           ├── source/         # AiSourceConnector, AiSourceTask
│           └── spi/            # AdapterRegistry (ServiceLoader discovery)
├── kafka-connect-ai-integration-tests/    # End-to-end tests (Testcontainers, WireMock, PostgreSQL, Redis)
├── docker/                     # Dockerfile + docker-compose.yml
└── docs/                       # Documentation
```

## Building from Source

**Requirements:**
- Java 17+
- Maven 3.9+

```bash
# Clone
git clone https://github.com/osodevops/kafka-connect-ai.git
cd kafka-connect-ai

# Build all modules
mvn clean package -DskipTests

# Run unit tests (143 tests)
mvn test -pl kafka-connect-ai-api,kafka-connect-ai-adapter-http,kafka-connect-ai-adapter-jdbc,kafka-connect-ai-adapter-kafka,kafka-connect-ai-connect

# Run integration tests (27 tests, requires Docker)
mvn verify -pl kafka-connect-ai-integration-tests
```

## Running Tests

```bash
# Unit tests only
mvn test -pl kafka-connect-ai-api,kafka-connect-ai-adapter-http,kafka-connect-ai-adapter-jdbc,kafka-connect-ai-adapter-kafka,kafka-connect-ai-connect

# Integration tests (Testcontainers — requires Docker)
mvn verify -pl kafka-connect-ai-integration-tests -am

# All tests
mvn verify
```

**Integration test infrastructure** (started automatically by Testcontainers):
- Kafka (KRaft mode)
- PostgreSQL 16
- Redis Stack (with RediSearch)
- WireMock (HTTP API simulation)

## Looking for Enterprise Apache Kafka Support?

[OSO](https://oso.sh) engineers are solely focused on deploying, operating, and maintaining Apache Kafka platforms. If you need SLA-backed support or advanced features for compliance and security, our **Enterprise Edition** extends the core tool with capabilities designed for large-scale, regulated environments.

### kafka-connect-ai: Enterprise Edition

| Feature Category | Enterprise Capability |
|------------------|----------------------|
| **Security & Compliance** | AES-256 Encryption (client-side encryption at rest) |
| | GDPR Compliance Tools (PII masking, data retention policies) |
| | Audit Logging (comprehensive trail of all operations) |
| | Role-Based Access Control (granular permissions) |
| **Advanced Integrations** | Schema Registry Integration (Avro/Protobuf with schema evolution) |
| | Secrets Management (Vault / AWS Secrets Manager integration) |
| | SSO / OIDC (Okta, Azure AD, Google Auth) |
| **Scale & Operations** | Custom Adapter SDK (build your own source/sink adapters) |
| | Multi-Tenant LLM Gateway (shared API key management, cost allocation) |
| | Log Shipping (Datadog, Splunk, Grafana Loki) |
| | Advanced Metrics & Dashboard (throughput, latency, cost drill-down UI) |
| **Support** | 24/7 SLA-Backed Support & dedicated Kafka consulting |

Need help resolving operational issues or planning an AI-powered data integration strategy? Our team of experts can help you design, deploy, and operate kafka-connect-ai at scale.

**[Talk with an expert today](https://oso.sh/contact/)** or email us at **enquiries@oso.sh**.

## Contributing

We welcome contributions of all kinds!

- **Report Bugs:** Found a bug? Open an [issue on GitHub](https://github.com/osodevops/kafka-connect-ai/issues).
- **Suggest Features:** Have an idea? [Request a feature](https://github.com/osodevops/kafka-connect-ai/issues/new).
- **Contribute Code:** Check out our [good first issues](https://github.com/osodevops/kafka-connect-ai/labels/good%20first%20issue) for beginner-friendly tasks.
- **Improve Docs:** Help us improve the documentation by submitting pull requests.

### Development Workflow

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

kafka-connect-ai is licensed under the [Apache License 2.0](LICENSE).

## Acknowledgments

Built with:
- [Apache Kafka Connect](https://kafka.apache.org/documentation/#connect) — Connector framework
- [Jackson](https://github.com/FasterXML/jackson) — JSON processing
- [HikariCP](https://github.com/brettwooldridge/HikariCP) — JDBC connection pooling
- [Micrometer](https://micrometer.io/) — Metrics instrumentation
- [Jedis](https://github.com/redis/jedis) — Redis client
- [Testcontainers](https://testcontainers.com/) — Integration testing

---

<p align="center">
  Made with care by <a href="https://oso.sh">OSO</a>
</p>
