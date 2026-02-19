# Nexus Troubleshooting Guide

Common issues, debugging techniques, and monitoring reference.

---

## Common Issues

### Connector Not Found After Deployment

**Symptom:** `curl http://localhost:8083/connector-plugins` does not list Nexus connectors.

**Causes and fixes:**

1. **Plugin path misconfigured** — Ensure `CONNECT_PLUGIN_PATH` (or `plugin.path` in worker properties) includes the directory containing the Nexus JAR.

2. **Non-JAR files in plugin directory** — The Connect plugin scanner expects only JAR files. If you mounted a whole `target/` directory, it will fail. Mount only the uber JAR:
   ```yaml
   volumes:
     - ./nexus-connect-0.1.0-SNAPSHOT-all.jar:/usr/share/java/nexus/nexus-connect.jar
   ```

3. **Wrong JAR variant** — Make sure you're using the `-all.jar` (uber JAR with shaded dependencies), not the thin JAR.

4. **Class version mismatch** — The uber JAR must be compiled for the JDK version your Connect runtime uses. Confluent Platform 7.8.0 runs Java 17. If you see `UnsupportedClassVersionError: class file version 65.0`, rebuild with `maven.compiler.release=17`.

### Connector Fails to Start

**Symptom:** Connector status shows `FAILED` with an error message.

**Debug steps:**

```bash
# Check connector status
curl -s http://localhost:8083/connectors/my-connector/status | python3 -m json.tool

# View Connect worker logs
docker logs nexus-connect --tail 200
```

**Common causes:**

| Error | Cause | Fix |
|-------|-------|-----|
| `Missing required configuration` | Required property not set | Check [Configuration Reference](configuration.md) for required properties |
| `Unknown adapter type` | Invalid `nexus.source.adapter` value | Must be `http`, `jdbc`, or `kafka` |
| `Connection refused` | Target service unreachable | Check network connectivity, hostnames, ports |
| `401 Unauthorized` | Invalid LLM API key | Verify `ai.llm.api.key` is correct and active |
| `ClassNotFoundException` | Missing JDBC driver | Add `jdbc.driver.class` or ensure driver is on classpath |

### LLM Call Failures

**Symptom:** Records are not being transformed, or appearing in the DLQ topic.

**Debug steps:**

1. Check Connect worker logs for LLM-related errors:
   ```bash
   docker logs nexus-connect 2>&1 | grep -i "llm\|anthropic\|openai"
   ```

2. Common LLM errors:

| Error | Cause | Fix |
|-------|-------|-----|
| `401` / `invalid_api_key` | Bad API key | Update `ai.llm.api.key` |
| `429` / `rate_limit_exceeded` | Too many requests | Reduce `ai.batch.parallel.calls`, increase `ai.retry.backoff.ms` |
| `400` / `invalid_model` | Model ID not recognised | Check `ai.llm.model` matches provider's model list |
| `500` / `overloaded` | Provider under load | Retries handle this automatically; increase `ai.max.retries` if needed |
| JSON parse error on response | LLM returned non-JSON | Ensure `ai.agent.system.prompt` explicitly requests JSON output; enable `ai.structured.output` |

### Schema Enforcement Failures

**Symptom:** `SchemaEnforcer` rejects LLM output. Records fail validation.

**Fix:**

1. Verify your `ai.agent.target.schema` is valid JSON Schema
2. Ensure the system prompt instructs the LLM to produce output matching the schema
3. Enable structured output (`ai.structured.output=true`) to enforce the schema at the LLM level
4. Increase `ai.max.retries` — the pipeline retries on schema validation failure

### HTTP Adapter Issues

**Timeout errors:**
- Increase `http.source.timeout.ms` or `http.sink.timeout.ms` (default: 30000ms)

**Rate limiting (429 responses):**
- Reduce `http.rate.limit.rps` to match the API's rate limit
- The sink adapter automatically retries on 429 with exponential backoff

**Pagination not working:**
- Verify `http.pagination.type` matches the API's pagination mechanism
- For cursor pagination, check `http.pagination.cursor.field` points to the correct response field

**OAuth2 token failures:**
- Verify `http.auth.oauth2.token.url`, `client.id`, and `client.secret`
- Check that `http.auth.oauth2.scope` is correct (or omit if not required)

### JDBC Adapter Issues

**Connection failures:**
- Verify `jdbc.url` format: `jdbc:postgresql://host:port/database`
- Check `jdbc.user` and `jdbc.password`
- Ensure the database is reachable from the Connect worker network

**No records captured:**
- For `timestamp` mode: ensure `jdbc.timestamp.column` exists and is updated on changes
- For `incrementing` mode: ensure `jdbc.incrementing.column` is a monotonically increasing ID
- Check `jdbc.poll.interval.ms` — reduce for faster capture

**Sink auto-DDL failures:**
- `jdbc.sink.auto.ddl=true` creates tables based on the first batch of records
- Ensure the database user has `CREATE TABLE` privileges
- Complex nested JSON may not map cleanly to SQL columns

### Kafka-to-Kafka Adapter Issues

**Consumer not receiving records:**
- Verify `kafka.source.bootstrap.servers` points to the upstream cluster
- Check `kafka.source.topics` or `kafka.source.topics.regex`
- Set `kafka.source.consumer.auto.offset.reset=earliest` to read from the beginning
- Verify the consumer group (`kafka.source.group.id`) has no committed offsets blocking consumption

**Security:**
- For SASL/SSL clusters, set `kafka.source.security.protocol` and pass SASL properties via `kafka.source.consumer.*`

### Semantic Cache Not Working

**Symptom:** `ai.cache.enabled=true` but cache hit ratio is 0.

**Fix:**

1. **Redis not reachable** — Verify `ai.cache.redis.url` and that Redis Stack is running (requires RediSearch module for vector search)
2. **Threshold too high** — Lower `ai.cache.similarity.threshold` (default 0.95). For exact-match deduplication, 0.99 works well. For fuzzy matching, try 0.90.
3. **Embedding API failure** — The cache uses OpenAI embeddings (`ai.cache.embedding.model`). Verify the API key has access to OpenAI's embedding endpoint.
4. **Records too unique** — If every record is genuinely different, the cache won't help. Check the `nexus.cache.hit.ratio` metric.

### Docker-Specific Issues

**Connect worker not starting:**
```bash
# Check if all dependencies are healthy
docker compose ps

# Kafka must be healthy before Connect starts
docker compose logs kafka | tail -20
```

**Volume mount issues:**
- Mount a single JAR file, not a directory:
  ```yaml
  volumes:
    - ./nexus-connect-0.1.0-SNAPSHOT-all.jar:/usr/share/java/nexus/nexus-connect.jar
  ```

**Container networking:**
- Services in Docker Compose use the service name as hostname (e.g. `kafka:29092`, not `localhost:9092`)
- External connections from the host use `localhost` with the mapped port

---

## Debugging Techniques

### Enable DEBUG Logging

Set Connect worker log level for Nexus classes:

```bash
curl -X PUT http://localhost:8083/admin/loggers/sh.oso.nexus \
  -H "Content-Type: application/json" \
  -d '{"level": "DEBUG"}'
```

Reset to INFO:

```bash
curl -X PUT http://localhost:8083/admin/loggers/sh.oso.nexus \
  -H "Content-Type: application/json" \
  -d '{"level": "INFO"}'
```

### Structured Logging

Nexus uses MDC-based structured logging via `LogContext`. Log entries include:

| MDC Key | Description |
|---------|-------------|
| `connector` | Connector name |
| `task` | Task ID |
| `adapter` | Adapter type |
| `topic` | Target topic |

### Inspect Connect Internal Topics

```bash
# View connector configs
docker exec nexus-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic _nexus-configs \
  --from-beginning

# View connector offsets
docker exec nexus-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic _nexus-offsets \
  --from-beginning

# View connector status
docker exec nexus-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic _nexus-status \
  --from-beginning
```

### Consume DLQ Records

If a DLQ topic is configured (`nexus.dlq.topic`), failed records land there with error metadata:

```bash
docker exec nexus-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic my-connector-dlq \
  --from-beginning \
  --property print.headers=true
```

---

## Monitoring & Metrics

Nexus exposes 16 JMX metrics via Micrometer, registered with the `nexus` prefix.

### Metric Reference

#### Records

| Metric | Type | Description |
|--------|------|-------------|
| `nexus.records.processed.total` | Counter | Total records successfully processed |
| `nexus.records.failed.total` | Counter | Total records that failed processing |

#### LLM

| Metric | Type | Description |
|--------|------|-------------|
| `nexus.llm.calls.total` | Counter | Total LLM API calls made |
| `nexus.llm.call.latency` | Timer | LLM call latency (with percentile histogram) |
| `nexus.llm.tokens.input.total` | Counter | Total input tokens consumed |
| `nexus.llm.tokens.output.total` | Counter | Total output tokens produced |
| `nexus.llm.cost.usd.total` | Counter | Estimated total LLM cost in USD |

#### Cache

| Metric | Type | Description |
|--------|------|-------------|
| `nexus.cache.hits.total` | Counter | Total semantic cache hits |
| `nexus.cache.misses.total` | Counter | Total semantic cache misses |
| `nexus.cache.hit.ratio` | Gauge | Cache hit ratio (hits / total lookups) |

#### Router

| Metric | Type | Description |
|--------|------|-------------|
| `nexus.router.tier0.total` | Counter | Records routed to Tier 0 (deterministic) |
| `nexus.router.tier1.total` | Counter | Records routed to Tier 1 (fast model) |
| `nexus.router.tier2.total` | Counter | Records routed to Tier 2 (default model) |
| `nexus.router.tier3.total` | Counter | Records routed to Tier 3 (powerful model) |

#### Adapters

| Metric | Type | Description |
|--------|------|-------------|
| `nexus.adapter.fetch.latency` | Timer | Source adapter fetch latency (with percentile histogram) |
| `nexus.adapter.write.latency` | Timer | Sink adapter write latency (with percentile histogram) |

#### Batching

| Metric | Type | Description |
|--------|------|-------------|
| `nexus.batch.size` | Distribution Summary | Batch sizes processed |

### Accessing Metrics via JMX

Metrics are exposed via JMX using the Micrometer JMX registry. Connect to the Connect worker's JMX port using `jconsole`, `jmxterm`, or any JMX-compatible monitoring tool.

Enable JMX on the Connect worker:

```bash
KAFKA_JMX_OPTS="-Dcom.sun.management.jmxremote \
  -Dcom.sun.management.jmxremote.port=9999 \
  -Dcom.sun.management.jmxremote.authenticate=false \
  -Dcom.sun.management.jmxremote.ssl=false"
```

### Prometheus / Grafana

Use the [JMX Exporter](https://github.com/prometheus/jmx_exporter) as a Java agent to expose metrics in Prometheus format:

```bash
KAFKA_OPTS="-javaagent:/opt/jmx_exporter/jmx_prometheus_javaagent.jar=9404:/opt/jmx_exporter/config.yml"
```

Example `config.yml` for Nexus metrics:

```yaml
rules:
  - pattern: "metrics<name=nexus\\.(.+)><>(.+)"
    name: "nexus_$1"
    type: GAUGE
```

### Key Alerts

| Alert | Condition | Severity |
|-------|-----------|----------|
| Record failures | `nexus.records.failed.total` increasing | Critical |
| LLM latency spike | `nexus.llm.call.latency` p99 > 30s | Warning |
| LLM cost runaway | `nexus.llm.cost.usd.total` rate > budget/hour | Warning |
| Cache ineffective | `nexus.cache.hit.ratio` < 0.3 | Info |
| All calls on Tier 3 | `nexus.router.tier3.total` > 50% of total | Warning |

---

## Connect REST API Reference

Quick reference for the Kafka Connect REST API endpoints used with Nexus.

```bash
# List connector plugins
GET /connector-plugins

# List active connectors
GET /connectors

# Create a connector
POST /connectors
Content-Type: application/json
{"name": "...", "config": {...}}

# Get connector status
GET /connectors/{name}/status

# Get connector config
GET /connectors/{name}/config

# Update connector config
PUT /connectors/{name}/config
Content-Type: application/json
{...config...}

# Pause connector
PUT /connectors/{name}/pause

# Resume connector
PUT /connectors/{name}/resume

# Restart connector
POST /connectors/{name}/restart

# Restart a specific task
POST /connectors/{name}/tasks/{taskId}/restart

# Delete connector
DELETE /connectors/{name}

# Set log level
PUT /admin/loggers/{logger}
Content-Type: application/json
{"level": "DEBUG"}
```

All endpoints are relative to `http://localhost:8083` (default Connect REST port).
