# kafka-connect-ai Deployment Guide

Production deployment options for the kafka-connect-ai Kafka Connect connector.

---

## Deployment Options

| Method | Best For |
|--------|----------|
| [Docker Compose](#docker-compose) | Local development, testing |
| [Standalone Docker](#standalone-docker) | Single-node deployments |
| [Kubernetes / Strimzi](#kubernetes--strimzi) | Production Kubernetes clusters |
| [Bare Metal / VM](#bare-metal--vm) | Traditional infrastructure |
| [Confluent Platform](#confluent-platform) | Existing Confluent installations |

---

## Docker Compose

The project includes a ready-to-use `docker/docker-compose.yml` for local development.

### Services

| Service | Image | Port | Purpose |
|---------|-------|------|---------|
| kafka | confluentinc/cp-kafka:7.8.0 | 9092 | Kafka broker (KRaft mode) |
| schema-registry | confluentinc/cp-schema-registry:7.8.0 | 8081 | Schema Registry |
| connect | Custom (kafka-connect-ai) | 8083 | Kafka Connect + kafka-connect-ai plugin |
| postgres | postgres:16-alpine | 5432 | Sample database |
| redis | redis/redis-stack:7.4.0-v2 | 6379, 8001 | Semantic cache + RedisInsight |

### Start the Stack

```bash
# Build the uber JAR first
mvn clean package -pl kafka-connect-ai-connect -am -DskipTests

# Start all services
cd docker
docker compose up -d

# Check health
docker compose ps
```

### Environment Variables

Override Connect worker config through environment variables prefixed with `CONNECT_`:

```yaml
environment:
  CONNECT_BOOTSTRAP_SERVERS: kafka:29092
  CONNECT_GROUP_ID: kafka-connect-ai-connect-group
  CONNECT_KEY_CONVERTER: org.apache.kafka.connect.storage.StringConverter
  CONNECT_VALUE_CONVERTER: org.apache.kafka.connect.json.JsonConverter
  CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE: "false"
```

---

## Standalone Docker

Run the kafka-connect-ai Connect image without Docker Compose, connecting to existing Kafka infrastructure.

### Using the Pre-built Image

```bash
docker run -d \
  --name kafka-connect-ai-connect \
  -p 8083:8083 \
  -e CONNECT_BOOTSTRAP_SERVERS=kafka-1:9092,kafka-2:9092,kafka-3:9092 \
  -e CONNECT_REST_ADVERTISED_HOST_NAME=kafka-connect-ai-connect \
  -e CONNECT_REST_PORT=8083 \
  -e CONNECT_GROUP_ID=kafka-connect-ai-connect-group \
  -e CONNECT_CONFIG_STORAGE_TOPIC=_kcai-configs \
  -e CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR=3 \
  -e CONNECT_OFFSET_STORAGE_TOPIC=_kcai-offsets \
  -e CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR=3 \
  -e CONNECT_STATUS_STORAGE_TOPIC=_kcai-status \
  -e CONNECT_STATUS_STORAGE_REPLICATION_FACTOR=3 \
  -e CONNECT_KEY_CONVERTER=org.apache.kafka.connect.storage.StringConverter \
  -e CONNECT_VALUE_CONVERTER=org.apache.kafka.connect.json.JsonConverter \
  -e CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE=false \
  -e CONNECT_PLUGIN_PATH=/usr/share/java,/usr/share/confluent-hub-components \
  ghcr.io/osodevops/kafka-connect-ai-connect:latest
```

### Building a Custom Image

```dockerfile
FROM confluentinc/cp-kafka-connect:7.8.0

# Copy the kafka-connect-ai uber JAR
COPY kafka-connect-ai-connect-*-all.jar /usr/share/java/kafka-connect-ai/

ENV CONNECT_PLUGIN_PATH="/usr/share/java,/usr/share/confluent-hub-components"
```

Build and run:

```bash
docker build -t my-kafka-connect-ai-connect -f Dockerfile .
docker run -d --name kafka-connect-ai-connect -p 8083:8083 \
  -e CONNECT_BOOTSTRAP_SERVERS=... \
  my-kafka-connect-ai-connect
```

---

## Kubernetes / Strimzi

Deploy kafka-connect-ai on Kubernetes using the Strimzi Kafka operator.

### Prerequisites

- Strimzi operator installed
- Kafka cluster managed by Strimzi

### KafkaConnect Resource

```yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaConnect
metadata:
  name: kafka-connect-ai-connect
  annotations:
    strimzi.io/use-connector-resources: "true"
spec:
  version: 3.8.1
  replicas: 3
  bootstrapServers: my-kafka-cluster-kafka-bootstrap:9093
  tls:
    trustedCertificates:
      - secretName: my-kafka-cluster-cluster-ca-cert
        pattern: "*.crt"
  config:
    group.id: kafka-connect-ai-connect-group
    offset.storage.topic: _kcai-offsets
    offset.storage.replication.factor: 3
    config.storage.topic: _kcai-configs
    config.storage.replication.factor: 3
    status.storage.topic: _kcai-status
    status.storage.replication.factor: 3
    key.converter: org.apache.kafka.connect.storage.StringConverter
    value.converter: org.apache.kafka.connect.json.JsonConverter
    value.converter.schemas.enable: false
  build:
    output:
      type: docker
      image: my-registry/kafka-connect-ai-connect:latest
      pushSecret: registry-credentials
    plugins:
      - name: kafka-connect-ai
        artifacts:
          - type: jar
            url: https://github.com/osodevops/kafka-connect-ai/releases/download/v0.1.0/kafka-connect-ai-connect-0.1.0-all.jar
  resources:
    requests:
      memory: 1Gi
      cpu: 500m
    limits:
      memory: 2Gi
      cpu: 2000m
```

### KafkaConnector Resource

```yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaConnector
metadata:
  name: api-source
  labels:
    strimzi.io/cluster: kafka-connect-ai-connect
spec:
  class: sh.oso.connect.ai.connect.source.AiSourceConnector
  tasksMax: 2
  config:
    connect.ai.source.adapter: http
    connect.ai.topic: api-events
    http.source.url: "https://api.example.com/v1/data"
    http.source.poll.interval.ms: "60000"
    ai.llm.provider: anthropic
    ai.llm.api.key: "${env:ANTHROPIC_API_KEY}"
    ai.llm.model: "claude-sonnet-4-20250514"
    ai.agent.system.prompt: "Transform data into structured events."
```

### Secrets for API Keys

Store LLM API keys as Kubernetes secrets and mount them as environment variables:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: kafka-connect-ai-llm-keys
type: Opaque
stringData:
  ANTHROPIC_API_KEY: sk-ant-...
---
# In KafkaConnect spec:
spec:
  template:
    connectContainer:
      env:
        - name: ANTHROPIC_API_KEY
          valueFrom:
            secretKeyRef:
              name: kafka-connect-ai-llm-keys
              key: ANTHROPIC_API_KEY
```

---

## Bare Metal / VM

Install the kafka-connect-ai connector into an existing Kafka Connect installation.

### 1. Download the Uber JAR

Download from [GitHub Releases](https://github.com/osodevops/kafka-connect-ai/releases):

```bash
# Create plugin directory
mkdir -p /opt/kafka-connect/plugins/kafka-connect-ai

# Download and extract
curl -L https://github.com/osodevops/kafka-connect-ai/releases/download/v0.1.0/kafka-connect-ai-connect-0.1.0.zip \
  -o /tmp/kafka-connect-ai-connect.zip
unzip /tmp/kafka-connect-ai-connect.zip -d /opt/kafka-connect/plugins/kafka-connect-ai/
```

### 2. Configure Plugin Path

In your Connect worker properties (`connect-distributed.properties`):

```properties
plugin.path=/opt/kafka-connect/plugins,/usr/share/java
```

### 3. Restart Workers

```bash
# Restart the Connect worker process
systemctl restart kafka-connect
```

### 4. Verify Installation

```bash
curl -s http://localhost:8083/connector-plugins | grep ai
```

---

## Confluent Platform

For existing Confluent Platform installations.

### Installation

```bash
# Copy uber JAR to Confluent plugin directory
cp kafka-connect-ai-connect-*-all.jar /usr/share/confluent-hub-components/kafka-connect-ai/

# Or use a dedicated plugin directory
mkdir -p /opt/connectors/kafka-connect-ai
cp kafka-connect-ai-connect-*-all.jar /opt/connectors/kafka-connect-ai/
```

Update the Connect worker config to include the plugin path:

```properties
plugin.path=/usr/share/java,/usr/share/confluent-hub-components,/opt/connectors
```

### Confluent Control Center

Once deployed, kafka-connect-ai connectors appear in Confluent Control Center under **Connect > Connectors**. You can create and manage connector instances through the UI.

---

## Production Recommendations

### Replication Factors

For production, set replication factor to 3 (or match your Kafka cluster size):

```
CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR=3
CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR=3
CONNECT_STATUS_STORAGE_REPLICATION_FACTOR=3
```

### Resource Sizing

| Workload | Tasks | Memory | CPU | Notes |
|----------|-------|--------|-----|-------|
| Low (< 100 records/min) | 1 | 1 GB | 0.5 cores | Single connector, no batching |
| Medium (100–1K records/min) | 2–4 | 2 GB | 1–2 cores | Enable batching + parallel calls |
| High (> 1K records/min) | 4–8 | 4 GB | 2–4 cores | Full optimisation stack |

### LLM Cost Control

1. **Enable model routing** (`ai.router.enabled=true`) to use cheaper models for simple records
2. **Enable deterministic patterns** (`ai.router.deterministic.patterns=field_rename,type_cast`) to bypass LLM entirely where possible
3. **Enable semantic cache** (`ai.cache.enabled=true`) to deduplicate similar LLM calls
4. **Enable prompt caching** (`ai.agent.enable.prompt.caching=true`) to reduce Anthropic input token costs
5. **Use batch processing** (`ai.batch.size=50`, `ai.batch.parallel.calls=4`) to amortise per-call overhead

### Security

- Store API keys in external secret managers (Kubernetes Secrets, HashiCorp Vault, AWS Secrets Manager)
- Use the `PASSWORD` config type for `ai.llm.api.key` — Kafka Connect masks it in REST API responses
- Enable TLS for Kafka connections in production
- Use SASL authentication for multi-tenant Kafka clusters
- Network-isolate Redis (semantic cache) from public access

### Monitoring

kafka-connect-ai exposes 16 JMX metrics via Micrometer. See [Troubleshooting](troubleshooting.md#monitoring--metrics) for the full list.

Key metrics to alert on:

| Metric | Alert Condition |
|--------|-----------------|
| `connect.ai.records.failed.total` | > 0 (any failures) |
| `connect.ai.llm.call.latency` | p99 > 30s |
| `connect.ai.cache.hit.ratio` | < 0.5 (cache not effective) |
| `connect.ai.llm.cost.usd.total` | Exceeds budget threshold |

### High Availability

- Run multiple Connect workers in the same `group.id` for automatic failover
- Set `tasks.max` to distribute work across workers
- Use a dead-letter queue (`connect.ai.dlq.topic`) to capture failed records without blocking the pipeline
- Configure `offset.flush.interval.ms` (default 60s) based on your durability requirements
