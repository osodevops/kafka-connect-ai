# Installing kafka-connect-ai

kafka-connect-ai is distributed as an uber JAR that you drop into your Kafka Connect `plugin.path`.

## Option 1: Download from GitHub Releases

1. Go to [Releases](https://github.com/osodevops/kafka-connect-ai/releases) and download the latest `kafka-connect-ai-connect-{version}.zip`
2. Extract the ZIP:
   ```bash
   unzip kafka-connect-ai-connect-*.zip
   ```
3. Copy the uber JAR into a directory on your Kafka Connect `plugin.path`:
   ```bash
   cp kafka-connect-ai-connect-*/kafka-connect-ai-connect-*-all.jar /opt/kafka-connect/plugins/
   ```
4. Restart your Connect workers

## Option 2: Docker

Pull the pre-built image based on Confluent Platform:

```bash
docker pull ghcr.io/osodevops/kafka-connect-ai-connect:latest
```

Use in a docker-compose file:

```yaml
services:
  connect:
    image: ghcr.io/osodevops/kafka-connect-ai-connect:latest
    environment:
      CONNECT_BOOTSTRAP_SERVERS: kafka:9092
      CONNECT_GROUP_ID: kafka-connect-ai-connect
      CONNECT_CONFIG_STORAGE_TOPIC: _kcai-configs
      CONNECT_OFFSET_STORAGE_TOPIC: _kcai-offsets
      CONNECT_STATUS_STORAGE_TOPIC: _kcai-status
      CONNECT_KEY_CONVERTER: org.apache.kafka.connect.storage.StringConverter
      CONNECT_VALUE_CONVERTER: org.apache.kafka.connect.json.JsonConverter
      CONNECT_PLUGIN_PATH: /usr/share/java,/usr/share/confluent-hub-components
```

## Option 3: Build from Source

Requires Java 17+ and Maven 3.9+.

```bash
git clone https://github.com/osodevops/kafka-connect-ai.git
cd kafka-connect-ai
mvn clean package -pl kafka-connect-ai-connect -am -DskipTests
```

The uber JAR is at:
```
kafka-connect-ai-connect/target/kafka-connect-ai-connect-{version}-all.jar
```

Copy it into your Kafka Connect `plugin.path` and restart workers.

## Verifying the Installation

After restarting Connect, check that the kafka-connect-ai connectors are loaded:

```bash
curl -s http://localhost:8083/connector-plugins | jq '.[] | select(.class | startswith("sh.oso.connect.ai"))'
```

You should see `AiSourceConnector` and `AiSinkConnector` in the output.

## Next Steps

- [Quick Start Guide](quickstart.md) — get a connector running in 10 minutes
- [Configuration Reference](configuration.md) — all config properties
- [Use Cases](use-cases.md) — common integration patterns
- [Deployment Guide](deployment.md) — production deployment options
- [Troubleshooting](troubleshooting.md) — debugging and monitoring
