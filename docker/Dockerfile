FROM confluentinc/cp-kafka-connect:7.8.0

ARG JAR_FILE=kafka-connect-ai-connect-*-all.jar

# Copy the kafka-connect-ai uber JAR into the Connect plugin path
COPY --chown=appuser:appuser ${JAR_FILE} /usr/share/java/kafka-connect-ai/

ENV CONNECT_PLUGIN_PATH="/usr/share/java,/usr/share/confluent-hub-components"

LABEL org.opencontainers.image.source="https://github.com/osodevops/kafka-connect-ai"
LABEL org.opencontainers.image.description="AI-Powered Universal Kafka Connect Connector"
LABEL org.opencontainers.image.licenses="Apache-2.0"
