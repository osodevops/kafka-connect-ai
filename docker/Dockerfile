FROM confluentinc/cp-kafka-connect:7.8.0

ARG JAR_FILE=nexus-connect-*-all.jar

# Copy the Nexus uber JAR into the Connect plugin path
COPY --chown=appuser:appuser ${JAR_FILE} /usr/share/java/nexus/

ENV CONNECT_PLUGIN_PATH="/usr/share/java,/usr/share/confluent-hub-components"

LABEL org.opencontainers.image.source="https://github.com/osodevops/nexus"
LABEL org.opencontainers.image.description="Nexus AI-Powered Universal Kafka Connect Connector"
LABEL org.opencontainers.image.licenses="Apache-2.0"
