package sh.oso.connect.ai.adapter.kafka.config;

import sh.oso.connect.ai.api.config.AiConnectConfig;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class KafkaSourceConfig {

    public static final String CONSUMER_PREFIX = "kafka.source.consumer.";

    private final Map<String, String> props;

    public KafkaSourceConfig(Map<String, String> props) {
        this.props = props;
    }

    public String bootstrapServers() {
        return props.getOrDefault(AiConnectConfig.KAFKA_SOURCE_BOOTSTRAP_SERVERS, "");
    }

    public List<String> topics() {
        String topics = props.getOrDefault(AiConnectConfig.KAFKA_SOURCE_TOPICS, "");
        if (topics.isEmpty()) {
            return List.of();
        }
        return Arrays.asList(topics.split(","));
    }

    public String topicsRegex() {
        return props.getOrDefault(AiConnectConfig.KAFKA_SOURCE_TOPICS_REGEX, "");
    }

    public String groupId() {
        return props.getOrDefault(AiConnectConfig.KAFKA_SOURCE_GROUP_ID, "connect-ai-k2k-consumer");
    }

    public long pollTimeoutMs() {
        return Long.parseLong(props.getOrDefault(AiConnectConfig.KAFKA_SOURCE_POLL_TIMEOUT_MS, "1000"));
    }

    public String securityProtocol() {
        return props.getOrDefault(AiConnectConfig.KAFKA_SOURCE_SECURITY_PROTOCOL, "PLAINTEXT");
    }

    public Properties consumerProperties() {
        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", bootstrapServers());
        consumerProps.put("group.id", groupId());
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumerProps.put("enable.auto.commit", "false");
        consumerProps.put("auto.offset.reset", "earliest");
        consumerProps.put("security.protocol", securityProtocol());

        // Pass through any consumer.* properties (overrides above defaults)
        for (Map.Entry<String, String> entry : props.entrySet()) {
            if (entry.getKey().startsWith(CONSUMER_PREFIX)) {
                String key = entry.getKey().substring(CONSUMER_PREFIX.length());
                consumerProps.put(key, entry.getValue());
            }
        }

        return consumerProps;
    }
}
