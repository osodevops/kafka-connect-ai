package sh.oso.connect.ai.adapter.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.connect.ai.adapter.kafka.config.KafkaSourceConfig;
import sh.oso.connect.ai.api.adapter.SourceAdapter;
import sh.oso.connect.ai.api.config.AiConnectConfig;
import sh.oso.connect.ai.api.error.RetryableException;
import sh.oso.connect.ai.api.model.RawRecord;
import sh.oso.connect.ai.api.model.SourceOffset;

import java.time.Duration;
import java.util.*;
import java.util.regex.Pattern;

public class KafkaSourceAdapter implements SourceAdapter {

    private static final Logger log = LoggerFactory.getLogger(KafkaSourceAdapter.class);

    private KafkaConsumer<byte[], byte[]> consumer;
    private KafkaSourceConfig config;
    private String clusterAlias;

    @Override
    public String type() {
        return "kafka";
    }

    @Override
    public ConfigDef configDef() {
        return new ConfigDef()
                .define(AiConnectConfig.KAFKA_SOURCE_BOOTSTRAP_SERVERS, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, "Source Kafka bootstrap servers")
                .define(AiConnectConfig.KAFKA_SOURCE_TOPICS, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, "Comma-separated source topics")
                .define(AiConnectConfig.KAFKA_SOURCE_TOPICS_REGEX, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, "Source topic regex pattern")
                .define(AiConnectConfig.KAFKA_SOURCE_GROUP_ID, ConfigDef.Type.STRING, "connect-ai-k2k-consumer", ConfigDef.Importance.MEDIUM, "Consumer group ID")
                .define(AiConnectConfig.KAFKA_SOURCE_POLL_TIMEOUT_MS, ConfigDef.Type.LONG, 1000L, ConfigDef.Importance.LOW, "Consumer poll timeout in ms")
                .define(AiConnectConfig.KAFKA_SOURCE_SECURITY_PROTOCOL, ConfigDef.Type.STRING, "PLAINTEXT", ConfigDef.Importance.MEDIUM, "Security protocol: PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL");
    }

    @Override
    public void start(Map<String, String> props) {
        this.config = new KafkaSourceConfig(props);
        this.clusterAlias = config.bootstrapServers().split(",")[0].replace(":", "_");

        Properties consumerProps = config.consumerProperties();
        this.consumer = new KafkaConsumer<>(consumerProps);

        String topicsRegex = config.topicsRegex();
        List<String> topics = config.topics();

        if (!topicsRegex.isEmpty()) {
            consumer.subscribe(Pattern.compile(topicsRegex));
            log.info("KafkaSourceAdapter subscribed to regex: {}", topicsRegex);
        } else if (!topics.isEmpty()) {
            consumer.subscribe(topics);
            log.info("KafkaSourceAdapter subscribed to topics: {}", topics);
        } else {
            throw new IllegalArgumentException(
                    "Either kafka.source.topics or kafka.source.topics.regex must be configured");
        }
    }

    @Override
    public List<RawRecord> fetch(SourceOffset currentOffset, int maxRecords) throws InterruptedException {
        try {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(
                    Duration.ofMillis(config.pollTimeoutMs()));

            if (records.isEmpty()) {
                return List.of();
            }

            List<RawRecord> rawRecords = new ArrayList<>();
            int count = 0;

            for (ConsumerRecord<byte[], byte[]> record : records) {
                if (maxRecords > 0 && count >= maxRecords) {
                    break;
                }

                Map<String, String> partition = Map.of(
                        "adapter", "kafka",
                        "cluster", clusterAlias,
                        "topic", record.topic(),
                        "partition", String.valueOf(record.partition())
                );

                Map<String, Object> offset = Map.of(
                        "offset", record.offset()
                );

                SourceOffset sourceOffset = new SourceOffset(partition, offset);

                Map<String, String> metadata = Map.of(
                        "source.topic", record.topic(),
                        "source.partition", String.valueOf(record.partition()),
                        "source.offset", String.valueOf(record.offset())
                );

                rawRecords.add(new RawRecord(
                        record.key(),
                        record.value(),
                        metadata,
                        sourceOffset
                ));

                count++;
            }

            log.debug("Fetched {} records from Kafka source", rawRecords.size());
            return rawRecords;
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                throw (InterruptedException) e;
            }
            throw new RetryableException("Failed to poll from Kafka source: " + e.getMessage(), e);
        }
    }

    @Override
    public void commitOffset(SourceOffset offset) {
        if (consumer != null) {
            consumer.commitSync();
        }
    }

    @Override
    public boolean isHealthy() {
        return consumer != null;
    }

    @Override
    public void stop() {
        if (consumer != null) {
            consumer.close();
        }
        log.info("KafkaSourceAdapter stopped");
    }
}
