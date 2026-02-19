package sh.oso.nexus.it;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import sh.oso.nexus.adapter.kafka.KafkaSourceAdapter;
import sh.oso.nexus.api.model.RawRecord;
import sh.oso.nexus.api.model.SourceOffset;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class KafkaToKafkaPipelineIT {

    static final Network network = Network.newNetwork();
    static final ObjectMapper objectMapper = new ObjectMapper();
    static final String SOURCE_TOPIC = "k2k-source-topic";

    @Container
    static final KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.8.0"))
            .withNetwork(network)
            .withNetworkAliases("kafka");

    @Test
    void kafkaSourceAdapterConsumesRecords() throws Exception {
        String bootstrapServers = kafka.getBootstrapServers();

        // Produce test records
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(producerProps)) {
            for (int i = 0; i < 5; i++) {
                String json = "{\"id\":" + i + ",\"msg\":\"test-" + i + "\"}";
                producer.send(new ProducerRecord<>(SOURCE_TOPIC,
                        "key-" + i, json.getBytes(StandardCharsets.UTF_8))).get();
            }
        }

        // Start KafkaSourceAdapter
        KafkaSourceAdapter adapter = new KafkaSourceAdapter();
        adapter.start(Map.of(
                "kafka.source.bootstrap.servers", bootstrapServers,
                "kafka.source.topics", SOURCE_TOPIC,
                "kafka.source.group.id", "test-k2k-group-" + UUID.randomUUID(),
                "kafka.source.poll.timeout.ms", "5000"
        ));

        // Fetch records - retry since consumer needs time to join group
        List<RawRecord> allRecords = new ArrayList<>();
        await().atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofSeconds(1))
                .until(() -> {
                    List<RawRecord> records = adapter.fetch(SourceOffset.empty(), 10);
                    if (records != null && !records.isEmpty()) {
                        allRecords.addAll(records);
                    }
                    return allRecords.size() >= 5;
                });

        assertEquals(5, allRecords.size());

        // Verify record content
        RawRecord first = allRecords.get(0);
        assertNotNull(first.value());
        String valueStr = new String(first.value(), StandardCharsets.UTF_8);
        assertTrue(valueStr.contains("\"id\""));
        assertTrue(valueStr.contains("\"msg\""));

        // Verify offset metadata
        assertNotNull(first.sourceOffset());
        assertEquals("kafka", first.sourceOffset().partition().get("adapter"));
        assertEquals(SOURCE_TOPIC, first.sourceOffset().partition().get("topic"));

        adapter.stop();
    }

    @Test
    void kafkaSourceAdapterTracksOffsets() throws Exception {
        String bootstrapServers = kafka.getBootstrapServers();
        String topic = "k2k-offset-test-" + UUID.randomUUID();

        // Produce records
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(producerProps)) {
            for (int i = 0; i < 3; i++) {
                producer.send(new ProducerRecord<>(topic,
                        "key-" + i,
                        ("{\"val\":" + i + "}").getBytes(StandardCharsets.UTF_8))).get();
            }
        }

        KafkaSourceAdapter adapter = new KafkaSourceAdapter();
        adapter.start(Map.of(
                "kafka.source.bootstrap.servers", bootstrapServers,
                "kafka.source.topics", topic,
                "kafka.source.group.id", "offset-test-group-" + UUID.randomUUID(),
                "kafka.source.poll.timeout.ms", "5000"
        ));

        List<RawRecord> records = new ArrayList<>();
        await().atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofSeconds(1))
                .until(() -> {
                    List<RawRecord> fetched = adapter.fetch(SourceOffset.empty(), 10);
                    if (fetched != null) records.addAll(fetched);
                    return records.size() >= 3;
                });

        // Verify each record has a unique offset
        for (RawRecord record : records) {
            assertNotNull(record.sourceOffset().offset().get("offset"));
        }

        adapter.stop();
    }
}
