package sh.oso.nexus.adapter.kafka;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class KafkaSourceAdapterTest {

    @Test
    void typeReturnsKafka() {
        KafkaSourceAdapter adapter = new KafkaSourceAdapter();
        assertEquals("kafka", adapter.type());
    }

    @Test
    void configDefContainsBootstrapServers() {
        KafkaSourceAdapter adapter = new KafkaSourceAdapter();
        assertNotNull(adapter.configDef().configKeys().get("kafka.source.bootstrap.servers"));
    }

    @Test
    void configDefContainsTopics() {
        KafkaSourceAdapter adapter = new KafkaSourceAdapter();
        assertNotNull(adapter.configDef().configKeys().get("kafka.source.topics"));
    }

    @Test
    void isHealthyReturnsFalseBeforeStart() {
        KafkaSourceAdapter adapter = new KafkaSourceAdapter();
        assertFalse(adapter.isHealthy());
    }

    @Test
    void startWithoutTopicsOrRegexThrows() {
        KafkaSourceAdapter adapter = new KafkaSourceAdapter();
        assertThrows(IllegalArgumentException.class, () ->
                adapter.start(java.util.Map.of(
                        "kafka.source.bootstrap.servers", "localhost:9092"
                )));
    }
}
