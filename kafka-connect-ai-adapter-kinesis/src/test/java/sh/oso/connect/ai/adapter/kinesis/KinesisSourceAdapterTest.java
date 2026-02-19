package sh.oso.connect.ai.adapter.kinesis;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class KinesisSourceAdapterTest {

    private KinesisSourceAdapter adapter;

    @BeforeEach
    void setUp() {
        adapter = new KinesisSourceAdapter();
    }

    @Test
    void typeReturnsKinesis() {
        assertEquals("kinesis", adapter.type());
    }

    @Test
    void configDefContainsRequiredFields() {
        var configDef = adapter.configDef();
        assertNotNull(configDef.configKeys().get("kinesis.stream.name"));
        assertNotNull(configDef.configKeys().get("kinesis.region"));
        assertNotNull(configDef.configKeys().get("kinesis.iterator.type"));
        assertNotNull(configDef.configKeys().get("kinesis.poll.interval.ms"));
        assertNotNull(configDef.configKeys().get("kinesis.batch.size"));
    }

    @Test
    void isHealthyReturnsFalseBeforeStart() {
        assertFalse(adapter.isHealthy());
    }

    @Test
    void stopIsIdempotent() {
        assertDoesNotThrow(() -> adapter.stop());
        assertDoesNotThrow(() -> adapter.stop());
    }
}
