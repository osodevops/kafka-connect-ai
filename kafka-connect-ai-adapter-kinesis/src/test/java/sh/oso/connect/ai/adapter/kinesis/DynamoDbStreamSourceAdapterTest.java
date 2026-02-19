package sh.oso.connect.ai.adapter.kinesis;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DynamoDbStreamSourceAdapterTest {

    private DynamoDbStreamSourceAdapter adapter;

    @BeforeEach
    void setUp() {
        adapter = new DynamoDbStreamSourceAdapter();
    }

    @Test
    void typeReturnsDynamoDbStreams() {
        assertEquals("dynamodb-streams", adapter.type());
    }

    @Test
    void configDefContainsRequiredFields() {
        var configDef = adapter.configDef();
        assertNotNull(configDef.configKeys().get("dynamodb.table.name"));
        assertNotNull(configDef.configKeys().get("dynamodb.region"));
        assertNotNull(configDef.configKeys().get("dynamodb.poll.interval.ms"));
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
