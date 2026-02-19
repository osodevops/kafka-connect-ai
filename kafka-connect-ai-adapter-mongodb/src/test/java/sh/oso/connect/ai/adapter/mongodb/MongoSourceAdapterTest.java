package sh.oso.connect.ai.adapter.mongodb;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class MongoSourceAdapterTest {

    private MongoSourceAdapter adapter;

    @BeforeEach
    void setUp() {
        adapter = new MongoSourceAdapter();
    }

    @Test
    void typeReturnsMongoDb() {
        assertEquals("mongodb", adapter.type());
    }

    @Test
    void configDefContainsRequiredFields() {
        var configDef = adapter.configDef();
        assertNotNull(configDef.configKeys().get("mongodb.connection.string"));
        assertNotNull(configDef.configKeys().get("mongodb.database"));
        assertNotNull(configDef.configKeys().get("mongodb.collection"));
        assertNotNull(configDef.configKeys().get("mongodb.poll.mode"));
        assertNotNull(configDef.configKeys().get("mongodb.pipeline"));
        assertNotNull(configDef.configKeys().get("mongodb.poll.interval.ms"));
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
