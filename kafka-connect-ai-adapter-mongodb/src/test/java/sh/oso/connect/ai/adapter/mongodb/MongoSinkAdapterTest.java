package sh.oso.connect.ai.adapter.mongodb;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class MongoSinkAdapterTest {

    private MongoSinkAdapter adapter;

    @BeforeEach
    void setUp() {
        adapter = new MongoSinkAdapter();
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
        assertNotNull(configDef.configKeys().get("mongodb.write.mode"));
        assertNotNull(configDef.configKeys().get("mongodb.batch.size"));
    }

    @Test
    void writeEmptyListIsNoOp() {
        assertDoesNotThrow(() -> adapter.write(Collections.emptyList()));
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
