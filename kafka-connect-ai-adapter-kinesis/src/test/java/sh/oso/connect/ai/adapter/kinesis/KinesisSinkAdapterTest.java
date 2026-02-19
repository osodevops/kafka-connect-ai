package sh.oso.connect.ai.adapter.kinesis;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

class KinesisSinkAdapterTest {

    private KinesisSinkAdapter adapter;

    @BeforeEach
    void setUp() {
        adapter = new KinesisSinkAdapter();
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
        assertNotNull(configDef.configKeys().get("kinesis.partition.key"));
        assertNotNull(configDef.configKeys().get("kinesis.batch.size"));
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
