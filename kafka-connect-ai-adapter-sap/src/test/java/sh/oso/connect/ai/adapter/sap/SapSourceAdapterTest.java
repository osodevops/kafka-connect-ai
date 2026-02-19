package sh.oso.connect.ai.adapter.sap;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SapSourceAdapterTest {

    private SapSourceAdapter adapter;

    @BeforeEach
    void setUp() {
        adapter = new SapSourceAdapter();
    }

    @Test
    void typeReturnsSap() {
        assertEquals("sap", adapter.type());
    }

    @Test
    void configDefContainsRequiredFields() {
        var configDef = adapter.configDef();
        assertNotNull(configDef.configKeys().get("sap.ashost"));
        assertNotNull(configDef.configKeys().get("sap.sysnr"));
        assertNotNull(configDef.configKeys().get("sap.client"));
        assertNotNull(configDef.configKeys().get("sap.user"));
        assertNotNull(configDef.configKeys().get("sap.password"));
        assertNotNull(configDef.configKeys().get("sap.mode"));
        assertNotNull(configDef.configKeys().get("sap.function"));
        assertNotNull(configDef.configKeys().get("sap.poll.interval.ms"));
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
