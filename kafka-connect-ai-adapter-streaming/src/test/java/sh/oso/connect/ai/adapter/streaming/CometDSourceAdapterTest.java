package sh.oso.connect.ai.adapter.streaming;

import org.apache.kafka.common.config.ConfigDef;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sh.oso.connect.ai.api.config.AiConnectConfig;

import static org.junit.jupiter.api.Assertions.*;

class CometDSourceAdapterTest {

    private CometDSourceAdapter adapter;

    @BeforeEach
    void setUp() {
        adapter = new CometDSourceAdapter();
    }

    @Test
    void typeReturnsCometd() {
        assertEquals("cometd", adapter.type());
    }

    @Test
    void configDefContainsRequiredKeys() {
        ConfigDef configDef = adapter.configDef();

        assertNotNull(configDef.configKeys().get(AiConnectConfig.COMETD_URL));
        assertNotNull(configDef.configKeys().get(AiConnectConfig.COMETD_CHANNEL));
        assertNotNull(configDef.configKeys().get(AiConnectConfig.STREAMING_BUFFER_CAPACITY));
        assertNotNull(configDef.configKeys().get(AiConnectConfig.STREAMING_RECONNECT_BACKOFF_MS));
        assertNotNull(configDef.configKeys().get(AiConnectConfig.STREAMING_MAX_RECONNECT_BACKOFF_MS));
    }

    @Test
    void isHealthyFalseBeforeStart() {
        assertFalse(adapter.isHealthy());
    }

    @Test
    void stopIsIdempotent() {
        assertDoesNotThrow(() -> {
            adapter.stop();
            adapter.stop();
        });
    }

    @Test
    void closeCallsStop() {
        assertDoesNotThrow(() -> {
            adapter.close();
            adapter.close();
        });
    }

    @Test
    void cometdUrlIsHighImportance() {
        ConfigDef configDef = adapter.configDef();
        ConfigDef.ConfigKey key = configDef.configKeys().get(AiConnectConfig.COMETD_URL);

        assertEquals(ConfigDef.Importance.HIGH, key.importance);
    }

    @Test
    void cometdChannelIsHighImportance() {
        ConfigDef configDef = adapter.configDef();
        ConfigDef.ConfigKey key = configDef.configKeys().get(AiConnectConfig.COMETD_CHANNEL);

        assertEquals(ConfigDef.Importance.HIGH, key.importance);
    }
}
