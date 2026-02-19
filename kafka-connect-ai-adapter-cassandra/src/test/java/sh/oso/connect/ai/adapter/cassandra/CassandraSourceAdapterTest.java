package sh.oso.connect.ai.adapter.cassandra;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CassandraSourceAdapterTest {

    private CassandraSourceAdapter adapter;

    @BeforeEach
    void setUp() {
        adapter = new CassandraSourceAdapter();
    }

    @Test
    void typeReturnsCassandra() {
        assertEquals("cassandra", adapter.type());
    }

    @Test
    void configDefContainsRequiredFields() {
        var configDef = adapter.configDef();
        assertNotNull(configDef.configKeys().get("cassandra.contact.points"));
        assertNotNull(configDef.configKeys().get("cassandra.port"));
        assertNotNull(configDef.configKeys().get("cassandra.datacenter"));
        assertNotNull(configDef.configKeys().get("cassandra.keyspace"));
        assertNotNull(configDef.configKeys().get("cassandra.table"));
        assertNotNull(configDef.configKeys().get("cassandra.username"));
        assertNotNull(configDef.configKeys().get("cassandra.password"));
        assertNotNull(configDef.configKeys().get("cassandra.consistency.level"));
        assertNotNull(configDef.configKeys().get("cassandra.timestamp.column"));
        assertNotNull(configDef.configKeys().get("cassandra.poll.interval.ms"));
        assertNotNull(configDef.configKeys().get("cassandra.batch.size"));
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
