package sh.oso.nexus.adapter.jdbc;

import org.junit.jupiter.api.Test;
import sh.oso.nexus.adapter.jdbc.query.QueryMode;

import static org.junit.jupiter.api.Assertions.*;

class JdbcSourceAdapterTest {

    @Test
    void typeReturnsJdbc() {
        JdbcSourceAdapter adapter = new JdbcSourceAdapter();
        assertEquals("jdbc", adapter.type());
    }

    @Test
    void configDefContainsJdbcUrl() {
        JdbcSourceAdapter adapter = new JdbcSourceAdapter();
        assertNotNull(adapter.configDef().configKeys().get("jdbc.url"));
    }

    @Test
    void configDefContainsTable() {
        JdbcSourceAdapter adapter = new JdbcSourceAdapter();
        assertNotNull(adapter.configDef().configKeys().get("jdbc.table"));
    }

    @Test
    void configDefContainsQueryMode() {
        JdbcSourceAdapter adapter = new JdbcSourceAdapter();
        assertNotNull(adapter.configDef().configKeys().get("jdbc.query.mode"));
    }

    @Test
    void isHealthyReturnsFalseBeforeStart() {
        JdbcSourceAdapter adapter = new JdbcSourceAdapter();
        assertFalse(adapter.isHealthy());
    }

    @Test
    void queryModeFromStringParsesAllModes() {
        assertEquals(QueryMode.BULK, QueryMode.fromString("bulk"));
        assertEquals(QueryMode.TIMESTAMP, QueryMode.fromString("timestamp"));
        assertEquals(QueryMode.INCREMENTING, QueryMode.fromString("incrementing"));
        assertEquals(QueryMode.TIMESTAMP_INCREMENTING, QueryMode.fromString("timestamp+incrementing"));
        assertEquals(QueryMode.TIMESTAMP_INCREMENTING, QueryMode.fromString("timestamp_incrementing"));
    }

    @Test
    void queryModeFromStringThrowsOnUnknown() {
        assertThrows(IllegalArgumentException.class, () -> QueryMode.fromString("invalid"));
    }
}
