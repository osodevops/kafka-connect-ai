package sh.oso.connect.ai.adapter.jdbc;

import org.junit.jupiter.api.Test;
import sh.oso.connect.ai.adapter.jdbc.query.QueryBuilder;
import sh.oso.connect.ai.adapter.jdbc.query.QueryMode;
import sh.oso.connect.ai.api.model.SourceOffset;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class QueryBuilderTest {

    @Test
    void bulkModeGeneratesSimpleSelect() {
        QueryBuilder builder = new QueryBuilder("users", QueryMode.BULK, null, null);
        String sql = builder.buildQuery(SourceOffset.empty(), 100);
        assertEquals("SELECT * FROM users LIMIT 100", sql);
    }

    @Test
    void bulkModeWithNoLimitOmitsLimit() {
        QueryBuilder builder = new QueryBuilder("users", QueryMode.BULK, null, null);
        String sql = builder.buildQuery(SourceOffset.empty(), 0);
        assertEquals("SELECT * FROM users", sql);
    }

    @Test
    void timestampModeWithNoOffset() {
        QueryBuilder builder = new QueryBuilder("events", QueryMode.TIMESTAMP, "updated_at", null);
        String sql = builder.buildQuery(SourceOffset.empty(), 50);
        assertEquals("SELECT * FROM events ORDER BY updated_at ASC LIMIT 50", sql);
    }

    @Test
    void timestampModeWithOffset() {
        SourceOffset offset = new SourceOffset(Map.of(), Map.of("timestamp", "2024-01-01T00:00:00"));
        QueryBuilder builder = new QueryBuilder("events", QueryMode.TIMESTAMP, "updated_at", null);
        String sql = builder.buildQuery(offset, 50);
        assertEquals("SELECT * FROM events WHERE updated_at > '2024-01-01T00:00:00' ORDER BY updated_at ASC LIMIT 50", sql);
    }

    @Test
    void incrementingModeWithNoOffset() {
        QueryBuilder builder = new QueryBuilder("orders", QueryMode.INCREMENTING, null, "id");
        String sql = builder.buildQuery(SourceOffset.empty(), 100);
        assertEquals("SELECT * FROM orders ORDER BY id ASC LIMIT 100", sql);
    }

    @Test
    void incrementingModeWithOffset() {
        SourceOffset offset = new SourceOffset(Map.of(), Map.of("incrementing", 42));
        QueryBuilder builder = new QueryBuilder("orders", QueryMode.INCREMENTING, null, "id");
        String sql = builder.buildQuery(offset, 100);
        assertEquals("SELECT * FROM orders WHERE id > 42 ORDER BY id ASC LIMIT 100", sql);
    }

    @Test
    void timestampIncrementingModeWithBothOffsets() {
        SourceOffset offset = new SourceOffset(Map.of(),
                Map.of("timestamp", "2024-01-01T00:00:00", "incrementing", 42));
        QueryBuilder builder = new QueryBuilder("data", QueryMode.TIMESTAMP_INCREMENTING, "modified", "seq");
        String sql = builder.buildQuery(offset, 100);
        assertTrue(sql.contains("modified > '2024-01-01T00:00:00'"));
        assertTrue(sql.contains("seq > 42"));
        assertTrue(sql.contains("ORDER BY modified ASC, seq ASC"));
    }

    @Test
    void timestampIncrementingModeWithOnlyTimestamp() {
        SourceOffset offset = new SourceOffset(Map.of(), Map.of("timestamp", "2024-06-15"));
        QueryBuilder builder = new QueryBuilder("data", QueryMode.TIMESTAMP_INCREMENTING, "modified", "seq");
        String sql = builder.buildQuery(offset, 100);
        assertTrue(sql.contains("modified > '2024-06-15'"));
        assertFalse(sql.contains("seq >"));
    }
}
