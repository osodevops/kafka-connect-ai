package sh.oso.connect.ai.adapter.jdbc;

import org.junit.jupiter.api.Test;
import sh.oso.connect.ai.adapter.jdbc.sql.SqlGenerator;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SqlGeneratorTest {

    private final SqlGenerator generator = new SqlGenerator();

    @Test
    void generateInsertProducesCorrectSql() {
        String sql = generator.generateInsert("users", List.of("id", "name", "email"));
        assertEquals("INSERT INTO users (id, name, email) VALUES (?, ?, ?)", sql);
    }

    @Test
    void generateUpsertProducesConflictClause() {
        String sql = generator.generateUpsert("users",
                List.of("id", "name", "email"), List.of("id"));
        assertTrue(sql.contains("INSERT INTO users"));
        assertTrue(sql.contains("ON CONFLICT (id)"));
        assertTrue(sql.contains("DO UPDATE SET name = EXCLUDED.name, email = EXCLUDED.email"));
    }

    @Test
    void generateUpsertWithAllPkColumnsDoesNothing() {
        String sql = generator.generateUpsert("users",
                List.of("id"), List.of("id"));
        assertTrue(sql.contains("ON CONFLICT (id) DO NOTHING"));
    }

    @Test
    void generateUpsertWithCompositePk() {
        String sql = generator.generateUpsert("events",
                List.of("user_id", "event_type", "count"),
                List.of("user_id", "event_type"));
        assertTrue(sql.contains("ON CONFLICT (user_id, event_type)"));
        assertTrue(sql.contains("DO UPDATE SET count = EXCLUDED.count"));
    }

    @Test
    void generateCreateTableProducesValidDdl() {
        String sql = generator.generateCreateTable("users",
                List.of("id", "name", "active"),
                List.of("INTEGER", "TEXT", "BOOLEAN"),
                List.of("id"));
        assertEquals("CREATE TABLE IF NOT EXISTS users (id INTEGER, name TEXT, active BOOLEAN, PRIMARY KEY (id))", sql);
    }

    @Test
    void generateCreateTableWithNoPk() {
        String sql = generator.generateCreateTable("logs",
                List.of("message", "level"),
                List.of("TEXT", "TEXT"),
                List.of());
        assertEquals("CREATE TABLE IF NOT EXISTS logs (message TEXT, level TEXT)", sql);
    }

    @Test
    void generateUpdateProducesCorrectSql() {
        String sql = generator.generateUpdate("users",
                List.of("id", "name", "email"), List.of("id"));
        assertEquals("UPDATE users SET name = ?, email = ? WHERE id = ?", sql);
    }
}
