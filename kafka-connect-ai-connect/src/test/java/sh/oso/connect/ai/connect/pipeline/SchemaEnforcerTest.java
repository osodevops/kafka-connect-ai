package sh.oso.connect.ai.connect.pipeline;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SchemaEnforcerTest {

    @Test
    void validateWithNoSchemaAcceptsValidJson() {
        SchemaEnforcer enforcer = new SchemaEnforcer("");

        assertTrue(enforcer.validate("{\"name\":\"alice\"}"));
        assertTrue(enforcer.validate("[1,2,3]"));
        assertTrue(enforcer.validate("\"hello\""));
    }

    @Test
    void validateWithNoSchemaRejectsInvalidJson() {
        SchemaEnforcer enforcer = new SchemaEnforcer(null);

        assertFalse(enforcer.validate("not json at all"));
        assertFalse(enforcer.validate("{broken"));
    }

    @Test
    void validateWithSchemaChecksRequiredFields() {
        String schema = """
                {
                  "type": "object",
                  "required": ["id", "name"],
                  "properties": {
                    "id": { "type": "integer" },
                    "name": { "type": "string" }
                  }
                }
                """;
        SchemaEnforcer enforcer = new SchemaEnforcer(schema);

        assertTrue(enforcer.validate("{\"id\":1,\"name\":\"alice\"}"));
        assertFalse(enforcer.validate("{\"id\":1}"));
        assertFalse(enforcer.validate("{\"name\":\"alice\"}"));
    }

    @Test
    void validateRejectsNonObjectWhenSchemaPresent() {
        String schema = """
                {
                  "type": "object",
                  "properties": {
                    "id": { "type": "integer" }
                  }
                }
                """;
        SchemaEnforcer enforcer = new SchemaEnforcer(schema);

        assertFalse(enforcer.validate("[1,2,3]"));
        assertFalse(enforcer.validate("\"just a string\""));
    }

    @Test
    void hasSchemaReturnsTrueWhenSchemaProvided() {
        SchemaEnforcer withSchema = new SchemaEnforcer("{\"type\":\"object\"}");
        assertTrue(withSchema.hasSchema());
    }

    @Test
    void hasSchemaReturnsFalseWhenNoSchema() {
        SchemaEnforcer empty = new SchemaEnforcer("");
        assertFalse(empty.hasSchema());

        SchemaEnforcer nullSchema = new SchemaEnforcer(null);
        assertFalse(nullSchema.hasSchema());
    }

    @Test
    void validateChecksFieldTypes() {
        String schema = """
                {
                  "type": "object",
                  "required": ["id", "name", "active"],
                  "properties": {
                    "id": { "type": "integer" },
                    "name": { "type": "string" },
                    "active": { "type": "boolean" }
                  }
                }
                """;
        SchemaEnforcer enforcer = new SchemaEnforcer(schema);

        // Correct types
        assertTrue(enforcer.validate("{\"id\":1,\"name\":\"alice\",\"active\":true}"));

        // Wrong type: id should be integer, not string
        assertFalse(enforcer.validate("{\"id\":\"not_a_number\",\"name\":\"alice\",\"active\":true}"));

        // Wrong type: active should be boolean, not string
        assertFalse(enforcer.validate("{\"id\":1,\"name\":\"alice\",\"active\":\"yes\"}"));
    }

    @Test
    void validateChecksEnumConstraints() {
        String schema = """
                {
                  "type": "object",
                  "required": ["status"],
                  "properties": {
                    "status": { "type": "string", "enum": ["active", "inactive", "pending"] }
                  }
                }
                """;
        SchemaEnforcer enforcer = new SchemaEnforcer(schema);

        assertTrue(enforcer.validate("{\"status\":\"active\"}"));
        assertTrue(enforcer.validate("{\"status\":\"pending\"}"));
        assertFalse(enforcer.validate("{\"status\":\"deleted\"}"));
    }

    @Test
    void validateWithErrorsReturnsDetailedMessages() {
        String schema = """
                {
                  "type": "object",
                  "required": ["id", "name"],
                  "properties": {
                    "id": { "type": "integer" },
                    "name": { "type": "string" }
                  }
                }
                """;
        SchemaEnforcer enforcer = new SchemaEnforcer(schema);

        var errors = enforcer.validateWithErrors("{\"id\":\"oops\"}");
        assertFalse(errors.isEmpty());
        assertTrue(errors.stream().anyMatch(e -> e.contains("Missing required field: name")));
        assertTrue(errors.stream().anyMatch(e -> e.contains("expected integer")));
    }

    @Test
    void validateAcceptsNumberType() {
        String schema = """
                {
                  "type": "object",
                  "properties": {
                    "price": { "type": "number" }
                  },
                  "required": ["price"]
                }
                """;
        SchemaEnforcer enforcer = new SchemaEnforcer(schema);

        assertTrue(enforcer.validate("{\"price\":19.99}"));
        assertTrue(enforcer.validate("{\"price\":42}"));
        assertFalse(enforcer.validate("{\"price\":\"free\"}"));
    }

    @Test
    void validateAcceptsArrayType() {
        String schema = """
                {
                  "type": "object",
                  "properties": {
                    "tags": { "type": "array" }
                  },
                  "required": ["tags"]
                }
                """;
        SchemaEnforcer enforcer = new SchemaEnforcer(schema);

        assertTrue(enforcer.validate("{\"tags\":[\"a\",\"b\"]}"));
        assertFalse(enforcer.validate("{\"tags\":\"not_an_array\"}"));
    }
}
