package sh.oso.connect.ai.connect.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DeclarativeMappingExecutorTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void simpleFieldMapping() throws Exception {
        JsonNode input = MAPPER.readTree("""
                {"userName": "Alice", "emailAddress": "alice@example.com", "age": 30}
                """);
        JsonNode mappingSpec = MAPPER.readTree("""
                {
                  "mappings": {
                    "user_name": "$.userName",
                    "email": "$.emailAddress",
                    "age": "$.age"
                  }
                }
                """);

        JsonNode result = DeclarativeMappingExecutor.execute(input, mappingSpec);

        assertEquals("Alice", result.get("user_name").asText());
        assertEquals("alice@example.com", result.get("email").asText());
        assertEquals(30, result.get("age").asInt());
    }

    @Test
    void nestedFieldMapping() throws Exception {
        JsonNode input = MAPPER.readTree("""
                {"user": {"name": "Alice", "address": {"city": "London", "zip": "SW1"}}}
                """);
        JsonNode mappingSpec = MAPPER.readTree("""
                {
                  "mappings": {
                    "name": "$.user.name",
                    "city": "$.user.address.city"
                  }
                }
                """);

        JsonNode result = DeclarativeMappingExecutor.execute(input, mappingSpec);

        assertEquals("Alice", result.get("name").asText());
        assertEquals("London", result.get("city").asText());
    }

    @Test
    void arrayIndexMapping() throws Exception {
        JsonNode input = MAPPER.readTree("""
                {"items": [{"id": 1, "name": "Widget"}, {"id": 2, "name": "Gadget"}]}
                """);
        JsonNode mappingSpec = MAPPER.readTree("""
                {
                  "mappings": {
                    "first_item": "$.items[0].name",
                    "second_item": "$.items[1].name"
                  }
                }
                """);

        JsonNode result = DeclarativeMappingExecutor.execute(input, mappingSpec);

        assertEquals("Widget", result.get("first_item").asText());
        assertEquals("Gadget", result.get("second_item").asText());
    }

    @Test
    void defaultValues() throws Exception {
        JsonNode input = MAPPER.readTree("""
                {"name": "Alice"}
                """);
        JsonNode mappingSpec = MAPPER.readTree("""
                {
                  "mappings": {
                    "name": "$.name"
                  },
                  "defaults": {
                    "version": 1,
                    "source": "kafka-connect-ai"
                  }
                }
                """);

        JsonNode result = DeclarativeMappingExecutor.execute(input, mappingSpec);

        assertEquals("Alice", result.get("name").asText());
        assertEquals(1, result.get("version").asInt());
        assertEquals("kafka-connect-ai", result.get("source").asText());
    }

    @Test
    void defaultsDoNotOverrideExistingFields() throws Exception {
        JsonNode input = MAPPER.readTree("""
                {"name": "Alice"}
                """);
        JsonNode mappingSpec = MAPPER.readTree("""
                {
                  "mappings": {
                    "name": "$.name"
                  },
                  "defaults": {
                    "name": "DEFAULT"
                  }
                }
                """);

        JsonNode result = DeclarativeMappingExecutor.execute(input, mappingSpec);

        assertEquals("Alice", result.get("name").asText(), "Mapping should win over default");
    }

    @Test
    void typeCasts() throws Exception {
        JsonNode input = MAPPER.readTree("""
                {"age": "30", "score": "9.5", "active": "true", "count": 42}
                """);
        JsonNode mappingSpec = MAPPER.readTree("""
                {
                  "mappings": {
                    "age": "$.age",
                    "score": "$.score",
                    "active": "$.active",
                    "count": "$.count"
                  },
                  "typeCasts": {
                    "age": "integer",
                    "score": "number",
                    "active": "boolean",
                    "count": "string"
                  }
                }
                """);

        JsonNode result = DeclarativeMappingExecutor.execute(input, mappingSpec);

        assertTrue(result.get("age").isIntegralNumber());
        assertEquals(30, result.get("age").asInt());
        assertTrue(result.get("score").isFloatingPointNumber());
        assertEquals(9.5, result.get("score").asDouble());
        assertTrue(result.get("active").isBoolean());
        assertTrue(result.get("active").asBoolean());
        assertTrue(result.get("count").isTextual());
        assertEquals("42", result.get("count").asText());
    }

    @Test
    void missingSourceFieldProducesNoOutputField() throws Exception {
        JsonNode input = MAPPER.readTree("""
                {"name": "Alice"}
                """);
        JsonNode mappingSpec = MAPPER.readTree("""
                {
                  "mappings": {
                    "name": "$.name",
                    "missing": "$.nonexistent.path"
                  }
                }
                """);

        JsonNode result = DeclarativeMappingExecutor.execute(input, mappingSpec);

        assertEquals("Alice", result.get("name").asText());
        assertFalse(result.has("missing"), "Missing source should not produce output field");
    }

    @Test
    void isValidMappingSpec() {
        assertTrue(DeclarativeMappingExecutor.isValidMappingSpec("""
                {"mappings": {"a": "$.b"}}
                """));
        assertFalse(DeclarativeMappingExecutor.isValidMappingSpec("""
                {"noMappingsKey": true}
                """));
        assertFalse(DeclarativeMappingExecutor.isValidMappingSpec("not json"));
    }

    @Test
    void resolvePathWithDollarPrefix() throws Exception {
        JsonNode input = MAPPER.readTree("""
                {"user": {"name": "Alice"}}
                """);

        JsonNode result = DeclarativeMappingExecutor.resolvePath(input, "$.user.name");
        assertEquals("Alice", result.asText());
    }

    @Test
    void resolvePathWithoutDollarPrefix() throws Exception {
        JsonNode input = MAPPER.readTree("""
                {"user": {"name": "Alice"}}
                """);

        JsonNode result = DeclarativeMappingExecutor.resolvePath(input, "user.name");
        assertEquals("Alice", result.asText());
    }
}
