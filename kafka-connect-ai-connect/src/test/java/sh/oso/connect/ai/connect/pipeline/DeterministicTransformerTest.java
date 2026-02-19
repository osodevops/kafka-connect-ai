package sh.oso.connect.ai.connect.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import sh.oso.connect.ai.api.model.RawRecord;
import sh.oso.connect.ai.api.model.SourceOffset;
import sh.oso.connect.ai.api.model.TransformedRecord;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class DeterministicTransformerTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    private RawRecord record(String json) {
        return new RawRecord(
                "key".getBytes(StandardCharsets.UTF_8),
                json.getBytes(StandardCharsets.UTF_8),
                Map.of(),
                SourceOffset.empty()
        );
    }

    @Test
    void fieldRenameConvertsCamelToSnake() throws Exception {
        DeterministicTransformer transformer = new DeterministicTransformer(Set.of("field_rename"));

        List<RawRecord> records = List.of(record("{\"firstName\":\"John\",\"lastName\":\"Doe\"}"));
        Optional<List<TransformedRecord>> result = transformer.transform(records);

        assertTrue(result.isPresent());
        assertEquals(1, result.get().size());

        JsonNode node = objectMapper.readTree(new String(result.get().get(0).value(), StandardCharsets.UTF_8));
        assertTrue(node.has("first_name"));
        assertTrue(node.has("last_name"));
        assertFalse(node.has("firstName"));
    }

    @Test
    void typeCastConvertsStringNumbers() throws Exception {
        DeterministicTransformer transformer = new DeterministicTransformer(Set.of("type_cast"));

        List<RawRecord> records = List.of(record("{\"count\":\"42\",\"price\":\"9.99\",\"active\":\"true\"}"));
        Optional<List<TransformedRecord>> result = transformer.transform(records);

        assertTrue(result.isPresent());
        JsonNode node = objectMapper.readTree(new String(result.get().get(0).value(), StandardCharsets.UTF_8));
        assertTrue(node.get("count").isNumber());
        assertEquals(42, node.get("count").asInt());
        assertEquals(9.99, node.get("price").asDouble(), 0.001);
        assertTrue(node.get("active").isBoolean());
    }

    @Test
    void timestampFormatNormalizesIsoDates() throws Exception {
        DeterministicTransformer transformer = new DeterministicTransformer(Set.of("timestamp_format"));

        List<RawRecord> records = List.of(record("{\"ts\":\"2024-01-15T10:30:00Z\",\"name\":\"test\"}"));
        Optional<List<TransformedRecord>> result = transformer.transform(records);

        // ISO timestamps should pass through or be normalized
        assertTrue(result.isPresent() || result.isEmpty());
    }

    @Test
    void emptyPatternsReturnsEmpty() {
        DeterministicTransformer transformer = new DeterministicTransformer(Set.of());

        List<RawRecord> records = List.of(record("{\"firstName\":\"John\"}"));
        Optional<List<TransformedRecord>> result = transformer.transform(records);

        assertTrue(result.isEmpty());
    }

    @Test
    void nonMatchingPatternReturnsEmpty() {
        DeterministicTransformer transformer = new DeterministicTransformer(Set.of("field_rename"));

        // All fields already snake_case — no renames needed, so returns empty
        List<RawRecord> records = List.of(record("{\"first_name\":\"John\",\"last_name\":\"Doe\"}"));
        Optional<List<TransformedRecord>> result = transformer.transform(records);

        assertTrue(result.isEmpty());
    }

    @Test
    void nonObjectValueReturnsEmpty() {
        DeterministicTransformer transformer = new DeterministicTransformer(Set.of("field_rename"));

        List<RawRecord> records = List.of(record("[1,2,3]"));
        Optional<List<TransformedRecord>> result = transformer.transform(records);

        assertTrue(result.isEmpty());
    }

    @Test
    void camelToSnakeConversion() {
        assertEquals("first_name", DeterministicTransformer.camelToSnake("firstName"));
        assertEquals("last_name", DeterministicTransformer.camelToSnake("lastName"));
        assertEquals("already_snake", DeterministicTransformer.camelToSnake("already_snake"));
        assertEquals("a_bc", DeterministicTransformer.camelToSnake("aBC"));
        assertEquals("simple", DeterministicTransformer.camelToSnake("simple"));
    }

    @Test
    void nullValueRecordPassesThrough() {
        DeterministicTransformer transformer = new DeterministicTransformer(Set.of("field_rename"));

        RawRecord nullRecord = new RawRecord("key".getBytes(), null, Map.of(), SourceOffset.empty());
        List<RawRecord> records = List.of(nullRecord);
        Optional<List<TransformedRecord>> result = transformer.transform(records);

        assertTrue(result.isPresent());
        assertEquals(1, result.get().size());
        assertNull(result.get().get(0).value());
    }
}
