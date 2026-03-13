package sh.oso.connect.ai.connect.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.*;

class SchemaFingerprinterTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void sameStructureDifferentValuesProduceSameFingerprint() {
        byte[] record1 = """
                {"userName": "Alice", "age": 30, "active": true}
                """.getBytes(StandardCharsets.UTF_8);
        byte[] record2 = """
                {"userName": "Bob", "age": 25, "active": false}
                """.getBytes(StandardCharsets.UTF_8);

        String fp1 = SchemaFingerprinter.fingerprint(record1, "target");
        String fp2 = SchemaFingerprinter.fingerprint(record2, "target");

        assertEquals(fp1, fp2, "Same structure should produce same fingerprint");
    }

    @Test
    void differentStructureProducesDifferentFingerprint() {
        byte[] record1 = """
                {"userName": "Alice", "age": 30}
                """.getBytes(StandardCharsets.UTF_8);
        byte[] record2 = """
                {"email": "alice@example.com", "score": 9.5}
                """.getBytes(StandardCharsets.UTF_8);

        String fp1 = SchemaFingerprinter.fingerprint(record1, "target");
        String fp2 = SchemaFingerprinter.fingerprint(record2, "target");

        assertNotEquals(fp1, fp2, "Different structure should produce different fingerprint");
    }

    @Test
    void differentTargetSchemaProducesDifferentFingerprint() {
        byte[] record = """
                {"userName": "Alice"}
                """.getBytes(StandardCharsets.UTF_8);

        String fp1 = SchemaFingerprinter.fingerprint(record, "schema-v1");
        String fp2 = SchemaFingerprinter.fingerprint(record, "schema-v2");

        assertNotEquals(fp1, fp2);
    }

    @Test
    void fingerprintIsDeterministic() {
        byte[] record = """
                {"a": 1, "b": "hello"}
                """.getBytes(StandardCharsets.UTF_8);

        String fp1 = SchemaFingerprinter.fingerprint(record, "target");
        String fp2 = SchemaFingerprinter.fingerprint(record, "target");

        assertEquals(fp1, fp2);
    }

    @Test
    void fingerprintHandlesNestedObjects() throws Exception {
        byte[] record = """
                {"user": {"name": "Alice", "address": {"city": "London"}}}
                """.getBytes(StandardCharsets.UTF_8);

        String fp = SchemaFingerprinter.fingerprint(record, "");
        assertNotNull(fp);
        assertEquals(64, fp.length(), "SHA-256 produces 64 hex chars");
    }

    @Test
    void fingerprintHandlesArrays() {
        byte[] record = """
                {"items": [{"id": 1, "name": "Widget"}]}
                """.getBytes(StandardCharsets.UTF_8);

        String fp = SchemaFingerprinter.fingerprint(record, "");
        assertNotNull(fp);
    }

    @Test
    void fingerprintHandlesUnparseableInput() {
        byte[] record = "not json at all".getBytes(StandardCharsets.UTF_8);

        String fp = SchemaFingerprinter.fingerprint(record, "target");
        assertNotNull(fp, "Should fall back to raw content hash");
    }

    @Test
    void extractStructureProducesCorrectTypes() throws Exception {
        JsonNode node = MAPPER.readTree("""
                {"name": "Alice", "age": 30, "score": 9.5, "active": true, "data": null}
                """);
        TreeMap<String, String> structure = new TreeMap<>();
        SchemaFingerprinter.extractStructure(node, "", structure);

        assertEquals("string", structure.get("name"));
        assertEquals("integer", structure.get("age"));
        assertEquals("number", structure.get("score"));
        assertEquals("boolean", structure.get("active"));
        assertEquals("null", structure.get("data"));
    }

    @Test
    void describeStructureProducesReadableOutput() throws Exception {
        JsonNode node = MAPPER.readTree("""
                {"userName": "Alice", "age": 30, "address": {"city": "London", "zip": "SW1"}}
                """);

        String description = SchemaFingerprinter.describeStructure(node);

        assertTrue(description.contains("userName"));
        assertTrue(description.contains("string"));
        assertTrue(description.contains("age"));
        assertTrue(description.contains("integer"));
        assertTrue(description.contains("address"));
        assertTrue(description.contains("object"));
        assertTrue(description.contains("city"));
    }

    @Test
    void fieldOrderDoesNotAffectFingerprint() {
        byte[] record1 = """
                {"b": 1, "a": 2}
                """.getBytes(StandardCharsets.UTF_8);
        byte[] record2 = """
                {"a": 2, "b": 1}
                """.getBytes(StandardCharsets.UTF_8);

        String fp1 = SchemaFingerprinter.fingerprint(record1, "target");
        String fp2 = SchemaFingerprinter.fingerprint(record2, "target");

        assertEquals(fp1, fp2, "Field order should not affect fingerprint (TreeMap sorts keys)");
    }
}
