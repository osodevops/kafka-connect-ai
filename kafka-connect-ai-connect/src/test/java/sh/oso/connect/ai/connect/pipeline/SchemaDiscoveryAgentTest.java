package sh.oso.connect.ai.connect.pipeline;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import sh.oso.connect.ai.api.error.NonRetryableException;
import sh.oso.connect.ai.api.model.RawRecord;
import sh.oso.connect.ai.api.model.SourceOffset;
import sh.oso.connect.ai.connect.llm.LlmClient;
import sh.oso.connect.ai.connect.llm.LlmResponse;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class SchemaDiscoveryAgentTest {

    @Test
    void discoversSchemaFromSampleRecords() {
        LlmClient mockClient = mock(LlmClient.class);
        String schemaJson = """
                {
                  "type": "object",
                  "properties": {
                    "id": {"type": "integer"},
                    "name": {"type": "string"},
                    "email": {"type": "string"}
                  },
                  "required": ["id", "name", "email"]
                }
                """;
        when(mockClient.call(any())).thenReturn(
                new LlmResponse(schemaJson, 100, 50, "test-model", "end_turn"));

        SchemaDiscoveryAgent agent = new SchemaDiscoveryAgent(mockClient, "test-model", 4096);

        List<RawRecord> sample = List.of(
                new RawRecord(null,
                        "{\"id\": 1, \"name\": \"Alice\", \"email\": \"alice@test.com\"}".getBytes(StandardCharsets.UTF_8),
                        null, SourceOffset.empty())
        );

        DiscoveredSchema result = agent.discoverSchema(sample, "");
        assertNotNull(result);
        assertTrue(result.jsonSchema().contains("\"type\""));
        assertTrue(result.jsonSchema().contains("\"properties\""));
        assertEquals(1, result.sampleSize());
        assertNotNull(result.discoveredAt());
    }

    @Test
    void stripsMarkdownCodeBlockFromResponse() {
        LlmClient mockClient = mock(LlmClient.class);
        String wrappedSchema = """
                ```json
                {
                  "type": "object",
                  "properties": {
                    "id": {"type": "integer"}
                  },
                  "required": ["id"]
                }
                ```
                """;
        when(mockClient.call(any())).thenReturn(
                new LlmResponse(wrappedSchema, 100, 50, "test-model", "end_turn"));

        SchemaDiscoveryAgent agent = new SchemaDiscoveryAgent(mockClient, "test-model", 4096);
        List<RawRecord> sample = List.of(
                new RawRecord(null, "{\"id\": 1}".getBytes(StandardCharsets.UTF_8), null, null));

        DiscoveredSchema result = agent.discoverSchema(sample, "");
        assertNotNull(result);
        assertTrue(result.jsonSchema().contains("\"type\""));
    }

    @Test
    void throwsOnEmptySample() {
        LlmClient mockClient = mock(LlmClient.class);
        SchemaDiscoveryAgent agent = new SchemaDiscoveryAgent(mockClient, "test-model", 4096);
        assertThrows(NonRetryableException.class, () -> agent.discoverSchema(List.of(), ""));
    }

    @Test
    void throwsOnInvalidSchemaResponse() {
        LlmClient mockClient = mock(LlmClient.class);
        when(mockClient.call(any())).thenReturn(
                new LlmResponse("not a json schema", 100, 50, "test-model", "end_turn"));

        SchemaDiscoveryAgent agent = new SchemaDiscoveryAgent(mockClient, "test-model", 4096);
        List<RawRecord> sample = List.of(
                new RawRecord(null, "{\"id\": 1}".getBytes(StandardCharsets.UTF_8), null, null));

        assertThrows(NonRetryableException.class, () -> agent.discoverSchema(sample, ""));
    }
}
