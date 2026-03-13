package sh.oso.connect.ai.connect.pipeline;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import sh.oso.connect.ai.api.model.RawRecord;
import sh.oso.connect.ai.api.model.SourceOffset;
import sh.oso.connect.ai.connect.llm.LlmClient;
import sh.oso.connect.ai.connect.llm.LlmRequest;
import sh.oso.connect.ai.connect.llm.LlmResponse;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class TransformCompilerTest {

    @Mock
    private LlmClient llmClient;

    private RawRecord sampleRecord(String json) {
        return new RawRecord(
                "key".getBytes(StandardCharsets.UTF_8),
                json.getBytes(StandardCharsets.UTF_8),
                Map.of(),
                SourceOffset.empty()
        );
    }

    @Test
    void compilesDeclarativeMappingSuccessfully() {
        String mappingSpec = """
                {"mappings": {"user_name": "$.userName", "email": "$.emailAddress"}}
                """;

        when(llmClient.call(any(LlmRequest.class)))
                .thenReturn(new LlmResponse(mappingSpec, 100, 50, "claude-sonnet", "end_turn"));

        SchemaEnforcer enforcer = new SchemaEnforcer("");
        TransformCompiler compiler = new TransformCompiler(
                llmClient, "claude-sonnet", true, 3, null, enforcer, "");

        List<RawRecord> samples = List.of(
                sampleRecord("""
                        {"userName": "Alice", "emailAddress": "alice@example.com"}
                        """)
        );

        Optional<CompiledTransform> result = compiler.compile(samples, "fp123");

        assertTrue(result.isPresent());
        assertEquals(CompiledTransform.Type.DECLARATIVE, result.get().type());
        verify(llmClient, times(1)).call(any());
    }

    @Test
    void fallsBackToJavaScriptWhenDeclarativeFails() {
        // First call (declarative) returns invalid spec
        // Second call (JS) returns valid function body
        when(llmClient.call(any(LlmRequest.class)))
                .thenReturn(new LlmResponse("NEEDS_JAVASCRIPT", 100, 10, "claude-sonnet", "end_turn"))
                .thenReturn(new LlmResponse(
                        "var result = {}; result.name = input.userName || ''; return result;",
                        100, 50, "claude-sonnet", "end_turn"));

        SchemaEnforcer enforcer = new SchemaEnforcer("");
        GraalJsTransformExecutor jsExecutor = new GraalJsTransformExecutor(1000, 10);
        TransformCompiler compiler = new TransformCompiler(
                llmClient, "claude-sonnet", true, 1, jsExecutor, enforcer, "");

        List<RawRecord> samples = List.of(
                sampleRecord("""
                        {"userName": "Alice"}
                        """)
        );

        Optional<CompiledTransform> result = compiler.compile(samples, "fp123");

        assertTrue(result.isPresent());
        assertEquals(CompiledTransform.Type.JAVASCRIPT, result.get().type());
        verify(llmClient, times(2)).call(any());

        jsExecutor.close();
    }

    @Test
    void extractsCodeFromMarkdownBlocks() {
        assertEquals("{\"mappings\": {}}",
                TransformCompiler.extractCodeBlock("```json\n{\"mappings\": {}}\n```"));
        assertEquals("return {};",
                TransformCompiler.extractCodeBlock("```javascript\nreturn {};\n```"));
        assertEquals("raw code",
                TransformCompiler.extractCodeBlock("raw code"));
    }

    @Test
    void returnsEmptyForEmptyRecordList() {
        TransformCompiler compiler = new TransformCompiler(
                llmClient, "model", true, 3, null, new SchemaEnforcer(""), "");

        Optional<CompiledTransform> result = compiler.compile(List.of(), "fp");

        assertTrue(result.isEmpty());
        verifyNoInteractions(llmClient);
    }

    @Test
    void returnsEmptyForNullValueRecord() {
        TransformCompiler compiler = new TransformCompiler(
                llmClient, "model", true, 3, null, new SchemaEnforcer(""), "");

        List<RawRecord> samples = List.of(
                new RawRecord("key".getBytes(), null, Map.of(), SourceOffset.empty())
        );

        Optional<CompiledTransform> result = compiler.compile(samples, "fp");

        assertTrue(result.isEmpty());
        verifyNoInteractions(llmClient);
    }

    @Test
    void validatesDeclarativeOutputAgainstTargetSchema() {
        String targetSchema = """
                {"type":"object","properties":{"name":{"type":"string"},"age":{"type":"integer"}},"required":["name","age"]}
                """;

        // Mapping that produces correct output
        String goodMapping = """
                {"mappings": {"name": "$.userName", "age": "$.userAge"}}
                """;

        when(llmClient.call(any(LlmRequest.class)))
                .thenReturn(new LlmResponse(goodMapping, 100, 50, "claude-sonnet", "end_turn"));

        SchemaEnforcer enforcer = new SchemaEnforcer(targetSchema);
        TransformCompiler compiler = new TransformCompiler(
                llmClient, "claude-sonnet", true, 1, null, enforcer, targetSchema);

        List<RawRecord> samples = List.of(
                sampleRecord("""
                        {"userName": "Alice", "userAge": 30}
                        """)
        );

        Optional<CompiledTransform> result = compiler.compile(samples, "fp-valid");

        assertTrue(result.isPresent());
    }
}
