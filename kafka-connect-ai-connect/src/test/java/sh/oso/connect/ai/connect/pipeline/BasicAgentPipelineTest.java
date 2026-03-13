package sh.oso.connect.ai.connect.pipeline;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import sh.oso.connect.ai.api.error.NonRetryableException;
import sh.oso.connect.ai.api.model.RawRecord;
import sh.oso.connect.ai.api.model.SourceOffset;
import sh.oso.connect.ai.api.model.TransformedRecord;
import sh.oso.connect.ai.connect.llm.LlmClient;
import sh.oso.connect.ai.connect.llm.LlmRequest;
import sh.oso.connect.ai.connect.llm.LlmResponse;
import sh.oso.connect.ai.connect.metrics.AiConnectMetrics;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class BasicAgentPipelineTest {

    @Mock
    private LlmClient llmClient;

    private BasicAgentPipeline createPipeline() {
        return new BasicAgentPipeline(
                llmClient,
                new SchemaEnforcer(""),
                "You are a transformer",
                "test-model",
                false,
                0.0
        );
    }

    @Test
    void processEmptyListReturnsEmpty() {
        BasicAgentPipeline pipeline = createPipeline();

        List<TransformedRecord> result = pipeline.process(List.of());

        assertTrue(result.isEmpty());
        verifyNoInteractions(llmClient);
    }

    @Test
    void processCallsLlmAndReturnsTransformedRecords() {
        BasicAgentPipeline pipeline = createPipeline();

        String llmOutput = "[{\"key\":\"k1\",\"value\":\"v1\"}]";
        LlmResponse response = new LlmResponse(llmOutput, 10, 20, "test-model", "end_turn");
        when(llmClient.call(any(LlmRequest.class))).thenReturn(response);

        RawRecord input = new RawRecord(
                "key1".getBytes(StandardCharsets.UTF_8),
                "value1".getBytes(StandardCharsets.UTF_8),
                Map.of(),
                SourceOffset.empty()
        );

        List<TransformedRecord> result = pipeline.process(List.of(input));

        assertEquals(1, result.size());
        assertNotNull(result.get(0).value());
        verify(llmClient, times(1)).call(any(LlmRequest.class));
    }

    @Test
    void processRetriesOnMalformedLlmResponse() {
        BasicAgentPipeline pipeline = createPipeline();

        LlmResponse badResponse = new LlmResponse("not valid json", 10, 20, "test-model", "end_turn");
        String validOutput = "[{\"key\":\"k1\",\"value\":\"v1\"}]";
        LlmResponse goodResponse = new LlmResponse(validOutput, 10, 20, "test-model", "end_turn");

        when(llmClient.call(any(LlmRequest.class)))
                .thenReturn(badResponse)
                .thenReturn(goodResponse);

        RawRecord input = new RawRecord(
                "key1".getBytes(StandardCharsets.UTF_8),
                "value1".getBytes(StandardCharsets.UTF_8),
                Map.of(),
                SourceOffset.empty()
        );

        List<TransformedRecord> result = pipeline.process(List.of(input));

        assertEquals(1, result.size());
        verify(llmClient, times(2)).call(any(LlmRequest.class));
    }

    @Test
    void processThrowsAfterAllRetriesExhausted() {
        BasicAgentPipeline pipeline = createPipeline();

        LlmResponse badResponse = new LlmResponse("not json", 10, 20, "test-model", "end_turn");
        when(llmClient.call(any(LlmRequest.class))).thenReturn(badResponse);

        RawRecord input = new RawRecord(
                "key1".getBytes(StandardCharsets.UTF_8),
                "value1".getBytes(StandardCharsets.UTF_8),
                Map.of(),
                SourceOffset.empty()
        );

        assertThrows(NonRetryableException.class, () -> pipeline.process(List.of(input)));
    }

    @Test
    void processUsesCompiledTransformOnSecondBatch() {
        // Setup: pipeline with compiled transforms enabled, using a declarative mapping
        String targetSchema = """
                {
                  "type": "object",
                  "properties": {
                    "user_name": { "type": "string" },
                    "email_address": { "type": "string" }
                  },
                  "required": ["user_name", "email_address"]
                }
                """;
        SchemaEnforcer enforcer = new SchemaEnforcer(targetSchema);
        BasicAgentPipeline pipeline = new BasicAgentPipeline(
                llmClient, enforcer,
                "Transform user records", "test-model",
                false, 0.0
        );

        AiConnectMetrics metrics = AiConnectMetrics.createForTesting(new SimpleMeterRegistry());
        pipeline.setMetrics(metrics);

        // Enable compiled transforms
        pipeline.setCompiledTransformEnabled(true);
        CompiledTransformCache cache = new CompiledTransformCache(86400);
        pipeline.setCompiledTransformCache(cache);

        // Create a real TransformCompiler backed by the mock LLM client
        // The compiler will ask the LLM to generate mapping code
        GraalJsTransformExecutor jsExecutor = new GraalJsTransformExecutor(100, 10);
        pipeline.setJsExecutor(jsExecutor);
        TransformCompiler compiler = new TransformCompiler(
                llmClient, "test-model", false, 3, jsExecutor, enforcer, targetSchema);
        pipeline.setTransformCompiler(compiler);

        // Declarative mapping spec that the LLM will "generate" for the compiler
        String mappingSpec = """
                {
                  "mappings": {
                    "user_name": "$.userName",
                    "email_address": "$.emailAddress"
                  }
                }""";

        // LLM responses: first call is for data transformation, second is for compilation
        String llmDataOutput = "[{\"user_name\":\"Alice\",\"email_address\":\"alice@example.com\"}]";
        LlmResponse dataResponse = new LlmResponse(llmDataOutput, 10, 20, "test-model", "end_turn");
        LlmResponse compilerResponse = new LlmResponse(mappingSpec, 10, 20, "test-model", "end_turn");
        when(llmClient.call(any(LlmRequest.class)))
                .thenReturn(dataResponse)    // First: data transformation LLM call
                .thenReturn(compilerResponse); // Second: compiler asks LLM for mapping code

        // Input record
        String inputJson = "{\"userName\":\"Alice\",\"emailAddress\":\"alice@example.com\"}";
        RawRecord input = new RawRecord(
                "key1".getBytes(StandardCharsets.UTF_8),
                inputJson.getBytes(StandardCharsets.UTF_8),
                Map.of(),
                SourceOffset.empty()
        );

        // First batch: LLM is called for data transform, then compilation is triggered
        List<TransformedRecord> result1 = pipeline.process(List.of(input));
        assertEquals(1, result1.size());
        // LLM called twice: once for data, once for compilation
        verify(llmClient, times(2)).call(any(LlmRequest.class));
        assertEquals(1, cache.size());

        // Reset LLM mock to track second batch calls
        reset(llmClient);

        // Second batch (same schema): compiled transform should be used, LLM NOT called
        RawRecord input2 = new RawRecord(
                "key2".getBytes(StandardCharsets.UTF_8),
                "{\"userName\":\"Bob\",\"emailAddress\":\"bob@example.com\"}".getBytes(StandardCharsets.UTF_8),
                Map.of(),
                SourceOffset.empty()
        );

        List<TransformedRecord> result2 = pipeline.process(List.of(input2));
        assertEquals(1, result2.size());

        // LLM should NOT have been called for the second batch
        verifyNoInteractions(llmClient);

        // Verify the output came from the compiled transform
        String outputJson = new String(result2.get(0).value(), StandardCharsets.UTF_8);
        assertTrue(outputJson.contains("Bob"));
        assertTrue(outputJson.contains("bob@example.com"));

        // Verify metrics recorded the hit
        assertEquals(1.0, metrics.getCompiledTransformHits());
        assertEquals(1.0, metrics.getCompiledTransformMisses()); // first batch was a miss
        assertEquals(1.0, metrics.getCompiledTransformCompilations());

        // Cleanup
        jsExecutor.close();
    }
}
