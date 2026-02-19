package sh.oso.nexus.connect.pipeline;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import sh.oso.nexus.api.error.NonRetryableException;
import sh.oso.nexus.api.model.RawRecord;
import sh.oso.nexus.api.model.SourceOffset;
import sh.oso.nexus.api.model.TransformedRecord;
import sh.oso.nexus.connect.llm.LlmClient;
import sh.oso.nexus.connect.llm.LlmRequest;
import sh.oso.nexus.connect.llm.LlmResponse;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

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
}
