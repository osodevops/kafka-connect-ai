package sh.oso.nexus.connect.pipeline;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.nexus.api.error.NonRetryableException;
import sh.oso.nexus.api.model.RawRecord;
import sh.oso.nexus.connect.llm.LlmClient;
import sh.oso.nexus.connect.llm.LlmRequest;
import sh.oso.nexus.connect.llm.LlmResponse;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;

public class SchemaDiscoveryAgent {

    private static final Logger log = LoggerFactory.getLogger(SchemaDiscoveryAgent.class);

    private static final String DISCOVERY_PROMPT = """
            Analyze the following sample records and infer a JSON Schema that describes the structure of the output data.

            Requirements:
            1. Return a valid JSON Schema (draft-07 or later)
            2. Include "type": "object" at the root
            3. Include a "properties" object with field definitions
            4. Include a "required" array listing all fields that appear in every sample
            5. Infer appropriate types (string, number, integer, boolean, array, object)
            6. If a field has a small set of distinct values, consider using "enum"

            Respond with ONLY the JSON Schema. No additional text or explanation.

            Sample records:
            """;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final LlmClient llmClient;
    private final String model;
    private final int maxTokens;

    public SchemaDiscoveryAgent(LlmClient llmClient, String model, int maxTokens) {
        this.llmClient = llmClient;
        this.model = model;
        this.maxTokens = maxTokens;
    }

    public DiscoveredSchema discoverSchema(List<RawRecord> sample, String systemPrompt) {
        if (sample.isEmpty()) {
            throw new NonRetryableException("Cannot discover schema from empty sample");
        }

        String sampleJson = serializeSample(sample);
        String effectiveSystemPrompt = systemPrompt != null && !systemPrompt.isEmpty()
                ? systemPrompt + "\n\n" + DISCOVERY_PROMPT
                : DISCOVERY_PROMPT;

        LlmRequest request = LlmRequest.builder()
                .systemPrompt(effectiveSystemPrompt)
                .userPrompt(sampleJson)
                .model(model)
                .maxTokens(maxTokens)
                .temperature(0.0)
                .build();

        LlmResponse response = llmClient.call(request);
        String content = response.content().trim();

        // Strip markdown code block if present
        if (content.startsWith("```")) {
            int firstNewline = content.indexOf('\n');
            int lastFence = content.lastIndexOf("```");
            if (firstNewline > 0 && lastFence > firstNewline) {
                content = content.substring(firstNewline + 1, lastFence).trim();
            }
        }

        if (!isValidJsonSchema(content)) {
            throw new NonRetryableException("LLM did not return a valid JSON Schema: " + content);
        }

        log.info("Discovered schema from {} sample records: {}", sample.size(), content);
        return new DiscoveredSchema(content, sample.size(), Instant.now());
    }

    private String serializeSample(List<RawRecord> sample) {
        try {
            var array = objectMapper.createArrayNode();
            for (RawRecord record : sample) {
                if (record.value() != null) {
                    String valueStr = new String(record.value(), StandardCharsets.UTF_8);
                    try {
                        JsonNode node = objectMapper.readTree(valueStr);
                        array.add(node);
                    } catch (JsonProcessingException e) {
                        array.add(valueStr);
                    }
                }
            }
            return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(array);
        } catch (JsonProcessingException e) {
            throw new NonRetryableException("Failed to serialize sample records", e);
        }
    }

    private boolean isValidJsonSchema(String json) {
        try {
            JsonNode node = objectMapper.readTree(json);
            return node.isObject() && node.has("type") && node.has("properties");
        } catch (Exception e) {
            return false;
        }
    }
}
