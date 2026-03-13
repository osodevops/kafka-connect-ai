package sh.oso.connect.ai.connect.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.connect.ai.api.model.RawRecord;
import sh.oso.connect.ai.connect.llm.LlmClient;
import sh.oso.connect.ai.connect.llm.LlmRequest;
import sh.oso.connect.ai.connect.llm.LlmResponse;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

/**
 * Asks the LLM to generate transformation CODE (not transform data).
 *
 * Two-pass strategy:
 *   Pass 1: Try to generate a declarative mapping spec (Tier 0) — cheapest, safest, fastest execution
 *   Pass 2: If declarative fails, generate JavaScript function body (Tier 1) — handles complex logic
 *
 * The compiler validates generated code against sample records before returning.
 */
public class TransformCompiler {

    private static final Logger log = LoggerFactory.getLogger(TransformCompiler.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final LlmClient llmClient;
    private final String model;
    private final boolean enablePromptCaching;
    private final int validationSamples;
    private final GraalJsTransformExecutor jsExecutor;
    private final SchemaEnforcer schemaEnforcer;
    private final String targetSchema;

    public TransformCompiler(LlmClient llmClient, String model, boolean enablePromptCaching,
                             int validationSamples, GraalJsTransformExecutor jsExecutor,
                             SchemaEnforcer schemaEnforcer, String targetSchema) {
        this.llmClient = llmClient;
        this.model = model;
        this.enablePromptCaching = enablePromptCaching;
        this.validationSamples = validationSamples;
        this.jsExecutor = jsExecutor;
        this.schemaEnforcer = schemaEnforcer;
        this.targetSchema = targetSchema;
    }

    /**
     * Compile a transformation for the given schema pair.
     * Tries declarative mapping first, falls back to JavaScript.
     *
     * @param sampleRecords representative records to analyze and validate against
     * @param fingerprint   the schema fingerprint for cache keying
     * @return compiled transform, or empty if compilation fails entirely
     */
    public Optional<CompiledTransform> compile(List<RawRecord> sampleRecords, String fingerprint) {
        if (sampleRecords.isEmpty()) {
            return Optional.empty();
        }

        JsonNode sampleNode = parseSample(sampleRecords.get(0));
        if (sampleNode == null) {
            return Optional.empty();
        }

        String sourceDescription = SchemaFingerprinter.describeStructure(sampleNode);
        String sampleJson;
        try {
            sampleJson = OBJECT_MAPPER.writeValueAsString(sampleNode);
        } catch (Exception e) {
            return Optional.empty();
        }

        // Pass 1: Try declarative mapping
        Optional<CompiledTransform> declarative = tryDeclarativeCompile(
                sourceDescription, sampleJson, sampleRecords, fingerprint);
        if (declarative.isPresent()) {
            return declarative;
        }

        // Pass 2: Try JavaScript
        return tryJavaScriptCompile(sourceDescription, sampleJson, sampleRecords, fingerprint);
    }

    private Optional<CompiledTransform> tryDeclarativeCompile(
            String sourceDescription, String sampleJson,
            List<RawRecord> sampleRecords, String fingerprint) {

        String prompt = buildDeclarativePrompt(sourceDescription, sampleJson);

        try {
            LlmResponse response = callLlm(prompt);
            String content = extractCodeBlock(response.content().trim());

            if (!DeclarativeMappingExecutor.isValidMappingSpec(content)) {
                log.debug("LLM did not produce a valid declarative mapping spec");
                return Optional.empty();
            }

            // Validate against sample records
            JsonNode mappingSpec = DeclarativeMappingExecutor.parseMappingSpec(content);
            if (!validateDeclarative(mappingSpec, sampleRecords)) {
                log.debug("Declarative mapping validation failed against sample records");
                return Optional.empty();
            }

            log.info("Compiled declarative mapping for fingerprint {}", fingerprint);
            return Optional.of(CompiledTransform.declarative(content, fingerprint, response.model()));

        } catch (Exception e) {
            log.debug("Declarative compilation failed: {}", e.getMessage());
            return Optional.empty();
        }
    }

    private Optional<CompiledTransform> tryJavaScriptCompile(
            String sourceDescription, String sampleJson,
            List<RawRecord> sampleRecords, String fingerprint) {

        String prompt = buildJavaScriptPrompt(sourceDescription, sampleJson);

        for (int attempt = 0; attempt < 3; attempt++) {
            try {
                LlmResponse response = callLlm(prompt);
                String content = extractCodeBlock(response.content().trim());

                // Validate: execute against sample records
                if (validateJavaScript(content, sampleRecords)) {
                    log.info("Compiled JavaScript transform for fingerprint {} (attempt {})",
                            fingerprint, attempt);
                    return Optional.of(CompiledTransform.javascript(content, fingerprint, response.model()));
                }

                log.debug("JavaScript validation failed on attempt {}", attempt);
                // Build correction prompt for next attempt
                prompt = buildJavaScriptCorrectionPrompt(sourceDescription, sampleJson, content);

            } catch (Exception e) {
                log.debug("JavaScript compilation attempt {} failed: {}", attempt, e.getMessage());
            }
        }

        log.warn("Failed to compile transform for fingerprint {} after 3 attempts", fingerprint);
        return Optional.empty();
    }

    private String buildDeclarativePrompt(String sourceDescription, String sampleJson) {
        StringBuilder sb = new StringBuilder();
        sb.append("You are a data transformation compiler. Generate a DECLARATIVE field mapping specification.\n\n");
        sb.append("Source record structure: ").append(sourceDescription).append("\n\n");
        sb.append("Sample source record:\n").append(sampleJson).append("\n\n");

        if (targetSchema != null && !targetSchema.isEmpty()) {
            sb.append("Target JSON Schema:\n").append(targetSchema).append("\n\n");
        }

        sb.append("""
                Generate a JSON mapping specification with this EXACT structure:
                {
                  "mappings": {
                    "targetFieldName": "$.sourceFieldPath",
                    "another_field": "$.nested.source.path"
                  },
                  "defaults": {
                    "fieldWithDefaultValue": "static_value"
                  },
                  "typeCasts": {
                    "fieldName": "integer|number|boolean|string"
                  }
                }

                Rules:
                - Use "$." prefix for source paths (e.g., "$.userName", "$.address.city")
                - Array access: "$.items[0].name"
                - Only include "defaults" if there are fields with static values
                - Only include "typeCasts" if type conversion is needed
                - If the transformation requires conditional logic, string manipulation, date formatting, \
                or computation, respond with ONLY the text "NEEDS_JAVASCRIPT" — nothing else.
                - Respond with ONLY the JSON object, no explanation, no markdown formatting.
                """);

        return sb.toString();
    }

    private String buildJavaScriptPrompt(String sourceDescription, String sampleJson) {
        StringBuilder sb = new StringBuilder();
        sb.append("You are a data transformation compiler. Generate a JavaScript FUNCTION BODY.\n\n");
        sb.append("Source record structure: ").append(sourceDescription).append("\n\n");
        sb.append("Sample source record:\n").append(sampleJson).append("\n\n");

        if (targetSchema != null && !targetSchema.isEmpty()) {
            sb.append("Target JSON Schema:\n").append(targetSchema).append("\n\n");
        }

        sb.append("""
                Generate a JavaScript function body that transforms the input.
                The function receives a single parameter called 'input' (a parsed JSON object).
                It must return a new object matching the target schema.

                Rules:
                - Return ONLY the function body, no 'function' keyword, no wrapping
                - Must be deterministic: no Math.random(), no Date.now(), no external calls
                - Handle null/undefined values gracefully with || or ternary operators
                - Use only standard JavaScript (ES5 compatible)
                - No require(), import, fetch, XMLHttpRequest, or any I/O
                - Respond with ONLY the code, no explanation, no markdown formatting

                Example output:
                var result = {};
                result.user_name = input.userName || '';
                result.email = input.emailAddress || '';
                result.age = parseInt(input.age) || 0;
                return result;
                """);

        return sb.toString();
    }

    private String buildJavaScriptCorrectionPrompt(String sourceDescription,
                                                    String sampleJson, String previousCode) {
        return """
                Your previous JavaScript function body was invalid or produced incorrect output.
                Previous code:
                %s

                Source structure: %s
                Sample input: %s
                %s

                Fix the function body. Return ONLY the corrected code, no explanation.
                """.formatted(previousCode, sourceDescription, sampleJson,
                targetSchema != null ? "Target schema: " + targetSchema : "");
    }

    private boolean validateDeclarative(JsonNode mappingSpec, List<RawRecord> sampleRecords) {
        int samplesToCheck = Math.min(validationSamples, sampleRecords.size());
        for (int i = 0; i < samplesToCheck; i++) {
            try {
                JsonNode input = parseSample(sampleRecords.get(i));
                if (input == null) continue;

                JsonNode result = DeclarativeMappingExecutor.execute(input, mappingSpec);
                String resultJson = OBJECT_MAPPER.writeValueAsString(result);

                if (schemaEnforcer.hasSchema()) {
                    List<String> errors = schemaEnforcer.validateWithErrors(resultJson);
                    if (!errors.isEmpty()) {
                        log.debug("Declarative validation failed for sample {}: {}", i, errors);
                        return false;
                    }
                }
            } catch (Exception e) {
                log.debug("Declarative validation error on sample {}: {}", i, e.getMessage());
                return false;
            }
        }
        return true;
    }

    private boolean validateJavaScript(String jsFunctionBody, List<RawRecord> sampleRecords) {
        if (jsExecutor == null) {
            return false;
        }

        int samplesToCheck = Math.min(validationSamples, sampleRecords.size());
        for (int i = 0; i < samplesToCheck; i++) {
            try {
                JsonNode input = parseSample(sampleRecords.get(i));
                if (input == null) continue;

                String inputJson = OBJECT_MAPPER.writeValueAsString(input);
                if (!jsExecutor.validate(jsFunctionBody, inputJson)) {
                    return false;
                }

                // Also validate output against target schema
                if (schemaEnforcer.hasSchema()) {
                    String resultJson = jsExecutor.execute(jsFunctionBody, inputJson);
                    List<String> errors = schemaEnforcer.validateWithErrors(resultJson);
                    if (!errors.isEmpty()) {
                        log.debug("JS validation failed for sample {}: {}", i, errors);
                        return false;
                    }
                }
            } catch (Exception e) {
                log.debug("JS validation error on sample {}: {}", i, e.getMessage());
                return false;
            }
        }
        return true;
    }

    private LlmResponse callLlm(String userPrompt) {
        LlmRequest request = LlmRequest.builder()
                .systemPrompt("You are a code generator for data transformation. " +
                        "You produce ONLY executable code, never explanations.")
                .userPrompt(userPrompt)
                .model(model)
                .enablePromptCaching(enablePromptCaching)
                .temperature(0.0)
                .maxTokens(4096)
                .build();

        return llmClient.call(request);
    }

    /**
     * Extract code from potential markdown code blocks.
     * Handles: ```json ... ```, ```javascript ... ```, or raw content.
     */
    static String extractCodeBlock(String content) {
        if (content.startsWith("```")) {
            // Find end of first line (language marker)
            int firstNewline = content.indexOf('\n');
            int lastFence = content.lastIndexOf("```");
            if (firstNewline >= 0 && lastFence > firstNewline) {
                return content.substring(firstNewline + 1, lastFence).trim();
            }
        }
        return content;
    }

    private JsonNode parseSample(RawRecord record) {
        if (record.value() == null) return null;
        try {
            String json = new String(record.value(), StandardCharsets.UTF_8);
            JsonNode node = OBJECT_MAPPER.readTree(json);
            // If the record has a "value" wrapper, unwrap it
            if (node.has("value") && node.has("key") && node.size() <= 3) {
                String innerValue = node.get("value").asText();
                try {
                    return OBJECT_MAPPER.readTree(innerValue);
                } catch (Exception e) {
                    // value is not JSON, use as-is
                }
            }
            return node;
        } catch (Exception e) {
            return null;
        }
    }
}
