package sh.oso.connect.ai.connect.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Pattern;

public class PiiMasker {

    private static final Logger log = LoggerFactory.getLogger(PiiMasker.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public record MaskResult(String maskedJson, Map<String, String> maskMap) {}

    private final Set<String> fieldNames;
    private final List<Pattern> valuePatterns;
    private final String replacement;
    private final boolean unmaskOutput;

    public PiiMasker(Set<String> fieldNames, List<Pattern> valuePatterns,
                     String replacement, boolean unmaskOutput) {
        this.fieldNames = new HashSet<>();
        for (String name : fieldNames) {
            this.fieldNames.add(name.toLowerCase());
        }
        this.valuePatterns = valuePatterns;
        this.replacement = replacement;
        this.unmaskOutput = unmaskOutput;
    }

    public MaskResult mask(String json) {
        try {
            JsonNode root = objectMapper.readTree(json);
            Map<String, String> maskMap = new LinkedHashMap<>();
            int[] counter = {0};
            JsonNode masked = maskNode(root, maskMap, counter);
            return new MaskResult(objectMapper.writeValueAsString(masked), maskMap);
        } catch (Exception e) {
            log.warn("Failed to mask PII in JSON: {}", e.getMessage());
            return new MaskResult(json, Map.of());
        }
    }

    private JsonNode maskNode(JsonNode node, Map<String, String> maskMap, int[] counter) {
        if (node.isObject()) {
            ObjectNode result = objectMapper.createObjectNode();
            Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                String fieldName = field.getKey();
                JsonNode value = field.getValue();

                if (shouldMaskField(fieldName) && value.isTextual()) {
                    String placeholder = "[MASKED_" + counter[0] + "]";
                    maskMap.put(placeholder, value.asText());
                    counter[0]++;
                    result.set(fieldName, new TextNode(placeholder));
                } else if (value.isTextual() && matchesPattern(value.asText())) {
                    String placeholder = "[MASKED_" + counter[0] + "]";
                    maskMap.put(placeholder, value.asText());
                    counter[0]++;
                    result.set(fieldName, new TextNode(placeholder));
                } else {
                    result.set(fieldName, maskNode(value, maskMap, counter));
                }
            }
            return result;
        } else if (node.isArray()) {
            ArrayNode result = objectMapper.createArrayNode();
            for (JsonNode element : node) {
                result.add(maskNode(element, maskMap, counter));
            }
            return result;
        } else if (node.isTextual() && matchesPattern(node.asText())) {
            String placeholder = "[MASKED_" + counter[0] + "]";
            maskMap.put(placeholder, node.asText());
            counter[0]++;
            return new TextNode(placeholder);
        }
        return node;
    }

    private boolean shouldMaskField(String fieldName) {
        return fieldNames.contains(fieldName.toLowerCase());
    }

    private boolean matchesPattern(String value) {
        for (Pattern pattern : valuePatterns) {
            if (pattern.matcher(value).find()) {
                return true;
            }
        }
        return false;
    }

    public String unmask(String llmOutput, Map<String, String> maskMap) {
        if (!unmaskOutput || maskMap.isEmpty()) {
            return llmOutput;
        }
        String result = llmOutput;
        for (Map.Entry<String, String> entry : maskMap.entrySet()) {
            result = result.replace(entry.getKey(), entry.getValue());
        }
        return result;
    }

    public boolean isEnabled() {
        return !fieldNames.isEmpty() || !valuePatterns.isEmpty();
    }
}
