package sh.oso.connect.ai.connect.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;

/**
 * Tier 0 executor: applies a declarative field-mapping specification using pure Jackson.
 *
 * The LLM generates a JSON mapping spec like:
 * {
 *   "mappings": {
 *     "user_name": "$.userName",
 *     "email_address": "$.emailAddress",
 *     "created_at": "$.metadata.createdAt"
 *   },
 *   "defaults": {
 *     "version": 1,
 *     "source": "kafka-connect-ai"
 *   },
 *   "typeCasts": {
 *     "age": "integer",
 *     "active": "boolean",
 *     "score": "number"
 *   }
 * }
 *
 * This executes in ~500ns per record — pure Java, no scripting engine, no security risk.
 */
public class DeclarativeMappingExecutor {

    private static final Logger log = LoggerFactory.getLogger(DeclarativeMappingExecutor.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /**
     * Execute a declarative mapping on a single input record.
     *
     * @param input      the source JSON record
     * @param mappingSpec the compiled mapping specification (parsed once, reused)
     * @return transformed output node
     */
    public static JsonNode execute(JsonNode input, JsonNode mappingSpec) {
        ObjectNode output = OBJECT_MAPPER.createObjectNode();

        // Apply field mappings: "targetField" -> "$.source.path"
        JsonNode mappings = mappingSpec.get("mappings");
        if (mappings != null && mappings.isObject()) {
            Iterator<Map.Entry<String, JsonNode>> fields = mappings.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> entry = fields.next();
                String targetField = entry.getKey();
                String sourcePath = entry.getValue().asText();
                JsonNode resolved = resolvePath(input, sourcePath);
                if (resolved != null && !resolved.isMissingNode()) {
                    output.set(targetField, resolved.deepCopy());
                }
            }
        }

        // Apply defaults: fields with static values
        JsonNode defaults = mappingSpec.get("defaults");
        if (defaults != null && defaults.isObject()) {
            Iterator<Map.Entry<String, JsonNode>> fields = defaults.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> entry = fields.next();
                String targetField = entry.getKey();
                if (!output.has(targetField)) {
                    output.set(targetField, entry.getValue().deepCopy());
                }
            }
        }

        // Apply type casts
        JsonNode typeCasts = mappingSpec.get("typeCasts");
        if (typeCasts != null && typeCasts.isObject()) {
            Iterator<Map.Entry<String, JsonNode>> fields = typeCasts.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> entry = fields.next();
                String field = entry.getKey();
                String targetType = entry.getValue().asText();
                if (output.has(field)) {
                    applyCast(output, field, targetType);
                }
            }
        }

        return output;
    }

    /**
     * Parse a compiled mapping spec from JSON string.
     */
    public static JsonNode parseMappingSpec(String mappingSpecJson) {
        try {
            return OBJECT_MAPPER.readTree(mappingSpecJson);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid mapping spec JSON: " + e.getMessage(), e);
        }
    }

    /**
     * Validate that a mapping spec has the expected structure.
     */
    public static boolean isValidMappingSpec(String json) {
        try {
            JsonNode spec = OBJECT_MAPPER.readTree(json);
            return spec.isObject() && spec.has("mappings") && spec.get("mappings").isObject();
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Resolve a dotted path expression like "$.user.name" or "$.addresses[0].city"
     * against a JSON node.
     */
    static JsonNode resolvePath(JsonNode root, String path) {
        if (path == null || path.isEmpty()) {
            return root;
        }

        // Strip leading "$." prefix
        String cleanPath = path.startsWith("$.") ? path.substring(2) : path;

        JsonNode current = root;
        String[] segments = cleanPath.split("\\.");
        for (String segment : segments) {
            if (current == null || current.isMissingNode() || current.isNull()) {
                return null;
            }

            // Handle array index: "items[0]"
            int bracketIdx = segment.indexOf('[');
            if (bracketIdx >= 0) {
                String fieldName = segment.substring(0, bracketIdx);
                String indexStr = segment.substring(bracketIdx + 1, segment.indexOf(']'));
                if (!fieldName.isEmpty()) {
                    current = current.get(fieldName);
                }
                if (current != null && current.isArray()) {
                    int idx = Integer.parseInt(indexStr);
                    current = current.get(idx);
                } else {
                    return null;
                }
            } else {
                current = current.get(segment);
            }
        }
        return current;
    }

    private static void applyCast(ObjectNode node, String field, String targetType) {
        JsonNode value = node.get(field);
        if (value == null || value.isNull()) return;

        try {
            switch (targetType) {
                case "integer" -> {
                    if (value.isTextual()) {
                        node.put(field, Long.parseLong(value.asText()));
                    } else if (value.isFloatingPointNumber()) {
                        node.put(field, value.asLong());
                    }
                }
                case "number" -> {
                    if (value.isTextual()) {
                        node.put(field, Double.parseDouble(value.asText()));
                    }
                }
                case "boolean" -> {
                    if (value.isTextual()) {
                        node.put(field, Boolean.parseBoolean(value.asText()));
                    } else if (value.isNumber()) {
                        node.put(field, value.asInt() != 0);
                    }
                }
                case "string" -> {
                    if (!value.isTextual()) {
                        node.put(field, value.asText());
                    }
                }
                default -> log.debug("Unknown type cast: {}", targetType);
            }
        } catch (NumberFormatException e) {
            log.debug("Type cast failed for field '{}' to {}: {}", field, targetType, e.getMessage());
        }
    }
}
