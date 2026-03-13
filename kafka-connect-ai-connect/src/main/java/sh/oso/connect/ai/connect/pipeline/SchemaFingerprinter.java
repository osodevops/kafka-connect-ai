package sh.oso.connect.ai.connect.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/**
 * Generates deterministic fingerprints from JSON record structure.
 * Used to detect when a new schema pair appears vs when we can reuse
 * a previously compiled transformation.
 *
 * Fingerprint is based on field names, types, and nesting depth — not values.
 * Two records with the same structure but different values produce the same fingerprint.
 */
public class SchemaFingerprinter {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /**
     * Generate a fingerprint from a sample JSON record and the target schema.
     * Returns a hex-encoded SHA-256 hash.
     */
    public static String fingerprint(byte[] sampleRecord, String targetSchema) {
        TreeMap<String, String> structure = new TreeMap<>();
        try {
            JsonNode node = OBJECT_MAPPER.readTree(sampleRecord);
            extractStructure(node, "", structure);
        } catch (Exception e) {
            // Unparseable record — use raw content hash as fallback
            structure.put("_raw", "unparseable");
        }

        String canonical = structure + "|" + (targetSchema != null ? targetSchema : "");
        return sha256(canonical);
    }

    /**
     * Generate a fingerprint from a parsed JsonNode.
     */
    public static String fingerprint(JsonNode sampleNode, String targetSchema) {
        TreeMap<String, String> structure = new TreeMap<>();
        extractStructure(sampleNode, "", structure);
        String canonical = structure + "|" + (targetSchema != null ? targetSchema : "");
        return sha256(canonical);
    }

    /**
     * Extract the structural schema from a JSON node.
     * Produces a sorted map of dotted field paths to type names.
     * Example: {"user": {"name": "Alice", "age": 30}} →
     *   {"user": "object", "user.age": "number", "user.name": "string"}
     */
    static void extractStructure(JsonNode node, String prefix, TreeMap<String, String> out) {
        if (node.isObject()) {
            if (!prefix.isEmpty()) {
                out.put(prefix, "object");
            }
            Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> entry = fields.next();
                String path = prefix.isEmpty() ? entry.getKey() : prefix + "." + entry.getKey();
                extractStructure(entry.getValue(), path, out);
            }
        } else if (node.isArray()) {
            out.put(prefix.isEmpty() ? "_root" : prefix, "array");
            if (!node.isEmpty()) {
                // Use first element as representative of array item schema
                extractStructure(node.get(0), (prefix.isEmpty() ? "_root" : prefix) + "[]", out);
            }
        } else if (node.isTextual()) {
            out.put(prefix, "string");
        } else if (node.isIntegralNumber()) {
            out.put(prefix, "integer");
        } else if (node.isFloatingPointNumber()) {
            out.put(prefix, "number");
        } else if (node.isBoolean()) {
            out.put(prefix, "boolean");
        } else if (node.isNull()) {
            out.put(prefix, "null");
        }
    }

    /**
     * Describe the structure of a record in human-readable form for LLM prompts.
     * Example output: "userName (string), age (integer), address (object: street (string), city (string))"
     */
    public static String describeStructure(JsonNode node) {
        StringBuilder sb = new StringBuilder();
        describeNode(node, sb, 0);
        return sb.toString();
    }

    private static void describeNode(JsonNode node, StringBuilder sb, int depth) {
        if (node.isObject()) {
            Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
            boolean first = true;
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> entry = fields.next();
                if (!first) sb.append(", ");
                first = false;
                sb.append(entry.getKey()).append(" (");
                JsonNode value = entry.getValue();
                if (value.isObject()) {
                    sb.append("object: ");
                    describeNode(value, sb, depth + 1);
                } else if (value.isArray()) {
                    sb.append("array");
                    if (!value.isEmpty()) {
                        sb.append(" of ");
                        describeNode(value.get(0), sb, depth + 1);
                    }
                } else {
                    sb.append(nodeTypeName(value));
                }
                sb.append(")");
            }
        } else {
            sb.append(nodeTypeName(node));
        }
    }

    private static String nodeTypeName(JsonNode node) {
        if (node.isTextual()) return "string";
        if (node.isIntegralNumber()) return "integer";
        if (node.isFloatingPointNumber()) return "number";
        if (node.isBoolean()) return "boolean";
        if (node.isNull()) return "null";
        if (node.isObject()) return "object";
        if (node.isArray()) return "array";
        return "unknown";
    }

    private static String sha256(String input) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(input.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder();
            for (byte b : hash) {
                hex.append(String.format("%02x", b));
            }
            return hex.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 not available", e);
        }
    }
}
