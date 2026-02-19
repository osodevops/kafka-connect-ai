package sh.oso.connect.ai.connect.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class SchemaEnforcer {

    private static final Logger log = LoggerFactory.getLogger(SchemaEnforcer.class);

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final String schemaJson;
    private final boolean hasSchema;
    private Set<String> requiredFields;
    private Map<String, String> fieldTypes;
    private Map<String, List<String>> fieldEnums;

    public SchemaEnforcer(String schemaJson) {
        this.schemaJson = schemaJson != null ? schemaJson.trim() : "";
        this.hasSchema = !this.schemaJson.isEmpty();

        if (this.hasSchema) {
            parseSchema();
        }
    }

    public boolean validate(String json) {
        return validateWithErrors(json).isEmpty();
    }

    public List<String> validateWithErrors(String json) {
        List<String> errors = new ArrayList<>();

        if (!hasSchema) {
            if (!isValidJson(json)) {
                errors.add("Invalid JSON");
            }
            return errors;
        }

        try {
            JsonNode node = objectMapper.readTree(json);
            if (!node.isObject()) {
                errors.add("Expected JSON object but got: " + node.getNodeType());
                return errors;
            }

            if (requiredFields != null) {
                for (String field : requiredFields) {
                    if (!node.has(field)) {
                        errors.add("Missing required field: " + field);
                    }
                }
            }

            if (fieldTypes != null) {
                for (Map.Entry<String, String> entry : fieldTypes.entrySet()) {
                    String field = entry.getKey();
                    String expectedType = entry.getValue();
                    if (node.has(field) && !node.get(field).isNull()) {
                        String error = validateType(node.get(field), field, expectedType);
                        if (error != null) {
                            errors.add(error);
                        }
                    }
                }
            }

            if (fieldEnums != null) {
                for (Map.Entry<String, List<String>> entry : fieldEnums.entrySet()) {
                    String field = entry.getKey();
                    List<String> allowedValues = entry.getValue();
                    if (node.has(field) && !node.get(field).isNull()) {
                        String value = node.get(field).asText();
                        if (!allowedValues.contains(value)) {
                            errors.add("Field '" + field + "' value '" + value
                                    + "' not in allowed values: " + allowedValues);
                        }
                    }
                }
            }
        } catch (Exception e) {
            errors.add("Schema validation failed: " + e.getMessage());
        }

        if (!errors.isEmpty()) {
            log.warn("Schema validation errors: {}", errors);
        }
        return errors;
    }

    private String validateType(JsonNode value, String field, String expectedType) {
        return switch (expectedType) {
            case "string" -> value.isTextual() ? null
                    : "Field '" + field + "' expected string but got " + value.getNodeType();
            case "number", "numeric" -> (value.isNumber()) ? null
                    : "Field '" + field + "' expected number but got " + value.getNodeType();
            case "integer" -> value.isIntegralNumber() ? null
                    : "Field '" + field + "' expected integer but got " + value.getNodeType();
            case "boolean" -> value.isBoolean() ? null
                    : "Field '" + field + "' expected boolean but got " + value.getNodeType();
            case "array" -> value.isArray() ? null
                    : "Field '" + field + "' expected array but got " + value.getNodeType();
            case "object" -> value.isObject() ? null
                    : "Field '" + field + "' expected object but got " + value.getNodeType();
            default -> null;
        };
    }

    private boolean isValidJson(String json) {
        try {
            objectMapper.readTree(json);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private void parseSchema() {
        try {
            JsonNode schema = objectMapper.readTree(schemaJson);

            // Parse required fields
            JsonNode required = schema.get("required");
            if (required != null && required.isArray()) {
                requiredFields = new HashSet<>();
                for (JsonNode field : required) {
                    requiredFields.add(field.asText());
                }
            }

            // Parse properties for types and enums
            JsonNode properties = schema.get("properties");
            if (properties != null) {
                if (requiredFields == null) {
                    requiredFields = new HashSet<>();
                    Iterator<String> fieldNames = properties.fieldNames();
                    while (fieldNames.hasNext()) {
                        requiredFields.add(fieldNames.next());
                    }
                }

                fieldTypes = new HashMap<>();
                fieldEnums = new HashMap<>();

                Iterator<Map.Entry<String, JsonNode>> fields = properties.fields();
                while (fields.hasNext()) {
                    Map.Entry<String, JsonNode> entry = fields.next();
                    String fieldName = entry.getKey();
                    JsonNode fieldDef = entry.getValue();

                    JsonNode typeNode = fieldDef.get("type");
                    if (typeNode != null) {
                        fieldTypes.put(fieldName, typeNode.asText());
                    }

                    JsonNode enumNode = fieldDef.get("enum");
                    if (enumNode != null && enumNode.isArray()) {
                        List<String> values = new ArrayList<>();
                        for (JsonNode v : enumNode) {
                            values.add(v.asText());
                        }
                        fieldEnums.put(fieldName, values);
                    }
                }
            }
        } catch (Exception e) {
            log.warn("Failed to parse schema, will skip validation: {}", e.getMessage());
            requiredFields = null;
            fieldTypes = null;
            fieldEnums = null;
        }
    }

    public boolean hasSchema() {
        return hasSchema;
    }

    public String getSchemaJson() {
        return schemaJson;
    }
}
