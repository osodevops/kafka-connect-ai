package sh.oso.nexus.connect.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.nexus.api.model.RawRecord;
import sh.oso.nexus.api.model.TransformedRecord;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.regex.Pattern;

public class DeterministicTransformer {

    private static final Logger log = LoggerFactory.getLogger(DeterministicTransformer.class);
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Set<String> patterns;

    public DeterministicTransformer(Set<String> patterns) {
        this.patterns = patterns;
    }

    public Optional<List<TransformedRecord>> transform(List<RawRecord> records) {
        if (patterns.isEmpty()) {
            return Optional.empty();
        }

        try {
            List<TransformedRecord> results = new ArrayList<>();
            for (RawRecord record : records) {
                if (record.value() == null) {
                    results.add(new TransformedRecord(record.key(), record.value(),
                            Map.of(), record.sourceOffset()));
                    continue;
                }

                String json = new String(record.value(), StandardCharsets.UTF_8);
                JsonNode node = objectMapper.readTree(json);

                if (!node.isObject()) {
                    return Optional.empty();
                }

                ObjectNode transformed = (ObjectNode) node.deepCopy();
                boolean anyApplied = false;

                for (String pattern : patterns) {
                    boolean applied = applyPattern(pattern, transformed);
                    anyApplied = anyApplied || applied;
                }

                if (!anyApplied) {
                    return Optional.empty();
                }

                byte[] value = objectMapper.writeValueAsBytes(transformed);
                results.add(new TransformedRecord(record.key(), value, Map.of(), record.sourceOffset()));
            }
            return Optional.of(results);
        } catch (Exception e) {
            log.debug("Deterministic transform failed: {}", e.getMessage());
            return Optional.empty();
        }
    }

    private boolean applyPattern(String pattern, ObjectNode node) {
        return switch (pattern) {
            case "field_rename" -> applyFieldRename(node);
            case "type_cast" -> applyTypeCast(node);
            case "timestamp_format" -> applyTimestampFormat(node);
            default -> false;
        };
    }

    private boolean applyFieldRename(ObjectNode node) {
        // Convert camelCase to snake_case
        List<String> fieldNames = new ArrayList<>();
        node.fieldNames().forEachRemaining(fieldNames::add);

        boolean changed = false;
        for (String name : fieldNames) {
            String snakeName = camelToSnake(name);
            if (!snakeName.equals(name)) {
                node.set(snakeName, node.get(name));
                node.remove(name);
                changed = true;
            }
        }
        return changed;
    }

    private boolean applyTypeCast(ObjectNode node) {
        boolean changed = false;
        List<String> fieldNames = new ArrayList<>();
        node.fieldNames().forEachRemaining(fieldNames::add);

        for (String name : fieldNames) {
            JsonNode value = node.get(name);
            if (value.isTextual()) {
                String text = value.asText();
                // Try parsing as number
                try {
                    if (text.contains(".")) {
                        double d = Double.parseDouble(text);
                        node.put(name, d);
                        changed = true;
                    } else {
                        long l = Long.parseLong(text);
                        node.put(name, l);
                        changed = true;
                    }
                } catch (NumberFormatException ignored) {
                    // Try parsing as boolean
                    if ("true".equalsIgnoreCase(text) || "false".equalsIgnoreCase(text)) {
                        node.put(name, Boolean.parseBoolean(text));
                        changed = true;
                    }
                }
            }
        }
        return changed;
    }

    private boolean applyTimestampFormat(ObjectNode node) {
        boolean changed = false;
        List<String> fieldNames = new ArrayList<>();
        node.fieldNames().forEachRemaining(fieldNames::add);

        for (String name : fieldNames) {
            JsonNode value = node.get(name);
            if (value.isTextual()) {
                String text = value.asText();
                try {
                    Instant instant = Instant.parse(text);
                    String iso = DateTimeFormatter.ISO_INSTANT.format(instant);
                    if (!iso.equals(text)) {
                        node.put(name, iso);
                        changed = true;
                    }
                } catch (DateTimeParseException ignored) {
                    // Try epoch millis
                    if (value.isNumber() || (value.isTextual() && text.matches("\\d{10,13}"))) {
                        try {
                            long epoch = Long.parseLong(text);
                            if (epoch > 1_000_000_000_000L) {
                                // epoch millis
                                node.put(name, Instant.ofEpochMilli(epoch).toString());
                            } else {
                                // epoch seconds
                                node.put(name, Instant.ofEpochSecond(epoch).toString());
                            }
                            changed = true;
                        } catch (NumberFormatException ignored2) {
                            // not a timestamp
                        }
                    }
                }
            }
        }
        return changed;
    }

    static String camelToSnake(String camel) {
        return Pattern.compile("([a-z])([A-Z])")
                .matcher(camel)
                .replaceAll("$1_$2")
                .toLowerCase();
    }
}
