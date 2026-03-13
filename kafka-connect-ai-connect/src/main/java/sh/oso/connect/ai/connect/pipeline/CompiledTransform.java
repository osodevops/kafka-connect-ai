package sh.oso.connect.ai.connect.pipeline;

import java.time.Instant;

/**
 * Represents a compiled transformation that can execute without an LLM call.
 * Two tiers:
 *   DECLARATIVE — a JSON field-mapping spec executed with Jackson (fastest, ~500ns/record)
 *   JAVASCRIPT  — a JS function executed in GraalVM sandbox (~2μs/record)
 */
public record CompiledTransform(
        Type type,
        String code,
        String schemaFingerprint,
        Instant createdAt,
        String generatedByModel
) {
    public enum Type {
        /** JSON mapping spec: {"sourceField": "targetField", ...} */
        DECLARATIVE,
        /** JavaScript function body executed in GraalVM sandbox */
        JAVASCRIPT
    }

    public static CompiledTransform declarative(String mappingSpec, String fingerprint, String model) {
        return new CompiledTransform(Type.DECLARATIVE, mappingSpec, fingerprint, Instant.now(), model);
    }

    public static CompiledTransform javascript(String jsCode, String fingerprint, String model) {
        return new CompiledTransform(Type.JAVASCRIPT, jsCode, fingerprint, Instant.now(), model);
    }
}
