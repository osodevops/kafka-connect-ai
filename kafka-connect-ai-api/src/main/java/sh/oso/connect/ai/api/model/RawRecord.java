package sh.oso.connect.ai.api.model;

import java.util.Map;

public record RawRecord(
        byte[] key,
        byte[] value,
        Map<String, String> metadata,
        SourceOffset sourceOffset
) {
    public RawRecord {
        metadata = metadata != null ? Map.copyOf(metadata) : Map.of();
        sourceOffset = sourceOffset != null ? sourceOffset : SourceOffset.empty();
    }
}
