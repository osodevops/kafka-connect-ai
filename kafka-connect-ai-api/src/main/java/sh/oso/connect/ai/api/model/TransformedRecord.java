package sh.oso.connect.ai.api.model;

import java.util.Map;

public record TransformedRecord(
        byte[] key,
        byte[] value,
        Map<String, String> headers,
        SourceOffset sourceOffset
) {
    public TransformedRecord {
        headers = headers != null ? Map.copyOf(headers) : Map.of();
        sourceOffset = sourceOffset != null ? sourceOffset : SourceOffset.empty();
    }
}
