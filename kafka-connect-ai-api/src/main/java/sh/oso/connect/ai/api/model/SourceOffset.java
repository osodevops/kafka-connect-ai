package sh.oso.connect.ai.api.model;

import java.util.Map;

public record SourceOffset(Map<String, String> partition, Map<String, Object> offset) {

    public SourceOffset {
        partition = Map.copyOf(partition);
        offset = Map.copyOf(offset);
    }

    public static SourceOffset empty() {
        return new SourceOffset(Map.of(), Map.of());
    }
}
