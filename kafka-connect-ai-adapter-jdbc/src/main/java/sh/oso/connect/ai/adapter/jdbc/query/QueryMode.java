package sh.oso.connect.ai.adapter.jdbc.query;

public enum QueryMode {
    BULK,
    TIMESTAMP,
    INCREMENTING,
    TIMESTAMP_INCREMENTING;

    public static QueryMode fromString(String value) {
        return switch (value.toLowerCase().replace("+", "_").replace("-", "_")) {
            case "bulk" -> BULK;
            case "timestamp" -> TIMESTAMP;
            case "incrementing" -> INCREMENTING;
            case "timestamp_incrementing", "timestamp+incrementing" -> TIMESTAMP_INCREMENTING;
            default -> throw new IllegalArgumentException(
                    "Unknown query mode: " + value + ". Supported: bulk, timestamp, incrementing, timestamp+incrementing");
        };
    }
}
