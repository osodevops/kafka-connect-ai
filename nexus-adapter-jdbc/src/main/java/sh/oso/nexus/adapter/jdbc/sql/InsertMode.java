package sh.oso.nexus.adapter.jdbc.sql;

public enum InsertMode {
    INSERT,
    UPSERT,
    UPDATE;

    public static InsertMode fromString(String value) {
        return switch (value.toLowerCase()) {
            case "insert" -> INSERT;
            case "upsert" -> UPSERT;
            case "update" -> UPDATE;
            default -> throw new IllegalArgumentException(
                    "Unknown insert mode: " + value + ". Supported: insert, upsert, update");
        };
    }
}
