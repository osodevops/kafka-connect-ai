package sh.oso.nexus.adapter.jdbc.query;

import sh.oso.nexus.api.model.SourceOffset;

import java.util.ArrayList;
import java.util.List;

public class QueryBuilder {

    private final String table;
    private final QueryMode mode;
    private final String timestampColumn;
    private final String incrementingColumn;

    public QueryBuilder(String table, QueryMode mode, String timestampColumn, String incrementingColumn) {
        this.table = table;
        this.mode = mode;
        this.timestampColumn = timestampColumn;
        this.incrementingColumn = incrementingColumn;
    }

    public String buildQuery(SourceOffset offset, int maxRecords) {
        StringBuilder sql = new StringBuilder("SELECT * FROM ").append(table);

        List<String> conditions = new ArrayList<>();

        switch (mode) {
            case TIMESTAMP -> {
                Object lastTs = offset.offset().get("timestamp");
                if (lastTs != null) {
                    conditions.add(timestampColumn + " > '" + lastTs + "'");
                }
                sql.append(buildWhere(conditions));
                sql.append(" ORDER BY ").append(timestampColumn).append(" ASC");
            }
            case INCREMENTING -> {
                Object lastId = offset.offset().get("incrementing");
                if (lastId != null) {
                    conditions.add(incrementingColumn + " > " + lastId);
                }
                sql.append(buildWhere(conditions));
                sql.append(" ORDER BY ").append(incrementingColumn).append(" ASC");
            }
            case TIMESTAMP_INCREMENTING -> {
                Object lastTs = offset.offset().get("timestamp");
                Object lastId = offset.offset().get("incrementing");
                if (lastTs != null && lastId != null) {
                    conditions.add("(" + timestampColumn + " > '" + lastTs + "'"
                            + " OR (" + timestampColumn + " = '" + lastTs + "'"
                            + " AND " + incrementingColumn + " > " + lastId + "))");
                } else if (lastTs != null) {
                    conditions.add(timestampColumn + " > '" + lastTs + "'");
                } else if (lastId != null) {
                    conditions.add(incrementingColumn + " > " + lastId);
                }
                sql.append(buildWhere(conditions));
                sql.append(" ORDER BY ").append(timestampColumn).append(" ASC, ")
                        .append(incrementingColumn).append(" ASC");
            }
            case BULK -> {
                // No WHERE clause, no ordering
            }
        }

        if (maxRecords > 0) {
            sql.append(" LIMIT ").append(maxRecords);
        }

        return sql.toString();
    }

    private String buildWhere(List<String> conditions) {
        if (conditions.isEmpty()) {
            return "";
        }
        return " WHERE " + String.join(" AND ", conditions);
    }

    public String getTimestampColumn() {
        return timestampColumn;
    }

    public String getIncrementingColumn() {
        return incrementingColumn;
    }

    public QueryMode getMode() {
        return mode;
    }
}
