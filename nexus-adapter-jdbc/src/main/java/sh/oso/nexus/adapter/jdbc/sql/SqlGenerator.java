package sh.oso.nexus.adapter.jdbc.sql;

import java.util.List;
import java.util.stream.Collectors;

public class SqlGenerator {

    public String generateInsert(String table, List<String> columns) {
        String cols = String.join(", ", columns);
        String placeholders = columns.stream().map(c -> "?").collect(Collectors.joining(", "));
        return "INSERT INTO " + table + " (" + cols + ") VALUES (" + placeholders + ")";
    }

    public String generateUpsert(String table, List<String> columns, List<String> pkColumns) {
        String cols = String.join(", ", columns);
        String placeholders = columns.stream().map(c -> "?").collect(Collectors.joining(", "));
        String conflictCols = String.join(", ", pkColumns);

        List<String> nonPkColumns = columns.stream()
                .filter(c -> !pkColumns.contains(c))
                .toList();

        String updateSet = nonPkColumns.stream()
                .map(c -> c + " = EXCLUDED." + c)
                .collect(Collectors.joining(", "));

        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ").append(table)
                .append(" (").append(cols).append(")")
                .append(" VALUES (").append(placeholders).append(")")
                .append(" ON CONFLICT (").append(conflictCols).append(")");

        if (!nonPkColumns.isEmpty()) {
            sql.append(" DO UPDATE SET ").append(updateSet);
        } else {
            sql.append(" DO NOTHING");
        }

        return sql.toString();
    }

    public String generateCreateTable(String table, List<String> columns,
                                       List<String> columnTypes, List<String> pkColumns) {
        StringBuilder sql = new StringBuilder("CREATE TABLE IF NOT EXISTS ").append(table).append(" (");

        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append(columns.get(i)).append(" ").append(columnTypes.get(i));
        }

        if (!pkColumns.isEmpty()) {
            sql.append(", PRIMARY KEY (").append(String.join(", ", pkColumns)).append(")");
        }

        sql.append(")");
        return sql.toString();
    }

    public String generateUpdate(String table, List<String> columns, List<String> pkColumns) {
        List<String> setCols = columns.stream()
                .filter(c -> !pkColumns.contains(c))
                .toList();

        String setClause = setCols.stream()
                .map(c -> c + " = ?")
                .collect(Collectors.joining(", "));

        String whereClause = pkColumns.stream()
                .map(c -> c + " = ?")
                .collect(Collectors.joining(" AND "));

        return "UPDATE " + table + " SET " + setClause + " WHERE " + whereClause;
    }
}
