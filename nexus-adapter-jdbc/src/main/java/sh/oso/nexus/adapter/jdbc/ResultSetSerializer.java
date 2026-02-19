package sh.oso.nexus.adapter.jdbc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

public class ResultSetSerializer {

    private final ObjectMapper objectMapper = new ObjectMapper();

    public byte[] serializeRow(ResultSet rs) throws SQLException {
        try {
            ObjectNode node = objectMapper.createObjectNode();
            ResultSetMetaData meta = rs.getMetaData();
            int columnCount = meta.getColumnCount();

            for (int i = 1; i <= columnCount; i++) {
                String name = meta.getColumnLabel(i);
                int type = meta.getColumnType(i);

                if (rs.getObject(i) == null) {
                    node.putNull(name);
                    continue;
                }

                switch (type) {
                    case Types.INTEGER, Types.SMALLINT, Types.TINYINT -> node.put(name, rs.getInt(i));
                    case Types.BIGINT -> node.put(name, rs.getLong(i));
                    case Types.FLOAT, Types.REAL -> node.put(name, rs.getFloat(i));
                    case Types.DOUBLE -> node.put(name, rs.getDouble(i));
                    case Types.DECIMAL, Types.NUMERIC -> node.put(name, rs.getBigDecimal(i));
                    case Types.BOOLEAN, Types.BIT -> node.put(name, rs.getBoolean(i));
                    default -> node.put(name, rs.getString(i));
                }
            }

            return objectMapper.writeValueAsBytes(node);
        } catch (Exception e) {
            throw new SQLException("Failed to serialize ResultSet row to JSON", e);
        }
    }
}
