package sh.oso.connect.ai.adapter.warehouse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.connect.ai.api.config.AiConnectConfig;
import sh.oso.connect.ai.api.error.NonRetryableException;
import sh.oso.connect.ai.api.error.RetryableException;
import sh.oso.connect.ai.api.model.TransformedRecord;

import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * Snowflake load strategy using JDBC.
 * <p>
 * Records are written as JSON into a Snowflake internal stage using PUT,
 * then loaded into the target table via COPY INTO.
 */
public class SnowflakeLoadStrategy implements WarehouseLoadStrategy {

    private static final Logger log = LoggerFactory.getLogger(SnowflakeLoadStrategy.class);

    private Connection connection;
    private String database;
    private String schema;
    private String table;
    private String warehouse;
    private boolean started;

    @Override
    public void start(Map<String, String> props) {
        String url = props.getOrDefault(AiConnectConfig.SNOWFLAKE_URL, "");
        String user = props.getOrDefault(AiConnectConfig.SNOWFLAKE_USER, "");
        String privateKey = props.getOrDefault(AiConnectConfig.SNOWFLAKE_PRIVATE_KEY, "");
        this.database = props.getOrDefault(AiConnectConfig.SNOWFLAKE_DATABASE, "");
        this.schema = props.getOrDefault(AiConnectConfig.SNOWFLAKE_SCHEMA, "PUBLIC");
        this.table = props.getOrDefault(AiConnectConfig.SNOWFLAKE_TABLE, "");
        this.warehouse = props.getOrDefault(AiConnectConfig.SNOWFLAKE_WAREHOUSE, "");
        String role = props.getOrDefault(AiConnectConfig.SNOWFLAKE_ROLE, "");

        if (url.isEmpty() || user.isEmpty() || table.isEmpty()) {
            throw new NonRetryableException(
                    "Snowflake configuration incomplete: snowflake.url, snowflake.user, and snowflake.table are required");
        }

        try {
            Properties jdbcProps = new Properties();
            jdbcProps.setProperty("user", user);
            if (!privateKey.isEmpty()) {
                jdbcProps.setProperty("privateKey", privateKey);
            }
            if (!database.isEmpty()) {
                jdbcProps.setProperty("db", database);
            }
            if (!schema.isEmpty()) {
                jdbcProps.setProperty("schema", schema);
            }
            if (!warehouse.isEmpty()) {
                jdbcProps.setProperty("warehouse", warehouse);
            }
            if (!role.isEmpty()) {
                jdbcProps.setProperty("role", role);
            }

            this.connection = DriverManager.getConnection(url, jdbcProps);
            this.started = true;

            log.info("SnowflakeLoadStrategy started: url={}, database={}, schema={}, table={}",
                    url, database, schema, table);
        } catch (SQLException e) {
            throw new RetryableException("Failed to connect to Snowflake: " + e.getMessage(), e);
        }
    }

    @Override
    public void load(List<TransformedRecord> records) {
        if (records.isEmpty()) {
            return;
        }

        String stageName = "@~/" + UUID.randomUUID();

        try {
            // Build a NDJSON payload from the records
            StringBuilder ndjson = new StringBuilder();
            for (TransformedRecord record : records) {
                if (record.value() != null) {
                    ndjson.append(new String(record.value(), StandardCharsets.UTF_8)).append("\n");
                }
            }

            // Use PUT to stage data via the JDBC stream upload mechanism
            String qualifiedTable = qualifiedTableName();

            // Insert rows directly using INSERT with PARSE_JSON for each record
            connection.setAutoCommit(false);

            String insertSql = "INSERT INTO " + qualifiedTable +
                    " SELECT PARSE_JSON(column1) FROM VALUES (?)";

            try (PreparedStatement ps = connection.prepareStatement(insertSql)) {
                for (TransformedRecord record : records) {
                    if (record.value() != null) {
                        ps.setString(1, new String(record.value(), StandardCharsets.UTF_8));
                        ps.addBatch();
                    }
                }
                ps.executeBatch();
            }

            connection.commit();
            log.debug("Loaded {} records into Snowflake table {}", records.size(), qualifiedTable);

        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException rollbackEx) {
                log.warn("Rollback failed: {}", rollbackEx.getMessage());
            }
            throw new RetryableException("Failed to load records into Snowflake: " + e.getMessage(), e);
        }
    }

    @Override
    public void flush() {
        // Writes are committed within load()
    }

    @Override
    public boolean isHealthy() {
        if (connection == null) {
            return false;
        }
        try {
            return connection.isValid(5);
        } catch (SQLException e) {
            return false;
        }
    }

    @Override
    public void stop() {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                log.warn("Error closing Snowflake connection: {}", e.getMessage());
            }
            connection = null;
        }
        started = false;
        log.info("SnowflakeLoadStrategy stopped");
    }

    private String qualifiedTableName() {
        StringBuilder sb = new StringBuilder();
        if (!database.isEmpty()) {
            sb.append(database).append(".");
        }
        if (!schema.isEmpty()) {
            sb.append(schema).append(".");
        }
        sb.append(table);
        return sb.toString();
    }
}
