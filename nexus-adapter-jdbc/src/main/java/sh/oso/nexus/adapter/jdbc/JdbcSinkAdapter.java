package sh.oso.nexus.adapter.jdbc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.nexus.adapter.jdbc.config.JdbcSinkConfig;
import sh.oso.nexus.adapter.jdbc.sql.InsertMode;
import sh.oso.nexus.adapter.jdbc.sql.SqlGenerator;
import sh.oso.nexus.api.adapter.SinkAdapter;
import sh.oso.nexus.api.config.NexusConfig;
import sh.oso.nexus.api.error.RetryableException;
import sh.oso.nexus.api.model.TransformedRecord;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.*;

public class JdbcSinkAdapter implements SinkAdapter {

    private static final Logger log = LoggerFactory.getLogger(JdbcSinkAdapter.class);

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final SqlGenerator sqlGenerator = new SqlGenerator();

    private HikariDataSource dataSource;
    private JdbcSinkConfig config;
    private InsertMode insertMode;
    private boolean tableCreated;

    @Override
    public String type() {
        return "jdbc";
    }

    @Override
    public ConfigDef configDef() {
        return new ConfigDef()
                .define(NexusConfig.JDBC_URL, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, "JDBC connection URL")
                .define(NexusConfig.JDBC_USER, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, "JDBC username")
                .define(NexusConfig.JDBC_PASSWORD, ConfigDef.Type.PASSWORD, "", ConfigDef.Importance.HIGH, "JDBC password")
                .define(NexusConfig.JDBC_TABLE, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, "Target table name")
                .define(NexusConfig.JDBC_SINK_PK_COLUMNS, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM, "Comma-separated primary key columns")
                .define(NexusConfig.JDBC_SINK_INSERT_MODE, ConfigDef.Type.STRING, "insert", ConfigDef.Importance.MEDIUM, "Insert mode: insert, upsert, update")
                .define(NexusConfig.JDBC_SINK_AUTO_DDL, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.LOW, "Auto-create table from record schema")
                .define(NexusConfig.JDBC_BATCH_SIZE, ConfigDef.Type.INT, 1000, ConfigDef.Importance.MEDIUM, "JDBC batch write size")
                .define(NexusConfig.JDBC_DRIVER_CLASS, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM, "JDBC driver class name");
    }

    @Override
    public void start(Map<String, String> props) {
        this.config = new JdbcSinkConfig(props);

        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(config.url());
        if (!config.user().isEmpty()) {
            hikariConfig.setUsername(config.user());
        }
        if (!config.password().isEmpty()) {
            hikariConfig.setPassword(config.password());
        }
        if (!config.driverClass().isEmpty()) {
            hikariConfig.setDriverClassName(config.driverClass());
        }
        hikariConfig.setMaximumPoolSize(5);
        hikariConfig.setMinimumIdle(1);
        hikariConfig.setPoolName("nexus-jdbc-sink");

        this.dataSource = new HikariDataSource(hikariConfig);
        this.insertMode = InsertMode.fromString(config.insertMode());
        this.tableCreated = false;

        log.info("JdbcSinkAdapter started: url={}, table={}, mode={}",
                config.url(), config.table(), insertMode);
    }

    @Override
    public void write(List<TransformedRecord> records) {
        if (records.isEmpty()) {
            return;
        }

        try {
            JsonNode firstRecord = objectMapper.readTree(
                    new String(records.get(0).value(), StandardCharsets.UTF_8));

            List<String> columns = new ArrayList<>();
            Iterator<String> fieldNames = firstRecord.fieldNames();
            while (fieldNames.hasNext()) {
                columns.add(fieldNames.next());
            }

            if (config.autoDdl() && !tableCreated) {
                createTableIfNeeded(columns, firstRecord);
                tableCreated = true;
            }

            String sql = buildSql(columns);
            log.debug("Writing {} records with SQL: {}", records.size(), sql);

            try (Connection conn = dataSource.getConnection();
                 PreparedStatement ps = conn.prepareStatement(sql)) {

                conn.setAutoCommit(false);

                for (TransformedRecord record : records) {
                    JsonNode node = objectMapper.readTree(
                            new String(record.value(), StandardCharsets.UTF_8));
                    setParameters(ps, columns, node);
                    ps.addBatch();
                }

                ps.executeBatch();
                conn.commit();
            }

            log.debug("Wrote {} records to table {}", records.size(), config.table());
        } catch (Exception e) {
            throw new RetryableException("Failed to write to JDBC sink: " + e.getMessage(), e);
        }
    }

    private String buildSql(List<String> columns) {
        return switch (insertMode) {
            case INSERT -> sqlGenerator.generateInsert(config.table(), columns);
            case UPSERT -> sqlGenerator.generateUpsert(config.table(), columns, config.pkColumns());
            case UPDATE -> sqlGenerator.generateUpdate(config.table(), columns, config.pkColumns());
        };
    }

    private void setParameters(PreparedStatement ps, List<String> columns, JsonNode node) throws Exception {
        if (insertMode == InsertMode.UPDATE) {
            // For UPDATE: SET columns first, then WHERE pk columns
            List<String> setCols = columns.stream()
                    .filter(c -> !config.pkColumns().contains(c))
                    .toList();
            int idx = 1;
            for (String col : setCols) {
                setParameter(ps, idx++, node.get(col));
            }
            for (String pkCol : config.pkColumns()) {
                setParameter(ps, idx++, node.get(pkCol));
            }
        } else {
            for (int i = 0; i < columns.size(); i++) {
                setParameter(ps, i + 1, node.get(columns.get(i)));
            }
        }
    }

    private void setParameter(PreparedStatement ps, int idx, JsonNode value) throws Exception {
        if (value == null || value.isNull()) {
            ps.setNull(idx, java.sql.Types.VARCHAR);
        } else if (value.isInt()) {
            ps.setInt(idx, value.intValue());
        } else if (value.isLong()) {
            ps.setLong(idx, value.longValue());
        } else if (value.isDouble() || value.isFloat()) {
            ps.setDouble(idx, value.doubleValue());
        } else if (value.isBoolean()) {
            ps.setBoolean(idx, value.booleanValue());
        } else {
            ps.setString(idx, value.asText());
        }
    }

    private void createTableIfNeeded(List<String> columns, JsonNode sampleRecord) {
        List<String> columnTypes = new ArrayList<>();
        for (String col : columns) {
            JsonNode value = sampleRecord.get(col);
            columnTypes.add(inferSqlType(value));
        }

        String ddl = sqlGenerator.generateCreateTable(
                config.table(), columns, columnTypes, config.pkColumns());
        log.info("Auto-DDL: {}", ddl);

        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute(ddl);
        } catch (Exception e) {
            log.warn("Auto-DDL failed (table may already exist): {}", e.getMessage());
        }
    }

    private String inferSqlType(JsonNode value) {
        if (value == null || value.isNull()) {
            return "TEXT";
        }
        if (value.isInt()) {
            return "INTEGER";
        }
        if (value.isLong()) {
            return "BIGINT";
        }
        if (value.isDouble() || value.isFloat()) {
            return "DOUBLE PRECISION";
        }
        if (value.isBoolean()) {
            return "BOOLEAN";
        }
        return "TEXT";
    }

    @Override
    public void flush() {
        // Writes are committed in batch within write()
    }

    @Override
    public boolean isHealthy() {
        return dataSource != null && !dataSource.isClosed();
    }

    @Override
    public void stop() {
        if (dataSource != null) {
            dataSource.close();
        }
        log.info("JdbcSinkAdapter stopped");
    }
}
