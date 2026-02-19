package sh.oso.nexus.adapter.jdbc;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.nexus.adapter.jdbc.config.JdbcSourceConfig;
import sh.oso.nexus.adapter.jdbc.query.QueryBuilder;
import sh.oso.nexus.adapter.jdbc.query.QueryMode;
import sh.oso.nexus.api.adapter.SourceAdapter;
import sh.oso.nexus.api.config.NexusConfig;
import sh.oso.nexus.api.error.RetryableException;
import sh.oso.nexus.api.model.RawRecord;
import sh.oso.nexus.api.model.SourceOffset;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;

public class JdbcSourceAdapter implements SourceAdapter {

    private static final Logger log = LoggerFactory.getLogger(JdbcSourceAdapter.class);

    private final ResultSetSerializer serializer = new ResultSetSerializer();

    private HikariDataSource dataSource;
    private JdbcSourceConfig config;
    private QueryBuilder queryBuilder;
    private long lastPollTimestamp;

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
                .define(NexusConfig.JDBC_TABLE, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, "Source table name")
                .define(NexusConfig.JDBC_QUERY_MODE, ConfigDef.Type.STRING, "bulk", ConfigDef.Importance.MEDIUM, "Query mode: bulk, timestamp, incrementing, timestamp+incrementing")
                .define(NexusConfig.JDBC_TIMESTAMP_COLUMN, ConfigDef.Type.STRING, "updated_at", ConfigDef.Importance.MEDIUM, "Timestamp column name")
                .define(NexusConfig.JDBC_INCREMENTING_COLUMN, ConfigDef.Type.STRING, "id", ConfigDef.Importance.MEDIUM, "Incrementing column name")
                .define(NexusConfig.JDBC_POLL_INTERVAL_MS, ConfigDef.Type.LONG, 5000L, ConfigDef.Importance.MEDIUM, "Poll interval in ms")
                .define(NexusConfig.JDBC_DRIVER_CLASS, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM, "JDBC driver class name")
                .define(NexusConfig.JDBC_QUERY, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM, "Custom SQL query (overrides table)")
                .define(NexusConfig.JDBC_TABLES, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM, "Comma-separated list of tables");
    }

    @Override
    public void start(Map<String, String> props) {
        this.config = new JdbcSourceConfig(props);

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
        hikariConfig.setMaximumPoolSize(2);
        hikariConfig.setMinimumIdle(1);
        hikariConfig.setPoolName("nexus-jdbc-source");

        this.dataSource = new HikariDataSource(hikariConfig);

        QueryMode mode = QueryMode.fromString(config.queryMode());
        this.queryBuilder = new QueryBuilder(config.table(), mode,
                config.timestampColumn(), config.incrementingColumn());
        this.lastPollTimestamp = 0;

        log.info("JdbcSourceAdapter started: url={}, table={}, mode={}",
                config.url(), config.table(), mode);
    }

    @Override
    public List<RawRecord> fetch(SourceOffset currentOffset, int maxRecords) throws InterruptedException {
        long now = System.currentTimeMillis();
        long elapsed = now - lastPollTimestamp;
        if (elapsed < config.pollIntervalMs()) {
            Thread.sleep(config.pollIntervalMs() - elapsed);
        }
        lastPollTimestamp = System.currentTimeMillis();

        String sql = queryBuilder.buildQuery(currentOffset, maxRecords);
        log.debug("Executing query: {}", sql);

        List<RawRecord> records = new ArrayList<>();

        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                byte[] value = serializer.serializeRow(rs);
                SourceOffset rowOffset = buildRowOffset(rs);
                records.add(new RawRecord(null, value,
                        Map.of("source.table", config.table()), rowOffset));
            }
        } catch (Exception e) {
            throw new RetryableException("Failed to fetch from JDBC source: " + e.getMessage(), e);
        }

        log.debug("Fetched {} records from table {}", records.size(), config.table());
        return records;
    }

    private SourceOffset buildRowOffset(ResultSet rs) {
        Map<String, String> partition = Map.of("adapter", "jdbc", "table", config.table());
        Map<String, Object> offset = new HashMap<>();

        try {
            QueryMode mode = queryBuilder.getMode();
            if (mode == QueryMode.TIMESTAMP || mode == QueryMode.TIMESTAMP_INCREMENTING) {
                Object ts = rs.getObject(queryBuilder.getTimestampColumn());
                if (ts != null) {
                    offset.put("timestamp", ts.toString());
                }
            }
            if (mode == QueryMode.INCREMENTING || mode == QueryMode.TIMESTAMP_INCREMENTING) {
                Object id = rs.getObject(queryBuilder.getIncrementingColumn());
                if (id != null) {
                    offset.put("incrementing", id);
                }
            }
        } catch (Exception e) {
            log.warn("Failed to extract offset columns: {}", e.getMessage());
        }

        return new SourceOffset(partition, offset);
    }

    @Override
    public void commitOffset(SourceOffset offset) {
        // JDBC source tracks offset via query WHERE clause
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
        log.info("JdbcSourceAdapter stopped");
    }
}
