package sh.oso.connect.ai.adapter.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Node;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.connect.ai.adapter.cassandra.config.CassandraSourceConfig;
import sh.oso.connect.ai.api.adapter.SourceAdapter;
import sh.oso.connect.ai.api.config.AiConnectConfig;
import sh.oso.connect.ai.api.error.RetryableException;
import sh.oso.connect.ai.api.model.RawRecord;
import sh.oso.connect.ai.api.model.SourceOffset;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CassandraSourceAdapter implements SourceAdapter {

    private static final Logger log = LoggerFactory.getLogger(CassandraSourceAdapter.class);

    private CqlSession session;
    private CassandraSourceConfig config;
    private long lastPollTimestamp;

    @Override
    public String type() {
        return "cassandra";
    }

    @Override
    public ConfigDef configDef() {
        return new ConfigDef()
                .define(AiConnectConfig.CASSANDRA_CONTACT_POINTS, ConfigDef.Type.STRING,
                        "localhost", ConfigDef.Importance.HIGH,
                        "Comma-separated list of Cassandra contact points")
                .define(AiConnectConfig.CASSANDRA_PORT, ConfigDef.Type.INT,
                        9042, ConfigDef.Importance.HIGH,
                        "Cassandra native transport port")
                .define(AiConnectConfig.CASSANDRA_DATACENTER, ConfigDef.Type.STRING,
                        "datacenter1", ConfigDef.Importance.HIGH,
                        "Cassandra local datacenter name")
                .define(AiConnectConfig.CASSANDRA_KEYSPACE, ConfigDef.Type.STRING,
                        ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH,
                        "Cassandra keyspace")
                .define(AiConnectConfig.CASSANDRA_TABLE, ConfigDef.Type.STRING,
                        ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH,
                        "Cassandra table to read from")
                .define(AiConnectConfig.CASSANDRA_USERNAME, ConfigDef.Type.STRING,
                        "", ConfigDef.Importance.MEDIUM,
                        "Cassandra authentication username")
                .define(AiConnectConfig.CASSANDRA_PASSWORD, ConfigDef.Type.PASSWORD,
                        "", ConfigDef.Importance.MEDIUM,
                        "Cassandra authentication password")
                .define(AiConnectConfig.CASSANDRA_CONSISTENCY_LEVEL, ConfigDef.Type.STRING,
                        "LOCAL_QUORUM", ConfigDef.Importance.MEDIUM,
                        "Cassandra read consistency level")
                .define(AiConnectConfig.CASSANDRA_TIMESTAMP_COLUMN, ConfigDef.Type.STRING,
                        "updated_at", ConfigDef.Importance.MEDIUM,
                        "Timestamp column used as watermark for incremental polling")
                .define(AiConnectConfig.CASSANDRA_POLL_INTERVAL_MS, ConfigDef.Type.LONG,
                        1000L, ConfigDef.Importance.MEDIUM,
                        "Poll interval in milliseconds")
                .define(AiConnectConfig.CASSANDRA_BATCH_SIZE, ConfigDef.Type.INT,
                        1000, ConfigDef.Importance.MEDIUM,
                        "Maximum number of records to fetch per poll");
    }

    @Override
    public void start(Map<String, String> props) {
        this.config = new CassandraSourceConfig(props);
        this.session = buildSession(config);
        this.lastPollTimestamp = 0;
        log.info("CassandraSourceAdapter started: keyspace={}, table={}, datacenter={}",
                config.keyspace(), config.table(), config.datacenter());
    }

    private CqlSession buildSession(CassandraSourceConfig config) {
        CqlSessionBuilder builder = CqlSession.builder();

        for (String contactPoint : config.contactPoints().split(",")) {
            builder.addContactPoint(new InetSocketAddress(contactPoint.trim(), config.port()));
        }

        builder.withLocalDatacenter(config.datacenter());

        if (config.keyspace() != null && !config.keyspace().isEmpty()) {
            builder.withKeyspace(config.keyspace());
        }

        String username = config.username();
        String password = config.password();
        if (username != null && !username.isEmpty()) {
            builder.withAuthCredentials(username, password);
        }

        return builder.build();
    }

    @Override
    public List<RawRecord> fetch(SourceOffset currentOffset, int maxRecords) throws InterruptedException {
        try {
            long now = System.currentTimeMillis();
            long elapsed = now - lastPollTimestamp;
            if (elapsed < config.pollIntervalMs()) {
                Thread.sleep(config.pollIntervalMs() - elapsed);
            }
            lastPollTimestamp = System.currentTimeMillis();

            return fetchWithTimestampWatermark(currentOffset, maxRecords);
        } catch (Exception e) {
            if (e instanceof InterruptedException ie) {
                throw ie;
            }
            throw new RetryableException("Failed to fetch from Cassandra: " + e.getMessage(), e);
        }
    }

    private List<RawRecord> fetchWithTimestampWatermark(SourceOffset currentOffset, int maxRecords) {
        String timestampCol = config.timestampColumn();
        String lastWriteTime = (String) currentOffset.offset().get("lastWriteTime");

        int fetchSize = Math.min(maxRecords, config.batchSize());

        SimpleStatement statement;
        if (lastWriteTime != null && !lastWriteTime.isEmpty()) {
            Instant watermark = Instant.parse(lastWriteTime);
            String cql = String.format(
                    "SELECT * FROM %s WHERE %s > ? LIMIT ? ALLOW FILTERING",
                    config.table(), timestampCol);
            statement = SimpleStatement.newInstance(cql, watermark, fetchSize);
        } else {
            String cql = String.format(
                    "SELECT * FROM %s LIMIT ?",
                    config.table());
            statement = SimpleStatement.newInstance(cql, fetchSize);
        }

        statement = statement.setPageSize(fetchSize);

        ResultSet rs = session.execute(statement);

        List<RawRecord> records = new ArrayList<>();
        String latestTimestamp = lastWriteTime;

        for (Row row : rs) {
            if (records.size() >= maxRecords) {
                break;
            }

            String rowJson = rowToJson(row);
            byte[] value = rowJson.getBytes(StandardCharsets.UTF_8);
            byte[] key = extractKey(row);

            Instant rowTimestamp = extractTimestamp(row, timestampCol);
            if (rowTimestamp != null) {
                String ts = rowTimestamp.toString();
                if (latestTimestamp == null || ts.compareTo(latestTimestamp) > 0) {
                    latestTimestamp = ts;
                }
            }

            Map<String, String> partition = Map.of(
                    "adapter", "cassandra",
                    "keyspace", config.keyspace(),
                    "table", config.table());
            Map<String, Object> offset = new HashMap<>();
            if (latestTimestamp != null) {
                offset.put("lastWriteTime", latestTimestamp);
            }

            Map<String, String> metadata = Map.of(
                    "source.keyspace", config.keyspace(),
                    "source.table", config.table());

            records.add(new RawRecord(key, value, metadata, new SourceOffset(partition, offset)));
        }

        log.debug("Fetched {} records from {}.{}", records.size(), config.keyspace(), config.table());
        return records;
    }

    private String rowToJson(Row row) {
        StringBuilder sb = new StringBuilder("{");
        var columnDefs = row.getColumnDefinitions();
        for (int i = 0; i < columnDefs.size(); i++) {
            if (i > 0) {
                sb.append(",");
            }
            String name = columnDefs.get(i).getName().asCql(true);
            Object val = row.getObject(i);
            sb.append("\"").append(name).append("\":");
            if (val == null) {
                sb.append("null");
            } else if (val instanceof String s) {
                sb.append("\"").append(escapeJson(s)).append("\"");
            } else if (val instanceof Instant inst) {
                sb.append("\"").append(inst.toString()).append("\"");
            } else {
                sb.append(val);
            }
        }
        sb.append("}");
        return sb.toString();
    }

    private String escapeJson(String s) {
        return s.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
    }

    private byte[] extractKey(Row row) {
        try {
            var columnDefs = row.getColumnDefinitions();
            if (columnDefs.contains("id")) {
                Object id = row.getObject(columnDefs.firstIndexOf("id"));
                if (id != null) {
                    return id.toString().getBytes(StandardCharsets.UTF_8);
                }
            }
            // Fall back to first column as key
            Object firstCol = row.getObject(0);
            return firstCol != null ? firstCol.toString().getBytes(StandardCharsets.UTF_8) : null;
        } catch (Exception e) {
            return null;
        }
    }

    private Instant extractTimestamp(Row row, String timestampCol) {
        try {
            var columnDefs = row.getColumnDefinitions();
            if (columnDefs.contains(timestampCol)) {
                return row.getInstant(columnDefs.firstIndexOf(timestampCol));
            }
        } catch (Exception e) {
            log.trace("Could not extract timestamp from column {}: {}", timestampCol, e.getMessage());
        }
        return null;
    }

    @Override
    public void commitOffset(SourceOffset offset) {
        // Offset is tracked via the lastWriteTime watermark in the offset map
    }

    @Override
    public boolean isHealthy() {
        if (session == null || session.isClosed()) {
            return false;
        }
        try {
            Map<java.util.UUID, Node> nodes = session.getMetadata().getNodes();
            return !nodes.isEmpty();
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public void stop() {
        if (session != null && !session.isClosed()) {
            session.close();
        }
        log.info("CassandraSourceAdapter stopped");
    }
}
