package sh.oso.connect.ai.adapter.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.connect.ai.adapter.cassandra.config.CassandraSinkConfig;
import sh.oso.connect.ai.api.adapter.SinkAdapter;
import sh.oso.connect.ai.api.config.AiConnectConfig;
import sh.oso.connect.ai.api.error.RetryableException;
import sh.oso.connect.ai.api.model.TransformedRecord;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

public class CassandraSinkAdapter implements SinkAdapter {

    private static final Logger log = LoggerFactory.getLogger(CassandraSinkAdapter.class);

    private CqlSession session;
    private CassandraSinkConfig config;
    private final ObjectMapper objectMapper = new ObjectMapper();

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
                        "Cassandra table to write to")
                .define(AiConnectConfig.CASSANDRA_USERNAME, ConfigDef.Type.STRING,
                        "", ConfigDef.Importance.MEDIUM,
                        "Cassandra authentication username")
                .define(AiConnectConfig.CASSANDRA_PASSWORD, ConfigDef.Type.PASSWORD,
                        "", ConfigDef.Importance.MEDIUM,
                        "Cassandra authentication password")
                .define(AiConnectConfig.CASSANDRA_CONSISTENCY_LEVEL, ConfigDef.Type.STRING,
                        "LOCAL_QUORUM", ConfigDef.Importance.MEDIUM,
                        "Cassandra write consistency level")
                .define(AiConnectConfig.CASSANDRA_BATCH_SIZE, ConfigDef.Type.INT,
                        1000, ConfigDef.Importance.MEDIUM,
                        "Maximum number of records per async batch write");
    }

    @Override
    public void start(Map<String, String> props) {
        this.config = new CassandraSinkConfig(props);
        this.session = buildSession(config);
        log.info("CassandraSinkAdapter started: keyspace={}, table={}, datacenter={}",
                config.keyspace(), config.table(), config.datacenter());
    }

    private CqlSession buildSession(CassandraSinkConfig config) {
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
    public void write(List<TransformedRecord> records) {
        if (records.isEmpty()) {
            return;
        }

        try {
            List<CompletionStage<AsyncResultSet>> futures = new ArrayList<>();

            for (int i = 0; i < records.size(); i += config.batchSize()) {
                int end = Math.min(i + config.batchSize(), records.size());
                List<TransformedRecord> batch = records.subList(i, end);

                for (TransformedRecord record : batch) {
                    String json = new String(record.value(), StandardCharsets.UTF_8);
                    Map<String, Object> fields = objectMapper.readValue(
                            json, new TypeReference<Map<String, Object>>() {});

                    SimpleStatement stmt = buildInsertStatement(fields);
                    futures.add(session.executeAsync(stmt));
                }
            }

            // Await all async writes
            CompletableFuture<?>[] cfArray = futures.stream()
                    .map(CompletionStage::toCompletableFuture)
                    .toArray(CompletableFuture[]::new);
            CompletableFuture.allOf(cfArray).join();

            log.debug("Wrote {} records to {}.{}", records.size(),
                    config.keyspace(), config.table());
        } catch (Exception e) {
            throw new RetryableException("Failed to write to Cassandra: " + e.getMessage(), e);
        }
    }

    private SimpleStatement buildInsertStatement(Map<String, Object> fields) {
        String columns = fields.keySet().stream()
                .collect(Collectors.joining(", "));
        String placeholders = fields.keySet().stream()
                .map(k -> "?")
                .collect(Collectors.joining(", "));

        String cql = String.format("INSERT INTO %s (%s) VALUES (%s)",
                config.table(), columns, placeholders);

        Object[] values = fields.values().toArray();
        return SimpleStatement.newInstance(cql, values);
    }

    @Override
    public void flush() {
        // Writes are committed asynchronously in write() and awaited via CompletableFuture.allOf()
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
        log.info("CassandraSinkAdapter stopped");
    }
}
