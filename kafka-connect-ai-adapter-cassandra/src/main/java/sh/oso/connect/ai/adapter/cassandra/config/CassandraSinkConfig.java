package sh.oso.connect.ai.adapter.cassandra.config;

import sh.oso.connect.ai.api.config.AiConnectConfig;

import java.util.Map;

public class CassandraSinkConfig {

    private final Map<String, String> props;

    public CassandraSinkConfig(Map<String, String> props) {
        this.props = props;
    }

    public String contactPoints() {
        return props.getOrDefault(AiConnectConfig.CASSANDRA_CONTACT_POINTS, "localhost");
    }

    public int port() {
        return Integer.parseInt(props.getOrDefault(AiConnectConfig.CASSANDRA_PORT, "9042"));
    }

    public String datacenter() {
        return props.getOrDefault(AiConnectConfig.CASSANDRA_DATACENTER, "datacenter1");
    }

    public String keyspace() {
        return props.get(AiConnectConfig.CASSANDRA_KEYSPACE);
    }

    public String table() {
        return props.get(AiConnectConfig.CASSANDRA_TABLE);
    }

    public String username() {
        return props.get(AiConnectConfig.CASSANDRA_USERNAME);
    }

    public String password() {
        return props.get(AiConnectConfig.CASSANDRA_PASSWORD);
    }

    public String consistencyLevel() {
        return props.getOrDefault(AiConnectConfig.CASSANDRA_CONSISTENCY_LEVEL, "LOCAL_QUORUM");
    }

    public int batchSize() {
        return Integer.parseInt(props.getOrDefault(AiConnectConfig.CASSANDRA_BATCH_SIZE, "1000"));
    }
}
