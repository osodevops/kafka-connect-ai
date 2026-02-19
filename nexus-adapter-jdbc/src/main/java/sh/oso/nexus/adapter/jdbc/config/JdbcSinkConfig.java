package sh.oso.nexus.adapter.jdbc.config;

import sh.oso.nexus.api.config.NexusConfig;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class JdbcSinkConfig {

    private final Map<String, String> props;

    public JdbcSinkConfig(Map<String, String> props) {
        this.props = props;
    }

    public String url() {
        return props.getOrDefault(NexusConfig.JDBC_URL, "");
    }

    public String user() {
        return props.getOrDefault(NexusConfig.JDBC_USER, "");
    }

    public String password() {
        return props.getOrDefault(NexusConfig.JDBC_PASSWORD, "");
    }

    public String table() {
        return props.getOrDefault(NexusConfig.JDBC_TABLE, "");
    }

    public List<String> pkColumns() {
        String pk = props.getOrDefault(NexusConfig.JDBC_SINK_PK_COLUMNS, "");
        if (pk.isEmpty()) {
            return List.of();
        }
        return Arrays.asList(pk.split(","));
    }

    public String insertMode() {
        return props.getOrDefault(NexusConfig.JDBC_SINK_INSERT_MODE, "insert");
    }

    public boolean autoDdl() {
        return Boolean.parseBoolean(props.getOrDefault(NexusConfig.JDBC_SINK_AUTO_DDL, "false"));
    }

    public int batchSize() {
        return Integer.parseInt(props.getOrDefault(NexusConfig.JDBC_BATCH_SIZE, "1000"));
    }

    public String driverClass() {
        return props.getOrDefault(NexusConfig.JDBC_DRIVER_CLASS, "");
    }
}
