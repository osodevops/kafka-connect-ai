package sh.oso.connect.ai.adapter.jdbc.config;

import sh.oso.connect.ai.api.config.AiConnectConfig;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class JdbcSinkConfig {

    private final Map<String, String> props;

    public JdbcSinkConfig(Map<String, String> props) {
        this.props = props;
    }

    public String url() {
        return props.getOrDefault(AiConnectConfig.JDBC_URL, "");
    }

    public String user() {
        return props.getOrDefault(AiConnectConfig.JDBC_USER, "");
    }

    public String password() {
        return props.getOrDefault(AiConnectConfig.JDBC_PASSWORD, "");
    }

    public String table() {
        return props.getOrDefault(AiConnectConfig.JDBC_TABLE, "");
    }

    public List<String> pkColumns() {
        String pk = props.getOrDefault(AiConnectConfig.JDBC_SINK_PK_COLUMNS, "");
        if (pk.isEmpty()) {
            return List.of();
        }
        return Arrays.asList(pk.split(","));
    }

    public String insertMode() {
        return props.getOrDefault(AiConnectConfig.JDBC_SINK_INSERT_MODE, "insert");
    }

    public boolean autoDdl() {
        return Boolean.parseBoolean(props.getOrDefault(AiConnectConfig.JDBC_SINK_AUTO_DDL, "false"));
    }

    public int batchSize() {
        return Integer.parseInt(props.getOrDefault(AiConnectConfig.JDBC_BATCH_SIZE, "1000"));
    }

    public String driverClass() {
        return props.getOrDefault(AiConnectConfig.JDBC_DRIVER_CLASS, "");
    }
}
