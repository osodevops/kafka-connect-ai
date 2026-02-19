package sh.oso.connect.ai.adapter.jdbc.config;

import sh.oso.connect.ai.api.config.AiConnectConfig;

import java.util.Map;

public class JdbcSourceConfig {

    private final Map<String, String> props;

    public JdbcSourceConfig(Map<String, String> props) {
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

    public String queryMode() {
        return props.getOrDefault(AiConnectConfig.JDBC_QUERY_MODE, "bulk");
    }

    public String timestampColumn() {
        return props.getOrDefault(AiConnectConfig.JDBC_TIMESTAMP_COLUMN, "updated_at");
    }

    public String incrementingColumn() {
        return props.getOrDefault(AiConnectConfig.JDBC_INCREMENTING_COLUMN, "id");
    }

    public long pollIntervalMs() {
        return Long.parseLong(props.getOrDefault(AiConnectConfig.JDBC_POLL_INTERVAL_MS, "5000"));
    }

    public String driverClass() {
        return props.getOrDefault(AiConnectConfig.JDBC_DRIVER_CLASS, "");
    }

    public String query() {
        return props.getOrDefault(AiConnectConfig.JDBC_QUERY, "");
    }

    public String tables() {
        return props.getOrDefault(AiConnectConfig.JDBC_TABLES, "");
    }
}
