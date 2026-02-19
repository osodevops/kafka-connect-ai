package sh.oso.nexus.adapter.jdbc.config;

import sh.oso.nexus.api.config.NexusConfig;

import java.util.Map;

public class JdbcSourceConfig {

    private final Map<String, String> props;

    public JdbcSourceConfig(Map<String, String> props) {
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

    public String queryMode() {
        return props.getOrDefault(NexusConfig.JDBC_QUERY_MODE, "bulk");
    }

    public String timestampColumn() {
        return props.getOrDefault(NexusConfig.JDBC_TIMESTAMP_COLUMN, "updated_at");
    }

    public String incrementingColumn() {
        return props.getOrDefault(NexusConfig.JDBC_INCREMENTING_COLUMN, "id");
    }

    public long pollIntervalMs() {
        return Long.parseLong(props.getOrDefault(NexusConfig.JDBC_POLL_INTERVAL_MS, "5000"));
    }

    public String driverClass() {
        return props.getOrDefault(NexusConfig.JDBC_DRIVER_CLASS, "");
    }

    public String query() {
        return props.getOrDefault(NexusConfig.JDBC_QUERY, "");
    }

    public String tables() {
        return props.getOrDefault(NexusConfig.JDBC_TABLES, "");
    }
}
