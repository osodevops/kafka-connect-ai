package sh.oso.connect.ai.adapter.mongodb.config;

import sh.oso.connect.ai.api.config.AiConnectConfig;

import java.util.Map;

public class MongoSourceConfig {

    private final Map<String, String> props;

    public MongoSourceConfig(Map<String, String> props) {
        this.props = props;
    }

    public String connectionString() {
        return props.getOrDefault(AiConnectConfig.MONGODB_CONNECTION_STRING, "mongodb://localhost:27017");
    }

    public String database() {
        return props.get(AiConnectConfig.MONGODB_DATABASE);
    }

    public String collection() {
        return props.get(AiConnectConfig.MONGODB_COLLECTION);
    }

    public String pollMode() {
        return props.getOrDefault(AiConnectConfig.MONGODB_POLL_MODE, "change_stream");
    }

    public String pipeline() {
        return props.getOrDefault(AiConnectConfig.MONGODB_PIPELINE, "[]");
    }

    public long pollIntervalMs() {
        return Long.parseLong(props.getOrDefault(AiConnectConfig.MONGODB_POLL_INTERVAL_MS, "1000"));
    }
}
