package sh.oso.connect.ai.adapter.mongodb.config;

import sh.oso.connect.ai.api.config.AiConnectConfig;

import java.util.Map;

public class MongoSinkConfig {

    private final Map<String, String> props;

    public MongoSinkConfig(Map<String, String> props) {
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

    public String writeMode() {
        return props.getOrDefault(AiConnectConfig.MONGODB_WRITE_MODE, "insert");
    }

    public int batchSize() {
        return Integer.parseInt(props.getOrDefault(AiConnectConfig.MONGODB_BATCH_SIZE, "1000"));
    }
}
