package sh.oso.connect.ai.adapter.kinesis.config;

import sh.oso.connect.ai.api.config.AiConnectConfig;

import java.util.Map;

public class DynamoDbStreamConfig {

    private final Map<String, String> props;

    public DynamoDbStreamConfig(Map<String, String> props) {
        this.props = props;
    }

    public String tableName() {
        return props.get(AiConnectConfig.DYNAMODB_TABLE_NAME);
    }

    public String region() {
        return props.getOrDefault(AiConnectConfig.DYNAMODB_REGION, "us-east-1");
    }

    public long pollIntervalMs() {
        return Long.parseLong(props.getOrDefault(AiConnectConfig.DYNAMODB_POLL_INTERVAL_MS, "1000"));
    }
}
