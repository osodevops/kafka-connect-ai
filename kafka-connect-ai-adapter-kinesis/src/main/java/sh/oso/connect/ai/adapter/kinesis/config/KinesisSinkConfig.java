package sh.oso.connect.ai.adapter.kinesis.config;

import sh.oso.connect.ai.api.config.AiConnectConfig;

import java.util.Map;

public class KinesisSinkConfig {

    private final Map<String, String> props;

    public KinesisSinkConfig(Map<String, String> props) {
        this.props = props;
    }

    public String streamName() {
        return props.get(AiConnectConfig.KINESIS_STREAM_NAME);
    }

    public String region() {
        return props.getOrDefault(AiConnectConfig.KINESIS_REGION, "us-east-1");
    }

    public String partitionKey() {
        return props.getOrDefault(AiConnectConfig.KINESIS_PARTITION_KEY, "default");
    }

    public int batchSize() {
        return Integer.parseInt(props.getOrDefault(AiConnectConfig.KINESIS_BATCH_SIZE, "500"));
    }
}
