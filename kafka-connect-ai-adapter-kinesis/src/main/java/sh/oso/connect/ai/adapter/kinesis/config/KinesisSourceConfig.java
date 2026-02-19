package sh.oso.connect.ai.adapter.kinesis.config;

import sh.oso.connect.ai.api.config.AiConnectConfig;

import java.util.Map;

public class KinesisSourceConfig {

    private final Map<String, String> props;

    public KinesisSourceConfig(Map<String, String> props) {
        this.props = props;
    }

    public String streamName() {
        return props.get(AiConnectConfig.KINESIS_STREAM_NAME);
    }

    public String region() {
        return props.getOrDefault(AiConnectConfig.KINESIS_REGION, "us-east-1");
    }

    public String iteratorType() {
        return props.getOrDefault(AiConnectConfig.KINESIS_ITERATOR_TYPE, "TRIM_HORIZON");
    }

    public long pollIntervalMs() {
        return Long.parseLong(props.getOrDefault(AiConnectConfig.KINESIS_POLL_INTERVAL_MS, "1000"));
    }

    public int batchSize() {
        return Integer.parseInt(props.getOrDefault(AiConnectConfig.KINESIS_BATCH_SIZE, "500"));
    }
}
