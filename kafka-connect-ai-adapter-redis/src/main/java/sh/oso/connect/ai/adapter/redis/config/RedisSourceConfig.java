package sh.oso.connect.ai.adapter.redis.config;

import sh.oso.connect.ai.api.config.AiConnectConfig;

import java.util.Map;

public class RedisSourceConfig {

    private final Map<String, String> props;

    public RedisSourceConfig(Map<String, String> props) {
        this.props = props;
    }

    public String url() {
        return props.getOrDefault(AiConnectConfig.REDIS_URL, "redis://localhost:6379");
    }

    public String mode() {
        return props.getOrDefault(AiConnectConfig.REDIS_SOURCE_MODE, "stream");
    }

    public String key() {
        return props.getOrDefault(AiConnectConfig.REDIS_KEY, "");
    }

    public String channel() {
        return props.getOrDefault(AiConnectConfig.REDIS_CHANNEL, "");
    }

    public String group() {
        return props.getOrDefault(AiConnectConfig.REDIS_GROUP, "connect-ai-group");
    }

    public String consumer() {
        return props.getOrDefault(AiConnectConfig.REDIS_CONSUMER, "connect-ai-consumer");
    }

    public long pollIntervalMs() {
        return Long.parseLong(props.getOrDefault(AiConnectConfig.REDIS_POLL_INTERVAL_MS, "1000"));
    }

    public int batchSize() {
        return Integer.parseInt(props.getOrDefault(AiConnectConfig.REDIS_BATCH_SIZE, "500"));
    }
}
