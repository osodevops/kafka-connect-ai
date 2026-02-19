package sh.oso.connect.ai.adapter.redis.config;

import sh.oso.connect.ai.api.config.AiConnectConfig;

import java.util.Map;

public class RedisSinkConfig {

    private final Map<String, String> props;

    public RedisSinkConfig(Map<String, String> props) {
        this.props = props;
    }

    public String url() {
        return props.getOrDefault(AiConnectConfig.REDIS_URL, "redis://localhost:6379");
    }

    public String mode() {
        return props.getOrDefault(AiConnectConfig.REDIS_SINK_MODE, "stream");
    }

    public String key() {
        return props.getOrDefault(AiConnectConfig.REDIS_KEY, "");
    }

    public String channel() {
        return props.getOrDefault(AiConnectConfig.REDIS_CHANNEL, "");
    }

    public int batchSize() {
        return Integer.parseInt(props.getOrDefault(AiConnectConfig.REDIS_BATCH_SIZE, "500"));
    }
}
