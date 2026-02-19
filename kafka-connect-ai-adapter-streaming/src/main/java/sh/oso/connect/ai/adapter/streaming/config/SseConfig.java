package sh.oso.connect.ai.adapter.streaming.config;

import sh.oso.connect.ai.api.config.AiConnectConfig;

import java.util.Map;

public class SseConfig {

    private final Map<String, String> props;

    public SseConfig(Map<String, String> props) {
        this.props = props;
    }

    public String url() {
        return props.getOrDefault(AiConnectConfig.SSE_URL, "");
    }

    public long reconnectMs() {
        return Long.parseLong(props.getOrDefault(AiConnectConfig.SSE_RECONNECT_MS, "3000"));
    }

    public int bufferCapacity() {
        return Integer.parseInt(props.getOrDefault(AiConnectConfig.STREAMING_BUFFER_CAPACITY, "10000"));
    }

    public long reconnectBackoffMs() {
        return Long.parseLong(props.getOrDefault(AiConnectConfig.STREAMING_RECONNECT_BACKOFF_MS, "1000"));
    }

    public long maxReconnectBackoffMs() {
        return Long.parseLong(props.getOrDefault(AiConnectConfig.STREAMING_MAX_RECONNECT_BACKOFF_MS, "60000"));
    }

    public Map<String, String> props() {
        return props;
    }
}
