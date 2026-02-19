package sh.oso.connect.ai.adapter.http.config;

import java.util.Map;

public class HttpSinkConfig {

    public static final String URL = "http.sink.url";
    public static final String METHOD = "http.sink.method";
    public static final String HEADERS = "http.sink.headers";
    public static final String AUTH_TYPE = "http.auth.type";
    public static final String RATE_LIMIT_RPS = "http.rate.limit.rps";
    public static final String TIMEOUT_MS = "http.sink.timeout.ms";
    public static final String BATCH_SIZE = "http.sink.batch.size";
    public static final String RETRY_MAX = "http.sink.retry.max";

    private final Map<String, String> props;

    public HttpSinkConfig(Map<String, String> props) {
        this.props = props;
    }

    public String url() {
        return props.getOrDefault(URL, "");
    }

    public String method() {
        return props.getOrDefault(METHOD, "POST");
    }

    public String headers() {
        return props.getOrDefault(HEADERS, "");
    }

    public String authType() {
        return props.getOrDefault(AUTH_TYPE, "none");
    }

    public double rateLimitRps() {
        return Double.parseDouble(props.getOrDefault(RATE_LIMIT_RPS, "10.0"));
    }

    public long timeoutMs() {
        return Long.parseLong(props.getOrDefault(TIMEOUT_MS, "30000"));
    }

    public int batchSize() {
        return Integer.parseInt(props.getOrDefault(BATCH_SIZE, "1"));
    }

    public int retryMax() {
        return Integer.parseInt(props.getOrDefault(RETRY_MAX, "3"));
    }

    public Map<String, String> props() {
        return props;
    }
}
