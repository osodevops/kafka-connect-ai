package sh.oso.connect.ai.adapter.http.config;

import java.util.Map;

public class HttpSourceConfig {

    public static final String URL = "http.source.url";
    public static final String METHOD = "http.source.method";
    public static final String HEADERS = "http.source.headers";
    public static final String POLL_INTERVAL_MS = "http.source.poll.interval.ms";
    public static final String RESPONSE_CONTENT_PATH = "http.source.response.content.path";
    public static final String AUTH_TYPE = "http.auth.type";
    public static final String PAGINATION_TYPE = "http.pagination.type";
    public static final String RATE_LIMIT_RPS = "http.rate.limit.rps";
    public static final String TIMEOUT_MS = "http.source.timeout.ms";

    private final Map<String, String> props;

    public HttpSourceConfig(Map<String, String> props) {
        this.props = props;
    }

    public String url() {
        return props.getOrDefault(URL, "");
    }

    public String method() {
        return props.getOrDefault(METHOD, "GET");
    }

    public String headers() {
        return props.getOrDefault(HEADERS, "");
    }

    public long pollIntervalMs() {
        return Long.parseLong(props.getOrDefault(POLL_INTERVAL_MS, "60000"));
    }

    public String responseContentPath() {
        return props.getOrDefault(RESPONSE_CONTENT_PATH, "");
    }

    public String authType() {
        return props.getOrDefault(AUTH_TYPE, "none");
    }

    public String paginationType() {
        return props.getOrDefault(PAGINATION_TYPE, "none");
    }

    public double rateLimitRps() {
        return Double.parseDouble(props.getOrDefault(RATE_LIMIT_RPS, "10.0"));
    }

    public long timeoutMs() {
        return Long.parseLong(props.getOrDefault(TIMEOUT_MS, "30000"));
    }

    public Map<String, String> props() {
        return props;
    }
}
