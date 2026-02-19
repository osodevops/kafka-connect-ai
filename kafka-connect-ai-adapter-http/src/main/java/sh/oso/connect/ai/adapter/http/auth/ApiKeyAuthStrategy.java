package sh.oso.connect.ai.adapter.http.auth;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.http.HttpRequest;
import java.util.Map;
import java.util.Objects;

/**
 * Applies API-key authentication by setting a configurable HTTP header
 * on each outbound request.
 */
public class ApiKeyAuthStrategy implements AuthStrategy {

    private static final Logger log = LoggerFactory.getLogger(ApiKeyAuthStrategy.class);

    static final String PROP_HEADER = "http.auth.apikey.header";
    static final String PROP_VALUE = "http.auth.apikey.value";
    static final String DEFAULT_HEADER = "X-API-Key";

    private String headerName;
    private String headerValue;

    @Override
    public void configure(Map<String, String> props) {
        this.headerName = props.getOrDefault(PROP_HEADER, DEFAULT_HEADER);
        this.headerValue = Objects.requireNonNull(
                props.get(PROP_VALUE), PROP_VALUE + " is required for API-key auth");

        log.info("API-key auth configured (header={})", headerName);
    }

    @Override
    public void apply(HttpRequest.Builder requestBuilder) {
        requestBuilder.header(headerName, headerValue);
    }

    @Override
    public String type() {
        return "apikey";
    }
}
