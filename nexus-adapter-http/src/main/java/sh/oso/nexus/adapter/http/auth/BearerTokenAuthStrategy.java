package sh.oso.nexus.adapter.http.auth;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.http.HttpRequest;
import java.util.Map;
import java.util.Objects;

/**
 * Applies a static Bearer token to outbound HTTP requests via the
 * {@code Authorization: Bearer <token>} header.
 */
public class BearerTokenAuthStrategy implements AuthStrategy {

    private static final Logger log = LoggerFactory.getLogger(BearerTokenAuthStrategy.class);

    static final String PROP_TOKEN = "http.auth.bearer.token";

    private String token;

    @Override
    public void configure(Map<String, String> props) {
        this.token = Objects.requireNonNull(
                props.get(PROP_TOKEN), PROP_TOKEN + " is required for Bearer auth");

        log.info("Bearer token auth configured");
    }

    @Override
    public void apply(HttpRequest.Builder requestBuilder) {
        requestBuilder.header("Authorization", "Bearer " + token);
    }

    @Override
    public String type() {
        return "bearer";
    }
}
