package sh.oso.connect.ai.adapter.http.auth;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.http.HttpRequest;
import java.util.Map;

/**
 * A no-op authentication strategy for endpoints that require no credentials.
 */
public class NoAuthStrategy implements AuthStrategy {

    private static final Logger log = LoggerFactory.getLogger(NoAuthStrategy.class);

    @Override
    public void configure(Map<String, String> props) {
        log.debug("No authentication configured");
    }

    @Override
    public void apply(HttpRequest.Builder requestBuilder) {
        // intentionally empty -- no credentials to apply
    }

    @Override
    public String type() {
        return "none";
    }
}
