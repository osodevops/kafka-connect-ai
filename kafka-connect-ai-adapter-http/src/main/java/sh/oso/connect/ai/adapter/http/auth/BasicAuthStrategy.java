package sh.oso.connect.ai.adapter.http.auth;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.http.HttpRequest;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.Objects;

/**
 * Applies HTTP Basic authentication by encoding the username and password
 * as a Base64 {@code Authorization} header.
 */
public class BasicAuthStrategy implements AuthStrategy {

    private static final Logger log = LoggerFactory.getLogger(BasicAuthStrategy.class);

    static final String PROP_USERNAME = "http.auth.basic.username";
    static final String PROP_PASSWORD = "http.auth.basic.password";

    private String encodedCredentials;

    @Override
    public void configure(Map<String, String> props) {
        String username = Objects.requireNonNull(
                props.get(PROP_USERNAME), PROP_USERNAME + " is required for Basic auth");
        String password = Objects.requireNonNull(
                props.get(PROP_PASSWORD), PROP_PASSWORD + " is required for Basic auth");

        this.encodedCredentials = Base64.getEncoder()
                .encodeToString((username + ":" + password).getBytes(StandardCharsets.UTF_8));

        log.info("Basic auth configured for user '{}'", username);
    }

    @Override
    public void apply(HttpRequest.Builder requestBuilder) {
        requestBuilder.header("Authorization", "Basic " + encodedCredentials);
    }

    @Override
    public String type() {
        return "basic";
    }
}
