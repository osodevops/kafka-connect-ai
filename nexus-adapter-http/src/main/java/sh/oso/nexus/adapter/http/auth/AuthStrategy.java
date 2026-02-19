package sh.oso.nexus.adapter.http.auth;

import java.net.http.HttpRequest;
import java.util.Map;

/**
 * Strategy interface for applying authentication to outbound HTTP requests.
 *
 * <p>Implementations handle a specific authentication scheme (Basic, Bearer, OAuth2, etc.)
 * and are configured via adapter properties before being applied to each request.</p>
 */
public interface AuthStrategy {

    /**
     * Configures this strategy from the adapter's property map.
     *
     * @param props the adapter configuration properties
     */
    void configure(Map<String, String> props);

    /**
     * Applies authentication headers (or other credentials) to the given request builder.
     *
     * @param requestBuilder the in-flight request builder to decorate
     */
    void apply(HttpRequest.Builder requestBuilder);

    /**
     * Returns the canonical type identifier for this strategy (e.g. "basic", "bearer").
     *
     * @return a non-null, lowercase type string
     */
    String type();
}
