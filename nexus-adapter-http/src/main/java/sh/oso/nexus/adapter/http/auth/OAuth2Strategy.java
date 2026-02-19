package sh.oso.nexus.adapter.http.auth;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Implements the OAuth 2.0 client-credentials grant flow.
 *
 * <p>Fetches an access token from the configured token endpoint using the
 * {@code client_credentials} grant type and caches it until shortly before
 * expiry. Token refresh is thread-safe.</p>
 */
public class OAuth2Strategy implements AuthStrategy {

    private static final Logger log = LoggerFactory.getLogger(OAuth2Strategy.class);

    static final String PROP_TOKEN_URL = "http.auth.oauth2.token.url";
    static final String PROP_CLIENT_ID = "http.auth.oauth2.client.id";
    static final String PROP_CLIENT_SECRET = "http.auth.oauth2.client.secret";
    static final String PROP_SCOPE = "http.auth.oauth2.scope";

    /** Refresh the token this many seconds before it actually expires. */
    private static final long EXPIRY_BUFFER_SECONDS = 30;
    private static final Duration HTTP_TIMEOUT = Duration.ofSeconds(30);

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ReentrantLock tokenLock = new ReentrantLock();

    private HttpClient httpClient;
    private String tokenUrl;
    private String clientId;
    private String clientSecret;
    private String scope;

    private volatile String accessToken;
    private volatile Instant tokenExpiry = Instant.MIN;

    @Override
    public void configure(Map<String, String> props) {
        this.tokenUrl = Objects.requireNonNull(
                props.get(PROP_TOKEN_URL), PROP_TOKEN_URL + " is required for OAuth2 auth");
        this.clientId = Objects.requireNonNull(
                props.get(PROP_CLIENT_ID), PROP_CLIENT_ID + " is required for OAuth2 auth");
        this.clientSecret = Objects.requireNonNull(
                props.get(PROP_CLIENT_SECRET), PROP_CLIENT_SECRET + " is required for OAuth2 auth");
        this.scope = props.getOrDefault(PROP_SCOPE, "");

        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(HTTP_TIMEOUT)
                .build();

        log.info("OAuth2 client-credentials auth configured (tokenUrl={})", tokenUrl);
    }

    @Override
    public void apply(HttpRequest.Builder requestBuilder) {
        requestBuilder.header("Authorization", "Bearer " + getAccessToken());
    }

    @Override
    public String type() {
        return "oauth2";
    }

    /**
     * Returns a valid access token, refreshing it if the cached token has
     * expired or is about to expire.
     */
    private String getAccessToken() {
        if (isTokenValid()) {
            return accessToken;
        }

        tokenLock.lock();
        try {
            // double-check after acquiring the lock
            if (isTokenValid()) {
                return accessToken;
            }
            refreshToken();
            return accessToken;
        } finally {
            tokenLock.unlock();
        }
    }

    private boolean isTokenValid() {
        return accessToken != null && Instant.now().isBefore(tokenExpiry);
    }

    private void refreshToken() {
        try {
            String body = buildTokenRequestBody();

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(tokenUrl))
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .timeout(HTTP_TIMEOUT)
                    .build();

            log.debug("Requesting new OAuth2 access token from {}", tokenUrl);

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200) {
                throw new IllegalStateException(
                        "OAuth2 token request failed with status " + response.statusCode()
                                + ": " + response.body());
            }

            JsonNode root = objectMapper.readTree(response.body());

            this.accessToken = root.path("access_token").asText(null);
            if (accessToken == null || accessToken.isEmpty()) {
                throw new IllegalStateException(
                        "OAuth2 token response did not contain an access_token");
            }

            long expiresIn = root.path("expires_in").asLong(3600);
            this.tokenExpiry = Instant.now()
                    .plusSeconds(expiresIn)
                    .minusSeconds(EXPIRY_BUFFER_SECONDS);

            log.info("OAuth2 access token refreshed (expires in {}s, effective cache {}s)",
                    expiresIn, expiresIn - EXPIRY_BUFFER_SECONDS);

        } catch (IOException e) {
            throw new IllegalStateException("Failed to fetch OAuth2 access token", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while fetching OAuth2 access token", e);
        }
    }

    private String buildTokenRequestBody() {
        StringJoiner params = new StringJoiner("&");
        params.add("grant_type=client_credentials");
        params.add("client_id=" + encode(clientId));
        params.add("client_secret=" + encode(clientSecret));
        if (!scope.isEmpty()) {
            params.add("scope=" + encode(scope));
        }
        return params.toString();
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }
}
