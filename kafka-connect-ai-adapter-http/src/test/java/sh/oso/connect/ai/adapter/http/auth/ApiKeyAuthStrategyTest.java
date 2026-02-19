package sh.oso.connect.ai.adapter.http.auth;

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpRequest;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ApiKeyAuthStrategyTest {

    @Test
    void applyWithDefaultHeaderName() {
        ApiKeyAuthStrategy strategy = new ApiKeyAuthStrategy();
        strategy.configure(Map.of("http.auth.apikey.value", "abc123"));

        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create("http://test.com"));
        strategy.apply(builder);

        HttpRequest request = builder.GET().build();
        assertEquals("abc123",
                request.headers().firstValue("X-API-Key").orElse(null));
    }

    @Test
    void applyWithCustomHeaderName() {
        ApiKeyAuthStrategy strategy = new ApiKeyAuthStrategy();
        strategy.configure(Map.of(
                "http.auth.apikey.header", "X-Custom-Key",
                "http.auth.apikey.value", "xyz789"
        ));

        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create("http://test.com"));
        strategy.apply(builder);

        HttpRequest request = builder.GET().build();
        assertEquals("xyz789",
                request.headers().firstValue("X-Custom-Key").orElse(null));
    }
}
