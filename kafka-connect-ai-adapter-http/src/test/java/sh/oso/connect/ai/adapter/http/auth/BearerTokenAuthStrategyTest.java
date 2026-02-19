package sh.oso.connect.ai.adapter.http.auth;

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpRequest;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class BearerTokenAuthStrategyTest {

    @Test
    void applyAddsCorrectBearerTokenHeader() {
        BearerTokenAuthStrategy strategy = new BearerTokenAuthStrategy();
        strategy.configure(Map.of("http.auth.bearer.token", "my-jwt-token"));

        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create("http://test.com"));
        strategy.apply(builder);

        HttpRequest request = builder.GET().build();
        assertEquals("Bearer my-jwt-token",
                request.headers().firstValue("Authorization").orElse(null));
    }
}
