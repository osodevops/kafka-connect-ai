package sh.oso.connect.ai.adapter.http.auth;

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpRequest;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class BasicAuthStrategyTest {

    @Test
    void configureSetsUsernameAndPasswordFromProps() {
        BasicAuthStrategy strategy = new BasicAuthStrategy();

        Map<String, String> props = Map.of(
                "http.auth.basic.username", "admin",
                "http.auth.basic.password", "secret"
        );

        assertDoesNotThrow(() -> strategy.configure(props));
    }

    @Test
    void applyAddsAuthorizationHeaderWithBase64EncodedValue() {
        BasicAuthStrategy strategy = new BasicAuthStrategy();
        strategy.configure(Map.of(
                "http.auth.basic.username", "admin",
                "http.auth.basic.password", "secret"
        ));

        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create("http://test.com"));
        strategy.apply(builder);

        HttpRequest request = builder.GET().build();
        String expected = "Basic " + Base64.getEncoder()
                .encodeToString("admin:secret".getBytes(StandardCharsets.UTF_8));

        assertEquals(expected, request.headers().firstValue("Authorization").orElse(null));
    }
}
