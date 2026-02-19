package sh.oso.nexus.adapter.http.auth;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class AuthStrategyFactoryTest {

    @Test
    void createNoneReturnsNoAuthStrategy() {
        AuthStrategy strategy = AuthStrategyFactory.create("none");
        assertInstanceOf(NoAuthStrategy.class, strategy);
    }

    @Test
    void createBasicReturnsBasicAuthStrategy() {
        AuthStrategy strategy = AuthStrategyFactory.create("basic");
        assertInstanceOf(BasicAuthStrategy.class, strategy);
    }

    @Test
    void createBearerReturnsBearerTokenAuthStrategy() {
        AuthStrategy strategy = AuthStrategyFactory.create("bearer");
        assertInstanceOf(BearerTokenAuthStrategy.class, strategy);
    }

    @Test
    void createOauth2ReturnsOAuth2Strategy() {
        AuthStrategy strategy = AuthStrategyFactory.create("oauth2");
        assertInstanceOf(OAuth2Strategy.class, strategy);
    }

    @Test
    void createApikeyReturnsApiKeyAuthStrategy() {
        AuthStrategy strategy = AuthStrategyFactory.create("apikey");
        assertInstanceOf(ApiKeyAuthStrategy.class, strategy);
    }

    @Test
    void createUnknownTypeThrowsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () -> AuthStrategyFactory.create("unknown"));
    }
}
