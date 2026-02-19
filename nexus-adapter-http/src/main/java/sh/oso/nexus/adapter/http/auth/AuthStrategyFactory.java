package sh.oso.nexus.adapter.http.auth;

/**
 * Factory for creating {@link AuthStrategy} instances based on a type identifier.
 *
 * <p>Supported types: {@code none}, {@code basic}, {@code bearer}, {@code oauth2}, {@code apikey}.</p>
 */
public final class AuthStrategyFactory {

    private AuthStrategyFactory() {}

    /**
     * Creates a new, unconfigured {@link AuthStrategy} for the given type.
     *
     * @param type the authentication type (case-insensitive)
     * @return a new strategy instance
     * @throws IllegalArgumentException if the type is not supported
     */
    public static AuthStrategy create(String type) {
        return switch (type.toLowerCase()) {
            case "none" -> new NoAuthStrategy();
            case "basic" -> new BasicAuthStrategy();
            case "bearer" -> new BearerTokenAuthStrategy();
            case "oauth2" -> new OAuth2Strategy();
            case "apikey" -> new ApiKeyAuthStrategy();
            default -> throw new IllegalArgumentException(
                    "Unsupported auth strategy type: " + type
                            + ". Supported: none, basic, bearer, oauth2, apikey");
        };
    }
}
