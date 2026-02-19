package sh.oso.connect.ai.connect.llm;

public final class LlmClientFactory {

    private LlmClientFactory() {}

    public static LlmClient create(String provider, String apiKey, String baseUrl) {
        return switch (provider.toLowerCase()) {
            case "anthropic" -> new AnthropicClient(apiKey, baseUrl);
            case "openai" -> new OpenAiClient(apiKey, baseUrl);
            default -> throw new IllegalArgumentException(
                    "Unsupported LLM provider: " + provider + ". Supported: anthropic, openai");
        };
    }
}
