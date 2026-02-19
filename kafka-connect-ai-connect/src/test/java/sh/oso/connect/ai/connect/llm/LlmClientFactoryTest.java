package sh.oso.connect.ai.connect.llm;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class LlmClientFactoryTest {

    @Test
    void createAnthropicClientReturnsAnthropicInstance() {
        LlmClient client = LlmClientFactory.create("anthropic", "test-key", "");

        assertInstanceOf(AnthropicClient.class, client);
    }

    @Test
    void createOpenAiClientReturnsOpenAiInstance() {
        LlmClient client = LlmClientFactory.create("openai", "test-key", "");

        assertInstanceOf(OpenAiClient.class, client);
    }

    @Test
    void createWithUnsupportedProviderThrowsIllegalArgumentException() {
        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> LlmClientFactory.create("gemini", "test-key", "")
        );

        assertTrue(ex.getMessage().contains("gemini"));
    }
}
