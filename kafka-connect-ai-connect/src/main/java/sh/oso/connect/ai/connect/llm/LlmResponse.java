package sh.oso.connect.ai.connect.llm;

public record LlmResponse(
        String content,
        int inputTokens,
        int outputTokens,
        String model,
        String stopReason,
        int cacheCreationInputTokens,
        int cacheReadInputTokens
) {
    public LlmResponse(String content, int inputTokens, int outputTokens,
                        String model, String stopReason) {
        this(content, inputTokens, outputTokens, model, stopReason, 0, 0);
    }
}
