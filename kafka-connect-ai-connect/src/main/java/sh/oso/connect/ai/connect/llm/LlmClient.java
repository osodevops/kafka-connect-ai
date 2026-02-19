package sh.oso.connect.ai.connect.llm;

public interface LlmClient {

    LlmResponse call(LlmRequest request);
}
