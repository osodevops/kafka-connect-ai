package sh.oso.nexus.connect.llm;

public interface LlmClient {

    LlmResponse call(LlmRequest request);
}
