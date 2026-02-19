package sh.oso.nexus.connect.llm;

import java.util.Optional;
import java.util.OptionalInt;

public record LlmRequest(
        String systemPrompt,
        String userPrompt,
        String model,
        Optional<String> jsonSchema,
        boolean enablePromptCaching,
        double temperature,
        int maxTokens,
        OptionalInt seed
) {
    public LlmRequest {
        jsonSchema = jsonSchema != null ? jsonSchema : Optional.empty();
        seed = seed != null ? seed : OptionalInt.empty();
        if (maxTokens <= 0) {
            maxTokens = 4096;
        }
    }

    /** Backwards-compatible constructor without seed. */
    public LlmRequest(String systemPrompt, String userPrompt, String model,
                      Optional<String> jsonSchema, boolean enablePromptCaching,
                      double temperature, int maxTokens) {
        this(systemPrompt, userPrompt, model, jsonSchema, enablePromptCaching,
                temperature, maxTokens, OptionalInt.empty());
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String systemPrompt = "";
        private String userPrompt = "";
        private String model = "";
        private Optional<String> jsonSchema = Optional.empty();
        private boolean enablePromptCaching = false;
        private double temperature = 0.0;
        private int maxTokens = 4096;
        private OptionalInt seed = OptionalInt.empty();

        public Builder systemPrompt(String systemPrompt) {
            this.systemPrompt = systemPrompt;
            return this;
        }

        public Builder userPrompt(String userPrompt) {
            this.userPrompt = userPrompt;
            return this;
        }

        public Builder model(String model) {
            this.model = model;
            return this;
        }

        public Builder jsonSchema(String jsonSchema) {
            this.jsonSchema = Optional.ofNullable(jsonSchema);
            return this;
        }

        public Builder enablePromptCaching(boolean enablePromptCaching) {
            this.enablePromptCaching = enablePromptCaching;
            return this;
        }

        public Builder temperature(double temperature) {
            this.temperature = temperature;
            return this;
        }

        public Builder maxTokens(int maxTokens) {
            this.maxTokens = maxTokens;
            return this;
        }

        public Builder seed(int seed) {
            this.seed = OptionalInt.of(seed);
            return this;
        }

        public LlmRequest build() {
            return new LlmRequest(systemPrompt, userPrompt, model, jsonSchema,
                    enablePromptCaching, temperature, maxTokens, seed);
        }
    }
}
