package sh.oso.connect.ai.connect.pipeline;

import sh.oso.connect.ai.connect.llm.LlmResponse;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;

public final class LlmCostCalculator {

    private LlmCostCalculator() {}

    record ModelPricing(double inputPerMToken, double outputPerMToken,
                        double cacheCreateMultiplier, double cacheReadMultiplier) {}

    private static final Map<String, ModelPricing> PRICING = new LinkedHashMap<>();

    static {
        // Anthropic models
        PRICING.put("claude-opus", new ModelPricing(15.0, 75.0, 1.25, 0.1));
        PRICING.put("claude-sonnet", new ModelPricing(3.0, 15.0, 1.25, 0.1));
        PRICING.put("claude-haiku", new ModelPricing(0.25, 1.25, 1.25, 0.1));

        // OpenAI models (sorted longest-first for matching)
        PRICING.put("gpt-4o-mini", new ModelPricing(0.15, 0.60, 1.0, 1.0));
        PRICING.put("gpt-4.1", new ModelPricing(2.0, 8.0, 1.0, 1.0));
        PRICING.put("gpt-4o", new ModelPricing(2.50, 10.0, 1.0, 1.0));
    }

    // Sorted by key length descending so longer matches win
    private static final java.util.List<Map.Entry<String, ModelPricing>> SORTED_ENTRIES;

    static {
        SORTED_ENTRIES = PRICING.entrySet().stream()
                .sorted(Comparator.comparingInt((Map.Entry<String, ModelPricing> e) -> e.getKey().length()).reversed())
                .toList();
    }

    public static double calculateCostUsd(LlmResponse response) {
        if (response.model() == null) {
            return 0.0;
        }

        ModelPricing pricing = findPricing(response.model());
        if (pricing == null) {
            return 0.0;
        }

        double inputCost = (response.inputTokens() / 1_000_000.0) * pricing.inputPerMToken();
        double outputCost = (response.outputTokens() / 1_000_000.0) * pricing.outputPerMToken();
        double cacheCreateCost = (response.cacheCreationInputTokens() / 1_000_000.0)
                * pricing.inputPerMToken() * pricing.cacheCreateMultiplier();
        double cacheReadCost = (response.cacheReadInputTokens() / 1_000_000.0)
                * pricing.inputPerMToken() * pricing.cacheReadMultiplier();

        return inputCost + outputCost + cacheCreateCost + cacheReadCost;
    }

    // Visible for testing
    static ModelPricing findPricing(String model) {
        String lower = model.toLowerCase();
        for (Map.Entry<String, ModelPricing> entry : SORTED_ENTRIES) {
            if (lower.contains(entry.getKey())) {
                return entry.getValue();
            }
        }
        return null;
    }
}
