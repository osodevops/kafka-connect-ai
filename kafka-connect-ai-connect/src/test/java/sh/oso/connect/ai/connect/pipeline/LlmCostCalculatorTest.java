package sh.oso.connect.ai.connect.pipeline;

import org.junit.jupiter.api.Test;
import sh.oso.connect.ai.connect.llm.LlmResponse;

import static org.junit.jupiter.api.Assertions.*;

class LlmCostCalculatorTest {

    @Test
    void calculateCost_claudeSonnet_correctMath() {
        // 1000 input tokens, 500 output tokens, claude-sonnet-4-20250514
        // input: 1000/1M * $3.0 = $0.003
        // output: 500/1M * $15.0 = $0.0075
        // total: $0.0105
        LlmResponse response = new LlmResponse(
                "output", 1000, 500, "claude-sonnet-4-20250514", "end_turn", 0, 0);

        double cost = LlmCostCalculator.calculateCostUsd(response);

        assertEquals(0.0105, cost, 0.000001);
    }

    @Test
    void calculateCost_cacheMultipliers_anthropic() {
        // claude-sonnet: inputPerMToken=3.0, cacheCreate=1.25x, cacheRead=0.1x
        // 1000 input, 500 output, 200 cache_create, 300 cache_read
        // input: 1000/1M * 3.0 = 0.003
        // output: 500/1M * 15.0 = 0.0075
        // cacheCreate: 200/1M * 3.0 * 1.25 = 0.00000075 * 1000 = 0.00075
        // cacheRead: 300/1M * 3.0 * 0.1 = 0.00009
        LlmResponse response = new LlmResponse(
                "output", 1000, 500, "claude-sonnet-4-20250514", "end_turn", 200, 300);

        double cost = LlmCostCalculator.calculateCostUsd(response);

        double expected = 0.003 + 0.0075 + 0.00075 + 0.00009;
        assertEquals(expected, cost, 0.000001);
    }

    @Test
    void calculateCost_unknownModel_returnsZero() {
        LlmResponse response = new LlmResponse(
                "output", 1000, 500, "unknown-model-v1", "end_turn", 0, 0);

        double cost = LlmCostCalculator.calculateCostUsd(response);

        assertEquals(0.0, cost);
    }

    @Test
    void calculateCost_nullModel_returnsZero() {
        LlmResponse response = new LlmResponse(
                "output", 1000, 500, null, "end_turn", 0, 0);

        double cost = LlmCostCalculator.calculateCostUsd(response);

        assertEquals(0.0, cost);
    }

    @Test
    void findPricing_gpt4oMini_matchedBeforeGpt4o() {
        // gpt-4o-mini should match the mini pricing, not gpt-4o
        var pricing = LlmCostCalculator.findPricing("gpt-4o-mini-2024-07-18");
        assertNotNull(pricing);
        assertEquals(0.15, pricing.inputPerMToken());
        assertEquals(0.60, pricing.outputPerMToken());
    }

    @Test
    void findPricing_gpt4o_matchesCorrectly() {
        var pricing = LlmCostCalculator.findPricing("gpt-4o-2024-08-06");
        assertNotNull(pricing);
        assertEquals(2.50, pricing.inputPerMToken());
    }

    @Test
    void calculateCost_claudeHaiku() {
        // 10000 input, 2000 output
        // input: 10000/1M * 0.25 = 0.0025
        // output: 2000/1M * 1.25 = 0.0025
        LlmResponse response = new LlmResponse(
                "output", 10000, 2000, "claude-haiku-4-5-20251001", "end_turn");

        double cost = LlmCostCalculator.calculateCostUsd(response);

        assertEquals(0.005, cost, 0.000001);
    }
}
