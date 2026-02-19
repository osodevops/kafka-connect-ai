package sh.oso.nexus.connect.metrics;

import org.slf4j.MDC;

public final class LogContext {

    private LogContext() {}

    // Static context keys
    private static final String CONNECTOR = "connector";
    private static final String TASK = "task";
    private static final String ADAPTER = "adapter";
    private static final String LLM_MODEL = "llm_model";

    // Per-batch context keys
    private static final String BATCH_SIZE = "batch_size";
    private static final String CACHE_HIT = "cache_hit";
    private static final String ROUTER_TIER = "router_tier";
    private static final String LATENCY_MS = "latency_ms";
    private static final String TOKENS_IN = "tokens_in";
    private static final String TOKENS_OUT = "tokens_out";

    public static void set(String connector, String task, String adapter, String llmModel) {
        MDC.put(CONNECTOR, connector);
        MDC.put(TASK, task);
        MDC.put(ADAPTER, adapter);
        MDC.put(LLM_MODEL, llmModel);
    }

    public static void withBatch(int batchSize, boolean cacheHit, int routerTier,
                                  long latencyMs, int tokensIn, int tokensOut) {
        MDC.put(BATCH_SIZE, String.valueOf(batchSize));
        MDC.put(CACHE_HIT, String.valueOf(cacheHit));
        MDC.put(ROUTER_TIER, String.valueOf(routerTier));
        MDC.put(LATENCY_MS, String.valueOf(latencyMs));
        MDC.put(TOKENS_IN, String.valueOf(tokensIn));
        MDC.put(TOKENS_OUT, String.valueOf(tokensOut));
    }

    public static void clearBatch() {
        MDC.remove(BATCH_SIZE);
        MDC.remove(CACHE_HIT);
        MDC.remove(ROUTER_TIER);
        MDC.remove(LATENCY_MS);
        MDC.remove(TOKENS_IN);
        MDC.remove(TOKENS_OUT);
    }

    public static void clear() {
        MDC.clear();
    }
}
