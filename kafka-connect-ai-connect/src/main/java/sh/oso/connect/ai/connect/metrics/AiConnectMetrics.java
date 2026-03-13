package sh.oso.connect.ai.connect.metrics;

import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.jmx.JmxMeterRegistry;
import io.micrometer.jmx.JmxConfig;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class AiConnectMetrics {

    private static final String PREFIX = "connect.ai";

    private static volatile AiConnectMetrics instance;

    private final MeterRegistry registry;

    private final Counter recordsProcessed;
    private final Counter recordsFailed;
    private final Counter llmCallsTotal;
    private final Timer llmCallLatency;
    private final Counter llmTokensInput;
    private final Counter llmTokensOutput;
    private final Counter llmCostUsd;
    private final Counter cacheHits;
    private final Counter cacheMisses;
    private final AtomicLong cacheHitCount = new AtomicLong(0);
    private final AtomicLong cacheTotalCount = new AtomicLong(0);
    private final Counter routerTier0;
    private final Counter routerTier1;
    private final Counter routerTier2;
    private final Counter routerTier3;
    private final Timer adapterFetchLatency;
    private final Timer adapterWriteLatency;
    private final DistributionSummary batchSize;
    private final Counter compiledTransformHits;
    private final Counter compiledTransformMisses;
    private final Counter compiledTransformCompilations;
    private final Counter compiledTransformInvalidations;
    private final Counter circuitBreakerOpen;
    private final Counter llmRateLimited;

    AiConnectMetrics(MeterRegistry registry) {
        this.registry = registry;

        this.recordsProcessed = Counter.builder(PREFIX + ".records.processed.total")
                .description("Total records successfully processed")
                .register(registry);

        this.recordsFailed = Counter.builder(PREFIX + ".records.failed.total")
                .description("Total records that failed processing")
                .register(registry);

        this.llmCallsTotal = Counter.builder(PREFIX + ".llm.calls.total")
                .description("Total LLM API calls made")
                .register(registry);

        this.llmCallLatency = Timer.builder(PREFIX + ".llm.call.latency")
                .description("LLM call latency")
                .publishPercentileHistogram()
                .register(registry);

        this.llmTokensInput = Counter.builder(PREFIX + ".llm.tokens.input.total")
                .description("Total input tokens consumed")
                .register(registry);

        this.llmTokensOutput = Counter.builder(PREFIX + ".llm.tokens.output.total")
                .description("Total output tokens produced")
                .register(registry);

        this.llmCostUsd = Counter.builder(PREFIX + ".llm.cost.usd.total")
                .description("Estimated total LLM cost in USD")
                .register(registry);

        this.cacheHits = Counter.builder(PREFIX + ".cache.hits.total")
                .description("Total semantic cache hits")
                .register(registry);

        this.cacheMisses = Counter.builder(PREFIX + ".cache.misses.total")
                .description("Total semantic cache misses")
                .register(registry);

        Gauge.builder(PREFIX + ".cache.hit.ratio", this, m -> {
            long total = m.cacheTotalCount.get();
            return total == 0 ? 0.0 : (double) m.cacheHitCount.get() / total;
        }).description("Cache hit ratio").register(registry);

        this.routerTier0 = Counter.builder(PREFIX + ".router.tier0.total")
                .description("Records routed to tier 0 (deterministic)")
                .register(registry);

        this.routerTier1 = Counter.builder(PREFIX + ".router.tier1.total")
                .description("Records routed to tier 1 (fast model)")
                .register(registry);

        this.routerTier2 = Counter.builder(PREFIX + ".router.tier2.total")
                .description("Records routed to tier 2 (default model)")
                .register(registry);

        this.routerTier3 = Counter.builder(PREFIX + ".router.tier3.total")
                .description("Records routed to tier 3 (powerful model)")
                .register(registry);

        this.adapterFetchLatency = Timer.builder(PREFIX + ".adapter.fetch.latency")
                .description("Source adapter fetch latency")
                .publishPercentileHistogram()
                .register(registry);

        this.adapterWriteLatency = Timer.builder(PREFIX + ".adapter.write.latency")
                .description("Sink adapter write latency")
                .publishPercentileHistogram()
                .register(registry);

        this.batchSize = DistributionSummary.builder(PREFIX + ".batch.size")
                .description("Batch sizes processed")
                .register(registry);

        this.compiledTransformHits = Counter.builder(PREFIX + ".compiled.transform.hits.total")
                .description("Total compiled transform cache hits")
                .register(registry);

        this.compiledTransformMisses = Counter.builder(PREFIX + ".compiled.transform.misses.total")
                .description("Total compiled transform cache misses")
                .register(registry);

        this.compiledTransformCompilations = Counter.builder(PREFIX + ".compiled.transform.compilations.total")
                .description("Total compiled transform compilations triggered")
                .register(registry);

        this.compiledTransformInvalidations = Counter.builder(PREFIX + ".compiled.transform.invalidations.total")
                .description("Total compiled transform invalidations")
                .register(registry);

        this.circuitBreakerOpen = Counter.builder(PREFIX + ".circuit.breaker.open.total")
                .description("Total times circuit breaker was open when LLM call attempted")
                .register(registry);

        this.llmRateLimited = Counter.builder(PREFIX + ".llm.rate.limited.total")
                .description("Total times LLM calls were rate limited")
                .register(registry);
    }

    public static AiConnectMetrics getInstance() {
        if (instance == null) {
            synchronized (AiConnectMetrics.class) {
                if (instance == null) {
                    MeterRegistry jmxRegistry;
                    try {
                        jmxRegistry = new JmxMeterRegistry(JmxConfig.DEFAULT, Clock.SYSTEM);
                    } catch (Exception e) {
                        jmxRegistry = new SimpleMeterRegistry();
                    }
                    instance = new AiConnectMetrics(jmxRegistry);
                }
            }
        }
        return instance;
    }

    // Visible for testing
    public static AiConnectMetrics createForTesting(MeterRegistry registry) {
        return new AiConnectMetrics(registry);
    }

    // Visible for testing — resets the singleton
    static void resetInstance() {
        synchronized (AiConnectMetrics.class) {
            instance = null;
        }
    }

    public MeterRegistry getRegistry() {
        return registry;
    }

    public void recordProcessed(int count) {
        recordsProcessed.increment(count);
    }

    public void recordFailed(int count) {
        recordsFailed.increment(count);
    }

    public void recordLlmCall() {
        llmCallsTotal.increment();
    }

    public void recordLlmLatency(long durationMs) {
        llmCallLatency.record(Duration.ofMillis(durationMs));
    }

    public void recordLlmTokens(int input, int output) {
        llmTokensInput.increment(input);
        llmTokensOutput.increment(output);
    }

    public void recordLlmCost(double usd) {
        llmCostUsd.increment(usd);
    }

    public void recordCacheHit() {
        cacheHits.increment();
        cacheHitCount.incrementAndGet();
        cacheTotalCount.incrementAndGet();
    }

    public void recordCacheMiss() {
        cacheMisses.increment();
        cacheTotalCount.incrementAndGet();
    }

    public void recordRouterTier(int tier) {
        switch (tier) {
            case 0 -> routerTier0.increment();
            case 1 -> routerTier1.increment();
            case 2 -> routerTier2.increment();
            case 3 -> routerTier3.increment();
            default -> routerTier2.increment();
        }
    }

    public void recordAdapterFetchLatency(long durationMs) {
        adapterFetchLatency.record(Duration.ofMillis(durationMs));
    }

    public void recordAdapterWriteLatency(long durationMs) {
        adapterWriteLatency.record(Duration.ofMillis(durationMs));
    }

    public void recordBatchSize(int size) {
        batchSize.record(size);
    }

    public double getRecordsProcessed() {
        return recordsProcessed.count();
    }

    public double getRecordsFailed() {
        return recordsFailed.count();
    }

    public double getLlmCallsTotal() {
        return llmCallsTotal.count();
    }

    public double getCacheHits() {
        return cacheHits.count();
    }

    public double getCacheMisses() {
        return cacheMisses.count();
    }

    public double getRouterTier0() {
        return routerTier0.count();
    }

    public double getRouterTier1() {
        return routerTier1.count();
    }

    public double getRouterTier2() {
        return routerTier2.count();
    }

    public double getRouterTier3() {
        return routerTier3.count();
    }

    public void recordCompiledTransformHit() {
        compiledTransformHits.increment();
    }

    public void recordCompiledTransformMiss() {
        compiledTransformMisses.increment();
    }

    public void recordCompiledTransformCompilation() {
        compiledTransformCompilations.increment();
    }

    public void recordCompiledTransformInvalidation() {
        compiledTransformInvalidations.increment();
    }

    public void recordCircuitBreakerOpen() {
        circuitBreakerOpen.increment();
    }

    public void registerCompiledTransformCacheSize(java.util.function.Supplier<Number> sizeSupplier) {
        Gauge.builder(PREFIX + ".compiled.transform.cache.size", sizeSupplier)
                .description("Current number of cached compiled transforms")
                .register(registry);
    }

    public double getCompiledTransformHits() {
        return compiledTransformHits.count();
    }

    public double getCompiledTransformMisses() {
        return compiledTransformMisses.count();
    }

    public double getCompiledTransformCompilations() {
        return compiledTransformCompilations.count();
    }

    public double getCompiledTransformInvalidations() {
        return compiledTransformInvalidations.count();
    }

    public double getCircuitBreakerOpen() {
        return circuitBreakerOpen.count();
    }

    public void recordLlmRateLimited() {
        llmRateLimited.increment();
    }

    public double getLlmRateLimited() {
        return llmRateLimited.count();
    }

    public double getLlmCostUsd() {
        return llmCostUsd.count();
    }
}
