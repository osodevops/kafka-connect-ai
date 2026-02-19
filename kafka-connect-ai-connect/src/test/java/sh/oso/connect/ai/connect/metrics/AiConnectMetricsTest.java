package sh.oso.connect.ai.connect.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class AiConnectMetricsTest {

    private MeterRegistry registry;
    private AiConnectMetrics metrics;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        metrics = AiConnectMetrics.createForTesting(registry);
    }

    @Test
    void allMetricsRegistered() {
        assertNotNull(registry.find("connect.ai.records.processed.total").counter());
        assertNotNull(registry.find("connect.ai.records.failed.total").counter());
        assertNotNull(registry.find("connect.ai.llm.calls.total").counter());
        assertNotNull(registry.find("connect.ai.llm.call.latency").timer());
        assertNotNull(registry.find("connect.ai.llm.tokens.input.total").counter());
        assertNotNull(registry.find("connect.ai.llm.tokens.output.total").counter());
        assertNotNull(registry.find("connect.ai.llm.cost.usd.total").counter());
        assertNotNull(registry.find("connect.ai.cache.hits.total").counter());
        assertNotNull(registry.find("connect.ai.cache.misses.total").counter());
        assertNotNull(registry.find("connect.ai.cache.hit.ratio").gauge());
        assertNotNull(registry.find("connect.ai.router.tier0.total").counter());
        assertNotNull(registry.find("connect.ai.router.tier1.total").counter());
        assertNotNull(registry.find("connect.ai.router.tier2.total").counter());
        assertNotNull(registry.find("connect.ai.router.tier3.total").counter());
        assertNotNull(registry.find("connect.ai.adapter.fetch.latency").timer());
        assertNotNull(registry.find("connect.ai.adapter.write.latency").timer());
        assertNotNull(registry.find("connect.ai.batch.size").summary());
    }

    @Test
    void recordsProcessedIncrements() {
        metrics.recordProcessed(5);
        assertEquals(5.0, metrics.getRecordsProcessed());

        metrics.recordProcessed(3);
        assertEquals(8.0, metrics.getRecordsProcessed());
    }

    @Test
    void recordsFailedIncrements() {
        metrics.recordFailed(2);
        assertEquals(2.0, metrics.getRecordsFailed());
    }

    @Test
    void llmCallsIncrements() {
        metrics.recordLlmCall();
        metrics.recordLlmCall();
        assertEquals(2.0, metrics.getLlmCallsTotal());
    }

    @Test
    void llmTokensTracked() {
        metrics.recordLlmTokens(100, 50);
        assertEquals(100.0, registry.find("connect.ai.llm.tokens.input.total").counter().count());
        assertEquals(50.0, registry.find("connect.ai.llm.tokens.output.total").counter().count());
    }

    @Test
    void cacheHitRatioComputed() {
        metrics.recordCacheHit();
        metrics.recordCacheHit();
        metrics.recordCacheMiss();

        assertEquals(2.0, metrics.getCacheHits());
        assertEquals(1.0, metrics.getCacheMisses());

        double ratio = registry.find("connect.ai.cache.hit.ratio").gauge().value();
        assertEquals(2.0 / 3.0, ratio, 0.01);
    }

    @Test
    void cacheHitRatioZeroWhenNoAccess() {
        double ratio = registry.find("connect.ai.cache.hit.ratio").gauge().value();
        assertEquals(0.0, ratio);
    }

    @Test
    void routerTiersIncrement() {
        metrics.recordRouterTier(0);
        metrics.recordRouterTier(0);
        metrics.recordRouterTier(1);
        metrics.recordRouterTier(2);
        metrics.recordRouterTier(3);

        assertEquals(2.0, metrics.getRouterTier0());
        assertEquals(1.0, metrics.getRouterTier1());
        assertEquals(1.0, metrics.getRouterTier2());
        assertEquals(1.0, metrics.getRouterTier3());
    }

    @Test
    void llmLatencyRecorded() {
        metrics.recordLlmLatency(150);
        assertEquals(1, registry.find("connect.ai.llm.call.latency").timer().count());
    }

    @Test
    void batchSizeRecorded() {
        metrics.recordBatchSize(50);
        metrics.recordBatchSize(30);
        assertEquals(2, registry.find("connect.ai.batch.size").summary().count());
    }

    @Test
    void adapterLatencyRecorded() {
        metrics.recordAdapterFetchLatency(25);
        metrics.recordAdapterWriteLatency(10);
        assertEquals(1, registry.find("connect.ai.adapter.fetch.latency").timer().count());
        assertEquals(1, registry.find("connect.ai.adapter.write.latency").timer().count());
    }

    @Test
    void llmCostTracked() {
        metrics.recordLlmCost(0.005);
        metrics.recordLlmCost(0.003);
        assertEquals(0.008, registry.find("connect.ai.llm.cost.usd.total").counter().count(), 0.0001);
    }
}
