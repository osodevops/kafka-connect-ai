package sh.oso.connect.ai.connect.source;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.connect.ai.api.adapter.SourceAdapter;
import sh.oso.connect.ai.api.config.AiConnectConfig;
import sh.oso.connect.ai.api.model.RawRecord;
import sh.oso.connect.ai.api.model.SourceOffset;
import sh.oso.connect.ai.api.model.TransformedRecord;
import sh.oso.connect.ai.api.pipeline.AgentPipeline;
import sh.oso.connect.ai.connect.config.AiSourceConfig;
import sh.oso.connect.ai.connect.llm.LlmClient;
import sh.oso.connect.ai.connect.llm.LlmClientFactory;
import sh.oso.connect.ai.connect.resilience.CircuitBreakingSourceAdapter;
import sh.oso.connect.ai.connect.metrics.LogContext;
import sh.oso.connect.ai.connect.metrics.AiConnectMetrics;
import sh.oso.connect.ai.connect.pipeline.BasicAgentPipeline;
import sh.oso.connect.ai.connect.pipeline.BatchAccumulator;
import sh.oso.connect.ai.connect.pipeline.DiscoveredSchema;
import sh.oso.connect.ai.connect.pipeline.SchemaDiscoveryAgent;
import sh.oso.connect.ai.connect.spi.AdapterRegistry;

import java.util.*;

public class AiSourceTask extends SourceTask {

    private static final Logger log = LoggerFactory.getLogger(AiSourceTask.class);
    private static final int DEFAULT_BATCH_SIZE = 100;
    private static final int SCHEMA_DISCOVERY_SAMPLE_SIZE = 10;

    private SourceAdapter adapter;
    private AgentPipeline pipeline;
    private String topic;
    private int batchSize;
    private SourceOffset lastOffset;
    private BatchAccumulator batchAccumulator;
    private AiConnectMetrics metrics;

    @Override
    public String version() {
        return "0.1.0";
    }

    @Override
    public void start(Map<String, String> props) {
        AiSourceConfig config = new AiSourceConfig(props);
        this.topic = config.topic();
        this.batchSize = Integer.parseInt(
                props.getOrDefault(AiConnectConfig.BATCH_SIZE, String.valueOf(DEFAULT_BATCH_SIZE)));
        this.lastOffset = SourceOffset.empty();

        AdapterRegistry registry = new AdapterRegistry();
        this.adapter = registry.getSourceAdapter(config.sourceAdapterType());
        this.adapter.start(props);

        boolean circuitBreakerEnabled = Boolean.parseBoolean(
                props.getOrDefault(AiConnectConfig.CIRCUIT_BREAKER_ENABLED, "false"));
        if (circuitBreakerEnabled) {
            this.adapter = new CircuitBreakingSourceAdapter(this.adapter, props);
            log.info("Circuit breaker enabled for source adapter '{}'", config.sourceAdapterType());
        }

        Map<String, String> effectiveProps = new HashMap<>(props);

        boolean schemaDiscovery = Boolean.parseBoolean(
                props.getOrDefault(AiConnectConfig.AGENT_SCHEMA_DISCOVERY, "false"));
        String existingSchema = props.getOrDefault(AiConnectConfig.AGENT_TARGET_SCHEMA, "");

        if (schemaDiscovery && existingSchema.isEmpty()) {
            try {
                DiscoveredSchema discovered = runSchemaDiscovery(props);
                if (discovered != null) {
                    effectiveProps.put(AiConnectConfig.AGENT_TARGET_SCHEMA, discovered.jsonSchema());
                    log.info("Schema discovery completed. Inferred schema from {} samples.",
                            discovered.sampleSize());
                }
            } catch (Exception e) {
                log.warn("Schema discovery failed, proceeding without schema: {}", e.getMessage());
            }
        }

        this.pipeline = new BasicAgentPipeline();
        this.pipeline.configure(effectiveProps);

        int accumulatorSize = Integer.parseInt(
                props.getOrDefault(AiConnectConfig.BATCH_ACCUMULATOR_SIZE, "50"));
        long maxWaitMs = Long.parseLong(
                props.getOrDefault(AiConnectConfig.BATCH_ACCUMULATOR_MAX_WAIT_MS, "2000"));
        this.batchAccumulator = new BatchAccumulator(accumulatorSize, maxWaitMs);

        this.metrics = AiConnectMetrics.getInstance();
        LogContext.set(config.sourceAdapterType(), topic, config.sourceAdapterType(),
                props.getOrDefault(AiConnectConfig.LLM_MODEL, ""));

        log.info("AiSourceTask started with adapter={}, topic={}, batchSize={}, accumulatorSize={}",
                config.sourceAdapterType(), topic, batchSize, accumulatorSize);
    }

    private DiscoveredSchema runSchemaDiscovery(Map<String, String> props) throws InterruptedException {
        List<RawRecord> sample = adapter.fetch(SourceOffset.empty(), SCHEMA_DISCOVERY_SAMPLE_SIZE);
        if (sample == null || sample.isEmpty()) {
            log.warn("No sample records available for schema discovery");
            return null;
        }

        String provider = props.getOrDefault(AiConnectConfig.LLM_PROVIDER, "anthropic");
        String apiKey = props.getOrDefault(AiConnectConfig.LLM_API_KEY, "");
        String baseUrl = props.getOrDefault(AiConnectConfig.LLM_BASE_URL, "");
        String model = props.getOrDefault(AiConnectConfig.LLM_MODEL, "claude-sonnet-4-20250514");
        int maxTokens = Integer.parseInt(props.getOrDefault(AiConnectConfig.LLM_MAX_TOKENS, "4096"));
        String systemPrompt = props.getOrDefault(AiConnectConfig.AGENT_SYSTEM_PROMPT, "");

        LlmClient llmClient = LlmClientFactory.create(provider, apiKey, baseUrl);
        SchemaDiscoveryAgent agent = new SchemaDiscoveryAgent(llmClient, model, maxTokens);
        return agent.discoverSchema(sample, systemPrompt);
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        long fetchStart = System.currentTimeMillis();
        List<RawRecord> rawRecords = adapter.fetch(lastOffset, batchSize);
        metrics.recordAdapterFetchLatency(System.currentTimeMillis() - fetchStart);

        if (rawRecords != null && !rawRecords.isEmpty()) {
            batchAccumulator.add(rawRecords);
        }

        if (!batchAccumulator.shouldFlush()) {
            return null;
        }

        List<List<RawRecord>> subBatches = batchAccumulator.flush();
        List<RawRecord> allRecords = subBatches.stream()
                .flatMap(List::stream)
                .toList();

        if (allRecords.isEmpty()) {
            return null;
        }

        metrics.recordBatchSize(allRecords.size());
        List<TransformedRecord> transformed = pipeline.process(allRecords);
        metrics.recordProcessed(transformed.size());

        List<SourceRecord> sourceRecords = new ArrayList<>();
        for (TransformedRecord record : transformed) {
            SourceRecord sourceRecord = new SourceRecord(
                    record.sourceOffset().partition(),
                    record.sourceOffset().offset(),
                    topic,
                    null, // partition
                    Schema.OPTIONAL_BYTES_SCHEMA,
                    record.key(),
                    Schema.BYTES_SCHEMA,
                    record.value()
            );

            record.headers().forEach((k, v) -> sourceRecord.headers().addString(k, v));

            sourceRecords.add(sourceRecord);
        }

        if (!transformed.isEmpty()) {
            lastOffset = transformed.get(transformed.size() - 1).sourceOffset();
        }

        return sourceRecords;
    }

    @Override
    public void stop() {
        if (adapter != null) {
            adapter.stop();
        }
        LogContext.clear();
        log.info("AiSourceTask stopped");
    }
}
