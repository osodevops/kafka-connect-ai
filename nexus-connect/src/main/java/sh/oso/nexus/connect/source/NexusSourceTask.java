package sh.oso.nexus.connect.source;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.nexus.api.adapter.SourceAdapter;
import sh.oso.nexus.api.config.NexusConfig;
import sh.oso.nexus.api.model.RawRecord;
import sh.oso.nexus.api.model.SourceOffset;
import sh.oso.nexus.api.model.TransformedRecord;
import sh.oso.nexus.api.pipeline.AgentPipeline;
import sh.oso.nexus.connect.config.NexusSourceConfig;
import sh.oso.nexus.connect.llm.LlmClient;
import sh.oso.nexus.connect.llm.LlmClientFactory;
import sh.oso.nexus.connect.metrics.LogContext;
import sh.oso.nexus.connect.metrics.NexusMetrics;
import sh.oso.nexus.connect.pipeline.BasicAgentPipeline;
import sh.oso.nexus.connect.pipeline.BatchAccumulator;
import sh.oso.nexus.connect.pipeline.DiscoveredSchema;
import sh.oso.nexus.connect.pipeline.SchemaDiscoveryAgent;
import sh.oso.nexus.connect.spi.AdapterRegistry;

import java.util.*;

public class NexusSourceTask extends SourceTask {

    private static final Logger log = LoggerFactory.getLogger(NexusSourceTask.class);
    private static final int DEFAULT_BATCH_SIZE = 100;
    private static final int SCHEMA_DISCOVERY_SAMPLE_SIZE = 10;

    private SourceAdapter adapter;
    private AgentPipeline pipeline;
    private String topic;
    private int batchSize;
    private SourceOffset lastOffset;
    private BatchAccumulator batchAccumulator;
    private NexusMetrics metrics;

    @Override
    public String version() {
        return "0.1.0";
    }

    @Override
    public void start(Map<String, String> props) {
        NexusSourceConfig config = new NexusSourceConfig(props);
        this.topic = config.topic();
        this.batchSize = Integer.parseInt(
                props.getOrDefault(NexusConfig.BATCH_SIZE, String.valueOf(DEFAULT_BATCH_SIZE)));
        this.lastOffset = SourceOffset.empty();

        AdapterRegistry registry = new AdapterRegistry();
        this.adapter = registry.getSourceAdapter(config.sourceAdapterType());
        this.adapter.start(props);

        Map<String, String> effectiveProps = new HashMap<>(props);

        boolean schemaDiscovery = Boolean.parseBoolean(
                props.getOrDefault(NexusConfig.AGENT_SCHEMA_DISCOVERY, "false"));
        String existingSchema = props.getOrDefault(NexusConfig.AGENT_TARGET_SCHEMA, "");

        if (schemaDiscovery && existingSchema.isEmpty()) {
            try {
                DiscoveredSchema discovered = runSchemaDiscovery(props);
                if (discovered != null) {
                    effectiveProps.put(NexusConfig.AGENT_TARGET_SCHEMA, discovered.jsonSchema());
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
                props.getOrDefault(NexusConfig.BATCH_ACCUMULATOR_SIZE, "50"));
        long maxWaitMs = Long.parseLong(
                props.getOrDefault(NexusConfig.BATCH_ACCUMULATOR_MAX_WAIT_MS, "2000"));
        this.batchAccumulator = new BatchAccumulator(accumulatorSize, maxWaitMs);

        this.metrics = NexusMetrics.getInstance();
        LogContext.set(config.sourceAdapterType(), topic, config.sourceAdapterType(),
                props.getOrDefault(NexusConfig.LLM_MODEL, ""));

        log.info("NexusSourceTask started with adapter={}, topic={}, batchSize={}, accumulatorSize={}",
                config.sourceAdapterType(), topic, batchSize, accumulatorSize);
    }

    private DiscoveredSchema runSchemaDiscovery(Map<String, String> props) throws InterruptedException {
        List<RawRecord> sample = adapter.fetch(SourceOffset.empty(), SCHEMA_DISCOVERY_SAMPLE_SIZE);
        if (sample == null || sample.isEmpty()) {
            log.warn("No sample records available for schema discovery");
            return null;
        }

        String provider = props.getOrDefault(NexusConfig.LLM_PROVIDER, "anthropic");
        String apiKey = props.getOrDefault(NexusConfig.LLM_API_KEY, "");
        String baseUrl = props.getOrDefault(NexusConfig.LLM_BASE_URL, "");
        String model = props.getOrDefault(NexusConfig.LLM_MODEL, "claude-sonnet-4-20250514");
        int maxTokens = Integer.parseInt(props.getOrDefault(NexusConfig.LLM_MAX_TOKENS, "4096"));
        String systemPrompt = props.getOrDefault(NexusConfig.AGENT_SYSTEM_PROMPT, "");

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
        log.info("NexusSourceTask stopped");
    }
}
