package sh.oso.connect.ai.connect.sink;

import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.connect.ai.api.adapter.SinkAdapter;
import sh.oso.connect.ai.api.config.AiConnectConfig;
import sh.oso.connect.ai.api.model.RawRecord;
import sh.oso.connect.ai.api.model.SourceOffset;
import sh.oso.connect.ai.api.model.TransformedRecord;
import sh.oso.connect.ai.api.pipeline.AgentPipeline;
import sh.oso.connect.ai.connect.config.AiSinkConfig;
import sh.oso.connect.ai.connect.metrics.LogContext;
import sh.oso.connect.ai.connect.metrics.AiConnectMetrics;
import sh.oso.connect.ai.connect.pipeline.BasicAgentPipeline;
import sh.oso.connect.ai.connect.resilience.CircuitBreakingSinkAdapter;
import sh.oso.connect.ai.connect.spi.AdapterRegistry;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class AiSinkTask extends SinkTask {

    private static final Logger log = LoggerFactory.getLogger(AiSinkTask.class);

    private SinkAdapter adapter;
    private AgentPipeline pipeline;
    private ErrantRecordReporter reporter;
    private AiConnectMetrics metrics;

    @Override
    public String version() {
        return "0.1.0";
    }

    @Override
    public void start(Map<String, String> props) {
        AiSinkConfig config = new AiSinkConfig(props);

        AdapterRegistry registry = new AdapterRegistry();
        this.adapter = registry.getSinkAdapter(config.sinkAdapterType());
        this.adapter.start(props);

        boolean circuitBreakerEnabled = Boolean.parseBoolean(
                props.getOrDefault(AiConnectConfig.CIRCUIT_BREAKER_ENABLED, "false"));
        if (circuitBreakerEnabled) {
            this.adapter = new CircuitBreakingSinkAdapter(this.adapter, props);
            log.info("Circuit breaker enabled for sink adapter '{}'", config.sinkAdapterType());
        }

        this.pipeline = new BasicAgentPipeline();
        this.pipeline.configure(props);

        this.metrics = AiConnectMetrics.getInstance();
        LogContext.set(config.sinkAdapterType(), "sink", config.sinkAdapterType(),
                props.getOrDefault(AiConnectConfig.LLM_MODEL, ""));

        try {
            this.reporter = context.errantRecordReporter();
        } catch (Exception e) {
            log.warn("ErrantRecordReporter not available, DLQ disabled");
            this.reporter = null;
        }

        log.info("AiSinkTask started with adapter={}", config.sinkAdapterType());
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (records.isEmpty()) {
            return;
        }

        List<RawRecord> rawRecords = new ArrayList<>();
        List<SinkRecord> originalRecords = new ArrayList<>(records);

        for (SinkRecord record : records) {
            byte[] key = record.key() instanceof byte[] ? (byte[]) record.key() : null;
            byte[] value = record.value() instanceof byte[] ? (byte[]) record.value() : null;
            rawRecords.add(new RawRecord(key, value, Map.of(), SourceOffset.empty()));
        }

        try {
            List<TransformedRecord> transformed = pipeline.process(rawRecords);
            long writeStart = System.currentTimeMillis();
            adapter.write(transformed);
            metrics.recordAdapterWriteLatency(System.currentTimeMillis() - writeStart);
            metrics.recordProcessed(transformed.size());
        } catch (Exception e) {
            log.error("Failed to process batch of {} records: {}", records.size(), e.getMessage());
            if (metrics != null) {
                metrics.recordFailed(records.size());
            }
            if (reporter != null) {
                for (SinkRecord record : originalRecords) {
                    // Add DLQ context headers before reporting
                    record.headers().addString("connect.ai.error.message", e.getMessage());
                    record.headers().addString("connect.ai.error.class", e.getClass().getName());
                    record.headers().addString("connect.ai.error.timestamp",
                            java.time.Instant.now().toString());
                    reporter.report(record, e);
                }
            } else {
                throw e;
            }
        }
    }

    @Override
    public void flush(Map<org.apache.kafka.common.TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> offsets) {
        if (adapter != null) {
            adapter.flush();
        }
    }

    @Override
    public Map<org.apache.kafka.common.TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> preCommit(
            Map<org.apache.kafka.common.TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> offsets) {
        // Flush before committing offsets to ensure exactly-once delivery
        flush(offsets);
        return offsets;
    }

    @Override
    public void stop() {
        if (pipeline != null) {
            try {
                pipeline.close();
            } catch (Exception e) {
                log.warn("Error closing pipeline: {}", e.getMessage());
            }
        }
        if (adapter != null) {
            adapter.stop();
        }
        LogContext.clear();
        log.info("AiSinkTask stopped");
    }
}
