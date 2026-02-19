package sh.oso.nexus.connect.sink;

import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.nexus.api.adapter.SinkAdapter;
import sh.oso.nexus.api.config.NexusConfig;
import sh.oso.nexus.api.model.RawRecord;
import sh.oso.nexus.api.model.SourceOffset;
import sh.oso.nexus.api.model.TransformedRecord;
import sh.oso.nexus.api.pipeline.AgentPipeline;
import sh.oso.nexus.connect.config.NexusSinkConfig;
import sh.oso.nexus.connect.metrics.LogContext;
import sh.oso.nexus.connect.metrics.NexusMetrics;
import sh.oso.nexus.connect.pipeline.BasicAgentPipeline;
import sh.oso.nexus.connect.spi.AdapterRegistry;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class NexusSinkTask extends SinkTask {

    private static final Logger log = LoggerFactory.getLogger(NexusSinkTask.class);

    private SinkAdapter adapter;
    private AgentPipeline pipeline;
    private ErrantRecordReporter reporter;
    private NexusMetrics metrics;

    @Override
    public String version() {
        return "0.1.0";
    }

    @Override
    public void start(Map<String, String> props) {
        NexusSinkConfig config = new NexusSinkConfig(props);

        AdapterRegistry registry = new AdapterRegistry();
        this.adapter = registry.getSinkAdapter(config.sinkAdapterType());
        this.adapter.start(props);

        this.pipeline = new BasicAgentPipeline();
        this.pipeline.configure(props);

        this.metrics = NexusMetrics.getInstance();
        LogContext.set(config.sinkAdapterType(), "sink", config.sinkAdapterType(),
                props.getOrDefault(NexusConfig.LLM_MODEL, ""));

        try {
            this.reporter = context.errantRecordReporter();
        } catch (Exception e) {
            log.warn("ErrantRecordReporter not available, DLQ disabled");
            this.reporter = null;
        }

        log.info("NexusSinkTask started with adapter={}", config.sinkAdapterType());
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
                    record.headers().addString("nexus.error.message", e.getMessage());
                    record.headers().addString("nexus.error.class", e.getClass().getName());
                    record.headers().addString("nexus.error.timestamp",
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
        if (adapter != null) {
            adapter.stop();
        }
        LogContext.clear();
        log.info("NexusSinkTask stopped");
    }
}
