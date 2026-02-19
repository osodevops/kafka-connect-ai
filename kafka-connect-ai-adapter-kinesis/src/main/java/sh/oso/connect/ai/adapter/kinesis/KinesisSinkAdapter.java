package sh.oso.connect.ai.adapter.kinesis;

import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.connect.ai.adapter.kinesis.config.KinesisSinkConfig;
import sh.oso.connect.ai.api.adapter.SinkAdapter;
import sh.oso.connect.ai.api.config.AiConnectConfig;
import sh.oso.connect.ai.api.error.RetryableException;
import sh.oso.connect.ai.api.model.TransformedRecord;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResultEntry;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class KinesisSinkAdapter implements SinkAdapter {

    private static final Logger log = LoggerFactory.getLogger(KinesisSinkAdapter.class);

    private KinesisClient kinesisClient;
    private KinesisSinkConfig config;

    private static final int MAX_PUT_RECORDS_BATCH = 500;

    @Override
    public String type() {
        return "kinesis";
    }

    @Override
    public ConfigDef configDef() {
        return new ConfigDef()
                .define(AiConnectConfig.KINESIS_STREAM_NAME, ConfigDef.Type.STRING,
                        ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH,
                        "Kinesis stream name")
                .define(AiConnectConfig.KINESIS_REGION, ConfigDef.Type.STRING,
                        "us-east-1", ConfigDef.Importance.HIGH,
                        "AWS region for Kinesis")
                .define(AiConnectConfig.KINESIS_PARTITION_KEY, ConfigDef.Type.STRING,
                        "default", ConfigDef.Importance.MEDIUM,
                        "Partition key for records (used when record key is null)")
                .define(AiConnectConfig.KINESIS_BATCH_SIZE, ConfigDef.Type.INT,
                        500, ConfigDef.Importance.MEDIUM,
                        "Maximum records per putRecords batch (max 500)");
    }

    @Override
    public void start(Map<String, String> props) {
        this.config = new KinesisSinkConfig(props);
        this.kinesisClient = KinesisClient.builder()
                .region(Region.of(config.region()))
                .build();
        log.info("KinesisSinkAdapter started: stream={}, region={}",
                config.streamName(), config.region());
    }

    @Override
    public void write(List<TransformedRecord> records) {
        if (records.isEmpty()) {
            return;
        }

        try {
            List<PutRecordsRequestEntry> batch = new ArrayList<>();
            for (TransformedRecord record : records) {
                String partitionKey = resolvePartitionKey(record);

                PutRecordsRequestEntry entry = PutRecordsRequestEntry.builder()
                        .data(SdkBytes.fromByteArray(record.value()))
                        .partitionKey(partitionKey)
                        .build();

                batch.add(entry);

                if (batch.size() >= MAX_PUT_RECORDS_BATCH) {
                    sendBatch(batch);
                    batch = new ArrayList<>();
                }
            }

            if (!batch.isEmpty()) {
                sendBatch(batch);
            }

            log.debug("Wrote {} records to Kinesis stream {}", records.size(), config.streamName());
        } catch (RetryableException e) {
            throw e;
        } catch (Exception e) {
            throw new RetryableException("Failed to write to Kinesis: " + e.getMessage(), e);
        }
    }

    private void sendBatch(List<PutRecordsRequestEntry> entries) {
        List<PutRecordsRequestEntry> pending = new ArrayList<>(entries);
        int maxRetries = 3;

        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            PutRecordsResponse response = kinesisClient.putRecords(
                    PutRecordsRequest.builder()
                            .streamName(config.streamName())
                            .records(pending)
                            .build());

            if (response.failedRecordCount() == 0) {
                return;
            }

            log.warn("Partial failure writing to Kinesis: {} of {} records failed (attempt {}/{})",
                    response.failedRecordCount(), pending.size(), attempt + 1, maxRetries + 1);

            if (attempt == maxRetries) {
                throw new RetryableException(
                        "Failed to write " + response.failedRecordCount() + " records to Kinesis after "
                                + (maxRetries + 1) + " attempts");
            }

            List<PutRecordsRequestEntry> retryEntries = new ArrayList<>();
            List<PutRecordsResultEntry> results = response.records();
            for (int i = 0; i < results.size(); i++) {
                if (results.get(i).errorCode() != null) {
                    retryEntries.add(pending.get(i));
                }
            }
            pending = retryEntries;
        }
    }

    private String resolvePartitionKey(TransformedRecord record) {
        if (record.key() != null && record.key().length > 0) {
            return new String(record.key(), StandardCharsets.UTF_8);
        }
        return config.partitionKey();
    }

    @Override
    public void flush() {
        // Writes are committed within write()
    }

    @Override
    public boolean isHealthy() {
        if (kinesisClient == null) {
            return false;
        }
        try {
            kinesisClient.describeStream(b -> b.streamName(config.streamName()));
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public void stop() {
        if (kinesisClient != null) {
            kinesisClient.close();
            kinesisClient = null;
        }
        log.info("KinesisSinkAdapter stopped");
    }
}
