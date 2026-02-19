package sh.oso.connect.ai.adapter.kinesis;

import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.connect.ai.adapter.kinesis.config.KinesisSourceConfig;
import sh.oso.connect.ai.api.adapter.SourceAdapter;
import sh.oso.connect.ai.api.config.AiConnectConfig;
import sh.oso.connect.ai.api.error.RetryableException;
import sh.oso.connect.ai.api.model.RawRecord;
import sh.oso.connect.ai.api.model.SourceOffset;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsResponse;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class KinesisSourceAdapter implements SourceAdapter {

    private static final Logger log = LoggerFactory.getLogger(KinesisSourceAdapter.class);

    private KinesisClient kinesisClient;
    private KinesisSourceConfig config;
    private final Map<String, String> shardIterators = new ConcurrentHashMap<>();
    private final Map<String, String> shardSequenceNumbers = new ConcurrentHashMap<>();
    private long lastShardRefreshTimestamp;
    private long lastPollTimestamp;

    private static final long SHARD_REFRESH_INTERVAL_MS = 60_000;

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
                .define(AiConnectConfig.KINESIS_ITERATOR_TYPE, ConfigDef.Type.STRING,
                        "TRIM_HORIZON", ConfigDef.Importance.MEDIUM,
                        "Shard iterator type: TRIM_HORIZON, LATEST, AT_SEQUENCE_NUMBER, AFTER_SEQUENCE_NUMBER")
                .define(AiConnectConfig.KINESIS_POLL_INTERVAL_MS, ConfigDef.Type.LONG,
                        1000L, ConfigDef.Importance.MEDIUM,
                        "Poll interval in milliseconds")
                .define(AiConnectConfig.KINESIS_BATCH_SIZE, ConfigDef.Type.INT,
                        500, ConfigDef.Importance.MEDIUM,
                        "Maximum records to fetch per shard per poll");
    }

    @Override
    public void start(Map<String, String> props) {
        this.config = new KinesisSourceConfig(props);
        this.kinesisClient = KinesisClient.builder()
                .region(Region.of(config.region()))
                .build();
        this.lastShardRefreshTimestamp = 0;
        this.lastPollTimestamp = 0;
        refreshShards(null);
        log.info("KinesisSourceAdapter started: stream={}, region={}, iteratorType={}",
                config.streamName(), config.region(), config.iteratorType());
    }

    @Override
    public List<RawRecord> fetch(SourceOffset currentOffset, int maxRecords) throws InterruptedException {
        try {
            long now = System.currentTimeMillis();
            long elapsed = now - lastPollTimestamp;
            if (elapsed < config.pollIntervalMs()) {
                Thread.sleep(config.pollIntervalMs() - elapsed);
            }
            lastPollTimestamp = System.currentTimeMillis();

            if (System.currentTimeMillis() - lastShardRefreshTimestamp > SHARD_REFRESH_INTERVAL_MS) {
                refreshShards(currentOffset);
            }

            List<RawRecord> allRecords = new ArrayList<>();
            List<String> completedShards = new ArrayList<>();

            for (Map.Entry<String, String> entry : shardIterators.entrySet()) {
                if (allRecords.size() >= maxRecords) {
                    break;
                }

                String shardId = entry.getKey();
                String shardIterator = entry.getValue();

                if (shardIterator == null) {
                    continue;
                }

                int remaining = maxRecords - allRecords.size();
                int limit = Math.min(remaining, config.batchSize());

                GetRecordsResponse response = kinesisClient.getRecords(
                        GetRecordsRequest.builder()
                                .shardIterator(shardIterator)
                                .limit(limit)
                                .build());

                for (Record record : response.records()) {
                    byte[] value = record.data().asByteArray();
                    byte[] key = record.partitionKey() != null
                            ? record.partitionKey().getBytes(StandardCharsets.UTF_8) : null;

                    Map<String, String> partition = Map.of(
                            "adapter", "kinesis",
                            "stream", config.streamName(),
                            "shardId", shardId);
                    Map<String, Object> offset = Map.of(
                            "shardId", shardId,
                            "sequenceNumber", record.sequenceNumber());

                    Map<String, String> metadata = Map.of(
                            "source.stream", config.streamName(),
                            "source.shardId", shardId,
                            "source.sequenceNumber", record.sequenceNumber(),
                            "source.partitionKey", record.partitionKey() != null ? record.partitionKey() : "");

                    allRecords.add(new RawRecord(key, value, metadata, new SourceOffset(partition, offset)));
                    shardSequenceNumbers.put(shardId, record.sequenceNumber());
                }

                String nextIterator = response.nextShardIterator();
                if (nextIterator == null) {
                    log.info("Shard {} has reached SHARD_END", shardId);
                    completedShards.add(shardId);
                } else {
                    shardIterators.put(shardId, nextIterator);
                }
            }

            for (String completedShard : completedShards) {
                shardIterators.remove(completedShard);
            }

            return allRecords;
        } catch (InterruptedException e) {
            throw e;
        } catch (Exception e) {
            throw new RetryableException("Failed to fetch from Kinesis: " + e.getMessage(), e);
        }
    }

    private void refreshShards(SourceOffset currentOffset) {
        try {
            ListShardsResponse response = kinesisClient.listShards(
                    ListShardsRequest.builder()
                            .streamName(config.streamName())
                            .build());

            for (Shard shard : response.shards()) {
                String shardId = shard.shardId();
                if (!shardIterators.containsKey(shardId)) {
                    String iterator = getInitialIterator(shardId, currentOffset);
                    if (iterator != null) {
                        shardIterators.put(shardId, iterator);
                    }
                }
            }

            lastShardRefreshTimestamp = System.currentTimeMillis();
            log.debug("Refreshed shards for stream {}: {} active shards",
                    config.streamName(), shardIterators.size());
        } catch (Exception e) {
            log.warn("Failed to refresh shards for stream {}: {}",
                    config.streamName(), e.getMessage());
        }
    }

    private String getInitialIterator(String shardId, SourceOffset currentOffset) {
        GetShardIteratorRequest.Builder builder = GetShardIteratorRequest.builder()
                .streamName(config.streamName())
                .shardId(shardId);

        String resumeSequence = null;
        if (currentOffset != null && currentOffset.offset() != null) {
            Object seqObj = currentOffset.offset().get("sequenceNumber");
            String offsetShardId = (String) currentOffset.offset().get("shardId");
            if (seqObj != null && shardId.equals(offsetShardId)) {
                resumeSequence = seqObj.toString();
            }
        }
        if (resumeSequence == null) {
            resumeSequence = shardSequenceNumbers.get(shardId);
        }

        if (resumeSequence != null) {
            builder.shardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER)
                    .startingSequenceNumber(resumeSequence);
        } else {
            builder.shardIteratorType(ShardIteratorType.fromValue(config.iteratorType()));
        }

        try {
            return kinesisClient.getShardIterator(builder.build()).shardIterator();
        } catch (Exception e) {
            log.warn("Failed to get shard iterator for shard {}: {}", shardId, e.getMessage());
            return null;
        }
    }

    @Override
    public void commitOffset(SourceOffset offset) {
        if (offset == null || offset.offset() == null) {
            return;
        }
        String shardId = (String) offset.offset().get("shardId");
        Object seqObj = offset.offset().get("sequenceNumber");
        if (shardId != null && seqObj != null) {
            shardSequenceNumbers.put(shardId, seqObj.toString());
        }
    }

    @Override
    public boolean isHealthy() {
        if (kinesisClient == null) {
            return false;
        }
        try {
            kinesisClient.listShards(
                    ListShardsRequest.builder()
                            .streamName(config.streamName())
                            .maxResults(1)
                            .build());
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public void stop() {
        shardIterators.clear();
        shardSequenceNumbers.clear();
        if (kinesisClient != null) {
            kinesisClient.close();
            kinesisClient = null;
        }
        log.info("KinesisSourceAdapter stopped");
    }
}
