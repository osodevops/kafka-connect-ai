package sh.oso.connect.ai.adapter.kinesis;

import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.connect.ai.adapter.kinesis.config.DynamoDbStreamConfig;
import sh.oso.connect.ai.api.adapter.SourceAdapter;
import sh.oso.connect.ai.api.config.AiConnectConfig;
import sh.oso.connect.ai.api.error.RetryableException;
import sh.oso.connect.ai.api.model.RawRecord;
import sh.oso.connect.ai.api.model.SourceOffset;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamResponse;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsRequest;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsResponse;
import software.amazon.awssdk.services.dynamodb.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.dynamodb.model.Record;
import software.amazon.awssdk.services.dynamodb.model.Shard;
import software.amazon.awssdk.services.dynamodb.model.ShardIteratorType;
import software.amazon.awssdk.services.dynamodb.model.StreamDescription;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DynamoDbStreamSourceAdapter implements SourceAdapter {

    private static final Logger log = LoggerFactory.getLogger(DynamoDbStreamSourceAdapter.class);

    private DynamoDbClient dynamoDbClient;
    private DynamoDbStreamsClient streamsClient;
    private DynamoDbStreamConfig config;
    private String streamArn;
    private final Map<String, String> shardIterators = new ConcurrentHashMap<>();
    private final Map<String, String> shardSequenceNumbers = new ConcurrentHashMap<>();
    private long lastShardRefreshTimestamp;
    private long lastPollTimestamp;

    private static final long SHARD_REFRESH_INTERVAL_MS = 60_000;

    @Override
    public String type() {
        return "dynamodb-streams";
    }

    @Override
    public ConfigDef configDef() {
        return new ConfigDef()
                .define(AiConnectConfig.DYNAMODB_TABLE_NAME, ConfigDef.Type.STRING,
                        ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH,
                        "DynamoDB table name")
                .define(AiConnectConfig.DYNAMODB_REGION, ConfigDef.Type.STRING,
                        "us-east-1", ConfigDef.Importance.HIGH,
                        "AWS region for DynamoDB")
                .define(AiConnectConfig.DYNAMODB_POLL_INTERVAL_MS, ConfigDef.Type.LONG,
                        1000L, ConfigDef.Importance.MEDIUM,
                        "Poll interval in milliseconds");
    }

    @Override
    public void start(Map<String, String> props) {
        this.config = new DynamoDbStreamConfig(props);
        Region region = Region.of(config.region());

        this.dynamoDbClient = DynamoDbClient.builder()
                .region(region)
                .build();
        this.streamsClient = DynamoDbStreamsClient.builder()
                .region(region)
                .build();

        this.streamArn = resolveStreamArn();
        this.lastShardRefreshTimestamp = 0;
        this.lastPollTimestamp = 0;
        refreshShards(null);
        log.info("DynamoDbStreamSourceAdapter started: table={}, region={}, streamArn={}",
                config.tableName(), config.region(), streamArn);
    }

    private String resolveStreamArn() {
        DescribeTableResponse response = dynamoDbClient.describeTable(
                DescribeTableRequest.builder()
                        .tableName(config.tableName())
                        .build());
        String arn = response.table().latestStreamArn();
        if (arn == null || arn.isEmpty()) {
            throw new IllegalStateException(
                    "DynamoDB table " + config.tableName() + " does not have streams enabled");
        }
        return arn;
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

                GetRecordsResponse response = streamsClient.getRecords(
                        GetRecordsRequest.builder()
                                .shardIterator(shardIterator)
                                .limit(Math.min(maxRecords - allRecords.size(), 1000))
                                .build());

                for (Record record : response.records()) {
                    byte[] value = serializeStreamRecord(record);
                    byte[] key = null;
                    if (record.dynamodb() != null && record.dynamodb().keys() != null) {
                        key = serializeKeys(record.dynamodb().keys());
                    }

                    Map<String, String> partition = Map.of(
                            "adapter", "dynamodb-streams",
                            "table", config.tableName(),
                            "shardId", shardId);
                    Map<String, Object> offset = Map.of(
                            "shardId", shardId,
                            "sequenceNumber", record.dynamodb().sequenceNumber());

                    Map<String, String> metadata = new HashMap<>();
                    metadata.put("source.table", config.tableName());
                    metadata.put("source.shardId", shardId);
                    metadata.put("source.sequenceNumber", record.dynamodb().sequenceNumber());
                    metadata.put("source.eventName", record.eventNameAsString() != null
                            ? record.eventNameAsString() : "UNKNOWN");

                    allRecords.add(new RawRecord(key, value, metadata, new SourceOffset(partition, offset)));
                    shardSequenceNumbers.put(shardId, record.dynamodb().sequenceNumber());
                }

                String nextIterator = response.nextShardIterator();
                if (nextIterator == null) {
                    log.info("DynamoDB Streams shard {} has reached SHARD_END", shardId);
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
            throw new RetryableException("Failed to fetch from DynamoDB Streams: " + e.getMessage(), e);
        }
    }

    private byte[] serializeStreamRecord(Record record) {
        StringBuilder sb = new StringBuilder("{");
        sb.append("\"eventName\":\"").append(record.eventNameAsString()).append("\"");
        if (record.dynamodb() != null) {
            if (record.dynamodb().newImage() != null) {
                sb.append(",\"newImage\":").append(attributeMapToJson(record.dynamodb().newImage()));
            }
            if (record.dynamodb().oldImage() != null) {
                sb.append(",\"oldImage\":").append(attributeMapToJson(record.dynamodb().oldImage()));
            }
            if (record.dynamodb().keys() != null) {
                sb.append(",\"keys\":").append(attributeMapToJson(record.dynamodb().keys()));
            }
        }
        sb.append("}");
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    private byte[] serializeKeys(Map<String, AttributeValue> keys) {
        return attributeMapToJson(keys).getBytes(StandardCharsets.UTF_8);
    }

    private String attributeMapToJson(Map<String, AttributeValue> attributes) {
        if (attributes == null || attributes.isEmpty()) {
            return "{}";
        }
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, AttributeValue> entry : attributes.entrySet()) {
            if (!first) {
                sb.append(",");
            }
            sb.append("\"").append(escapeJson(entry.getKey())).append("\":");
            sb.append(attributeValueToJson(entry.getValue()));
            first = false;
        }
        sb.append("}");
        return sb.toString();
    }

    private String attributeValueToJson(AttributeValue value) {
        if (value.s() != null) {
            return "\"" + escapeJson(value.s()) + "\"";
        } else if (value.n() != null) {
            return value.n();
        } else if (value.bool() != null) {
            return value.bool().toString();
        } else if (value.nul() != null && value.nul()) {
            return "null";
        } else {
            return "\"" + escapeJson(value.toString()) + "\"";
        }
    }

    private String escapeJson(String raw) {
        if (raw == null) {
            return "";
        }
        return raw.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
    }

    private void refreshShards(SourceOffset currentOffset) {
        try {
            DescribeStreamResponse response = streamsClient.describeStream(
                    DescribeStreamRequest.builder()
                            .streamArn(streamArn)
                            .build());

            StreamDescription description = response.streamDescription();
            for (Shard shard : description.shards()) {
                String shardId = shard.shardId();
                if (!shardIterators.containsKey(shardId)) {
                    String iterator = getInitialIterator(shardId, currentOffset);
                    if (iterator != null) {
                        shardIterators.put(shardId, iterator);
                    }
                }
            }

            lastShardRefreshTimestamp = System.currentTimeMillis();
            log.debug("Refreshed shards for DynamoDB stream {}: {} active shards",
                    config.tableName(), shardIterators.size());
        } catch (Exception e) {
            log.warn("Failed to refresh shards for DynamoDB stream {}: {}",
                    config.tableName(), e.getMessage());
        }
    }

    private String getInitialIterator(String shardId, SourceOffset currentOffset) {
        GetShardIteratorRequest.Builder builder = GetShardIteratorRequest.builder()
                .streamArn(streamArn)
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
                    .sequenceNumber(resumeSequence);
        } else {
            builder.shardIteratorType(ShardIteratorType.TRIM_HORIZON);
        }

        try {
            return streamsClient.getShardIterator(builder.build()).shardIterator();
        } catch (Exception e) {
            log.warn("Failed to get shard iterator for DynamoDB shard {}: {}", shardId, e.getMessage());
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
        if (streamsClient == null || dynamoDbClient == null) {
            return false;
        }
        try {
            dynamoDbClient.describeTable(
                    DescribeTableRequest.builder()
                            .tableName(config.tableName())
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
        if (streamsClient != null) {
            streamsClient.close();
            streamsClient = null;
        }
        if (dynamoDbClient != null) {
            dynamoDbClient.close();
            dynamoDbClient = null;
        }
        log.info("DynamoDbStreamSourceAdapter stopped");
    }
}
