package sh.oso.connect.ai.adapter.redis;

import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.params.XReadGroupParams;
import redis.clients.jedis.resps.StreamEntry;
import sh.oso.connect.ai.adapter.redis.config.RedisSourceConfig;
import sh.oso.connect.ai.api.adapter.SourceAdapter;
import sh.oso.connect.ai.api.config.AiConnectConfig;
import sh.oso.connect.ai.api.error.NonRetryableException;
import sh.oso.connect.ai.api.error.RetryableException;
import sh.oso.connect.ai.api.model.RawRecord;
import sh.oso.connect.ai.api.model.SourceOffset;
import sh.oso.connect.ai.api.util.BoundedRecordBuffer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class RedisSourceAdapter implements SourceAdapter {

    private static final Logger log = LoggerFactory.getLogger(RedisSourceAdapter.class);

    private JedisPooled jedis;
    private RedisSourceConfig config;
    private final AtomicBoolean running = new AtomicBoolean(false);

    // PubSub mode
    private BoundedRecordBuffer pubsubBuffer;
    private JedisPubSub pubsubListener;
    private Thread pubsubThread;

    @Override
    public String type() {
        return "redis";
    }

    @Override
    public ConfigDef configDef() {
        return new ConfigDef()
                .define(AiConnectConfig.REDIS_URL, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                        ConfigDef.Importance.HIGH, "Redis connection URL")
                .define(AiConnectConfig.REDIS_SOURCE_MODE, ConfigDef.Type.STRING, "stream",
                        ConfigDef.Importance.HIGH, "Source mode: stream, pubsub")
                .define(AiConnectConfig.REDIS_KEY, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                        ConfigDef.Importance.HIGH, "Redis stream key (stream mode)")
                .define(AiConnectConfig.REDIS_CHANNEL, ConfigDef.Type.STRING, "",
                        ConfigDef.Importance.HIGH, "Redis channel (pubsub mode)")
                .define(AiConnectConfig.REDIS_GROUP, ConfigDef.Type.STRING, "connect-ai-group",
                        ConfigDef.Importance.MEDIUM, "Consumer group name (stream mode)")
                .define(AiConnectConfig.REDIS_CONSUMER, ConfigDef.Type.STRING, "connect-ai-consumer",
                        ConfigDef.Importance.MEDIUM, "Consumer name within group (stream mode)")
                .define(AiConnectConfig.REDIS_POLL_INTERVAL_MS, ConfigDef.Type.LONG, 1000L,
                        ConfigDef.Importance.MEDIUM, "Poll interval in milliseconds")
                .define(AiConnectConfig.REDIS_BATCH_SIZE, ConfigDef.Type.INT, 500,
                        ConfigDef.Importance.MEDIUM, "Maximum records per fetch");
    }

    @Override
    public void start(Map<String, String> props) {
        this.config = new RedisSourceConfig(props);
        this.jedis = new JedisPooled(config.url());
        this.running.set(true);

        String mode = config.mode();
        switch (mode) {
            case "stream" -> startStreamMode();
            case "pubsub" -> startPubSubMode();
            default -> throw new NonRetryableException("Unknown redis source mode: " + mode);
        }

        log.info("RedisSourceAdapter started: url={}, mode={}", config.url(), mode);
    }

    private void startStreamMode() {
        try {
            jedis.xgroupCreate(config.key(), config.group(), StreamEntryID.LAST_ENTRY, true);
            log.info("Created consumer group '{}' on stream '{}'", config.group(), config.key());
        } catch (Exception e) {
            if (e.getMessage() != null && e.getMessage().contains("BUSYGROUP")) {
                log.debug("Consumer group '{}' already exists on stream '{}'", config.group(), config.key());
            } else {
                throw new RetryableException("Failed to create consumer group: " + e.getMessage(), e);
            }
        }
    }

    private void startPubSubMode() {
        this.pubsubBuffer = new BoundedRecordBuffer();

        this.pubsubListener = new JedisPubSub() {
            @Override
            public void onMessage(String channel, String message) {
                if (!running.get()) {
                    return;
                }
                RawRecord record = new RawRecord(
                        channel.getBytes(StandardCharsets.UTF_8),
                        message.getBytes(StandardCharsets.UTF_8),
                        Map.of("redis.channel", channel),
                        SourceOffset.empty()
                );
                if (!pubsubBuffer.offer(record)) {
                    log.warn("PubSub buffer full, dropping message from channel '{}'", channel);
                }
            }
        };

        this.pubsubThread = new Thread(() -> {
            while (running.get()) {
                try {
                    jedis.subscribe(pubsubListener, config.channel());
                } catch (Exception e) {
                    if (running.get()) {
                        log.warn("PubSub subscription interrupted, reconnecting: {}", e.getMessage());
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                    }
                }
            }
        }, "redis-pubsub-listener");
        this.pubsubThread.setDaemon(true);
        this.pubsubThread.start();

        log.info("PubSub subscription started on channel '{}'", config.channel());
    }

    @Override
    public List<RawRecord> fetch(SourceOffset currentOffset, int maxRecords) throws InterruptedException {
        return switch (config.mode()) {
            case "stream" -> fetchFromStream(currentOffset, maxRecords);
            case "pubsub" -> fetchFromPubSub(maxRecords);
            default -> List.of();
        };
    }

    private List<RawRecord> fetchFromStream(SourceOffset currentOffset, int maxRecords) {
        int count = Math.min(maxRecords, config.batchSize());
        List<RawRecord> records = new ArrayList<>();

        try {
            // Phase 1: Read pending entries (entries delivered but not yet acknowledged)
            Map<String, StreamEntryID> pendingStreams = new HashMap<>();
            pendingStreams.put(config.key(), new StreamEntryID("0-0"));

            XReadGroupParams pendingParams = new XReadGroupParams()
                    .count(count);

            List<Map.Entry<String, List<StreamEntry>>> pendingResult = jedis.xreadGroup(
                    config.group(), config.consumer(), pendingParams, pendingStreams);

            if (pendingResult != null) {
                for (Map.Entry<String, List<StreamEntry>> entry : pendingResult) {
                    for (StreamEntry streamEntry : entry.getValue()) {
                        records.add(toRawRecord(entry.getKey(), streamEntry));
                    }
                }
            }

            // ACK and return pending entries before reading new ones
            if (!records.isEmpty()) {
                ackEntries(pendingResult);
                log.debug("Fetched {} pending records from stream '{}'", records.size(), config.key());
                return records;
            }

            // Phase 2: Read new entries
            Map<String, StreamEntryID> newStreams = new HashMap<>();
            newStreams.put(config.key(), StreamEntryID.UNRECEIVED_ENTRY);

            long blockMs = Math.max(config.pollIntervalMs(), 100);
            XReadGroupParams newParams = new XReadGroupParams()
                    .count(count)
                    .block((int) blockMs);

            List<Map.Entry<String, List<StreamEntry>>> newResult = jedis.xreadGroup(
                    config.group(), config.consumer(), newParams, newStreams);

            if (newResult != null) {
                for (Map.Entry<String, List<StreamEntry>> entry : newResult) {
                    for (StreamEntry streamEntry : entry.getValue()) {
                        records.add(toRawRecord(entry.getKey(), streamEntry));
                    }
                }
                ackEntries(newResult);
            }

            log.debug("Fetched {} new records from stream '{}'", records.size(), config.key());
        } catch (Exception e) {
            throw new RetryableException("Failed to read from Redis stream: " + e.getMessage(), e);
        }

        return records;
    }

    private void ackEntries(List<Map.Entry<String, List<StreamEntry>>> result) {
        for (Map.Entry<String, List<StreamEntry>> entry : result) {
            StreamEntryID[] ids = entry.getValue().stream()
                    .map(StreamEntry::getID)
                    .toArray(StreamEntryID[]::new);
            if (ids.length > 0) {
                jedis.xack(entry.getKey(), config.group(), ids);
            }
        }
    }

    private RawRecord toRawRecord(String streamKey, StreamEntry entry) {
        Map<String, String> fields = entry.getFields();
        StringBuilder valueBuilder = new StringBuilder("{");
        int i = 0;
        for (Map.Entry<String, String> field : fields.entrySet()) {
            if (i > 0) {
                valueBuilder.append(",");
            }
            valueBuilder.append("\"").append(escapeJson(field.getKey())).append("\":");
            valueBuilder.append("\"").append(escapeJson(field.getValue())).append("\"");
            i++;
        }
        valueBuilder.append("}");

        Map<String, String> metadata = Map.of(
                "redis.stream", streamKey,
                "redis.entry.id", entry.getID().toString()
        );

        SourceOffset offset = new SourceOffset(
                Map.of("adapter", "redis", "stream", streamKey),
                Map.of("lastId", entry.getID().toString())
        );

        return new RawRecord(
                entry.getID().toString().getBytes(StandardCharsets.UTF_8),
                valueBuilder.toString().getBytes(StandardCharsets.UTF_8),
                metadata,
                offset
        );
    }

    private String escapeJson(String value) {
        return value.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
    }

    private List<RawRecord> fetchFromPubSub(int maxRecords) throws InterruptedException {
        int count = Math.min(maxRecords, config.batchSize());
        return pubsubBuffer.drain(count, Duration.ofMillis(config.pollIntervalMs()));
    }

    @Override
    public void commitOffset(SourceOffset offset) {
        if (!"stream".equals(config.mode())) {
            return;
        }

        Object lastId = offset.offset().get("lastId");
        if (lastId == null) {
            return;
        }

        try {
            jedis.xack(config.key(), config.group(), new StreamEntryID(lastId.toString()));
            log.debug("Acknowledged stream entry: {}", lastId);
        } catch (Exception e) {
            log.warn("Failed to acknowledge stream entry {}: {}", lastId, e.getMessage());
        }
    }

    @Override
    public boolean isHealthy() {
        if (jedis == null || !running.get()) {
            return false;
        }
        try {
            String pong = jedis.ping();
            return "PONG".equalsIgnoreCase(pong);
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public void stop() {
        running.set(false);

        if (pubsubListener != null && pubsubListener.isSubscribed()) {
            try {
                pubsubListener.unsubscribe();
            } catch (Exception e) {
                log.debug("Error unsubscribing pubsub listener: {}", e.getMessage());
            }
        }

        if (pubsubThread != null) {
            pubsubThread.interrupt();
        }

        if (pubsubBuffer != null) {
            pubsubBuffer.clear();
        }

        if (jedis != null) {
            try {
                jedis.close();
            } catch (Exception e) {
                log.debug("Error closing Jedis connection: {}", e.getMessage());
            }
        }

        log.info("RedisSourceAdapter stopped");
    }
}
