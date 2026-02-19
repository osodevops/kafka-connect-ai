package sh.oso.connect.ai.adapter.redis;

import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.params.XAddParams;
import sh.oso.connect.ai.adapter.redis.config.RedisSinkConfig;
import sh.oso.connect.ai.api.adapter.SinkAdapter;
import sh.oso.connect.ai.api.config.AiConnectConfig;
import sh.oso.connect.ai.api.error.NonRetryableException;
import sh.oso.connect.ai.api.error.RetryableException;
import sh.oso.connect.ai.api.model.TransformedRecord;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class RedisSinkAdapter implements SinkAdapter {

    private static final Logger log = LoggerFactory.getLogger(RedisSinkAdapter.class);

    private JedisPooled jedis;
    private RedisSinkConfig config;
    private final AtomicBoolean running = new AtomicBoolean(false);

    @Override
    public String type() {
        return "redis";
    }

    @Override
    public ConfigDef configDef() {
        return new ConfigDef()
                .define(AiConnectConfig.REDIS_URL, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                        ConfigDef.Importance.HIGH, "Redis connection URL")
                .define(AiConnectConfig.REDIS_SINK_MODE, ConfigDef.Type.STRING, "stream",
                        ConfigDef.Importance.HIGH, "Sink mode: stream, set, publish")
                .define(AiConnectConfig.REDIS_KEY, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                        ConfigDef.Importance.HIGH, "Redis key (stream/set mode)")
                .define(AiConnectConfig.REDIS_CHANNEL, ConfigDef.Type.STRING, "",
                        ConfigDef.Importance.HIGH, "Redis channel (publish mode)")
                .define(AiConnectConfig.REDIS_BATCH_SIZE, ConfigDef.Type.INT, 500,
                        ConfigDef.Importance.MEDIUM, "Batch size for pipeline operations");
    }

    @Override
    public void start(Map<String, String> props) {
        this.config = new RedisSinkConfig(props);
        this.jedis = new JedisPooled(config.url());
        this.running.set(true);

        String mode = config.mode();
        if (!"stream".equals(mode) && !"set".equals(mode) && !"publish".equals(mode)) {
            throw new NonRetryableException("Unknown redis sink mode: " + mode);
        }

        log.info("RedisSinkAdapter started: url={}, mode={}", config.url(), mode);
    }

    @Override
    public void write(List<TransformedRecord> records) {
        if (records.isEmpty()) {
            return;
        }

        try {
            switch (config.mode()) {
                case "stream" -> writeStream(records);
                case "set" -> writeSet(records);
                case "publish" -> writePublish(records);
                default -> throw new NonRetryableException("Unknown redis sink mode: " + config.mode());
            }
            log.debug("Wrote {} records in '{}' mode", records.size(), config.mode());
        } catch (NonRetryableException e) {
            throw e;
        } catch (Exception e) {
            throw new RetryableException("Failed to write to Redis: " + e.getMessage(), e);
        }
    }

    private void writeStream(List<TransformedRecord> records) {
        Pipeline pipeline = jedis.pipelined();
        for (TransformedRecord record : records) {
            Map<String, String> fields = new HashMap<>();
            fields.put("value", new String(record.value(), StandardCharsets.UTF_8));
            if (record.key() != null && record.key().length > 0) {
                fields.put("key", new String(record.key(), StandardCharsets.UTF_8));
            }
            for (Map.Entry<String, String> header : record.headers().entrySet()) {
                fields.put("header." + header.getKey(), header.getValue());
            }
            pipeline.xadd(config.key(), XAddParams.xAddParams(), fields);
        }
        pipeline.sync();
    }

    private void writeSet(List<TransformedRecord> records) {
        Pipeline pipeline = jedis.pipelined();
        for (TransformedRecord record : records) {
            String key;
            if (record.key() != null && record.key().length > 0) {
                key = new String(record.key(), StandardCharsets.UTF_8);
            } else {
                key = config.key();
            }
            String value = new String(record.value(), StandardCharsets.UTF_8);
            pipeline.set(key, value);
        }
        pipeline.sync();
    }

    private void writePublish(List<TransformedRecord> records) {
        Pipeline pipeline = jedis.pipelined();
        String channel = config.channel();
        for (TransformedRecord record : records) {
            String message = new String(record.value(), StandardCharsets.UTF_8);
            pipeline.publish(channel, message);
        }
        pipeline.sync();
    }

    @Override
    public void flush() {
        // Writes are committed via pipeline.sync() within write()
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

        if (jedis != null) {
            try {
                jedis.close();
            } catch (Exception e) {
                log.debug("Error closing Jedis connection: {}", e.getMessage());
            }
        }

        log.info("RedisSinkAdapter stopped");
    }
}
