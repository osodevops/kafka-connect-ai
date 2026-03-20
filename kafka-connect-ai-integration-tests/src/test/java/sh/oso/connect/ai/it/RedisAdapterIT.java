package sh.oso.connect.ai.it;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.params.XReadGroupParams;
import sh.oso.connect.ai.adapter.redis.RedisSinkAdapter;
import sh.oso.connect.ai.adapter.redis.RedisSourceAdapter;
import sh.oso.connect.ai.api.model.RawRecord;
import sh.oso.connect.ai.api.model.SourceOffset;
import sh.oso.connect.ai.api.model.TransformedRecord;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for Redis source and sink adapters using Testcontainers.
 */
@Testcontainers
class RedisAdapterIT {

    static final ObjectMapper objectMapper = new ObjectMapper();

    @Container
    static final GenericContainer<?> redis = new GenericContainer<>(
            DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379);

    static JedisPooled jedis;
    static String redisUrl;

    @BeforeAll
    static void setUp() {
        redisUrl = "redis://" + redis.getHost() + ":" + redis.getMappedPort(6379);
        jedis = new JedisPooled(redis.getHost(), redis.getMappedPort(6379));
    }

    @AfterAll
    static void tearDown() {
        if (jedis != null) {
            jedis.close();
        }
    }

    @Test
    void sinkStreamModeWritesToRedisStream() throws Exception {
        RedisSinkAdapter sink = new RedisSinkAdapter();
        sink.start(Map.of(
                "redis.url", redisUrl,
                "redis.sink.mode", "stream",
                "redis.key", "test:output:stream"
        ));

        sink.write(List.of(
                new TransformedRecord(
                        "key1".getBytes(StandardCharsets.UTF_8),
                        "{\"name\":\"Alice\",\"score\":95}".getBytes(StandardCharsets.UTF_8),
                        Map.of(), SourceOffset.empty()),
                new TransformedRecord(
                        "key2".getBytes(StandardCharsets.UTF_8),
                        "{\"name\":\"Bob\",\"score\":87}".getBytes(StandardCharsets.UTF_8),
                        Map.of(), SourceOffset.empty())
        ));

        // Verify entries were written to the stream
        long streamLen = jedis.xlen("test:output:stream");
        assertEquals(2, streamLen);

        assertTrue(sink.isHealthy());
        sink.stop();
    }

    @Test
    void sinkSetModeWritesToRedisKeys() throws Exception {
        RedisSinkAdapter sink = new RedisSinkAdapter();
        sink.start(Map.of(
                "redis.url", redisUrl,
                "redis.sink.mode", "set"
        ));

        sink.write(List.of(
                new TransformedRecord(
                        "user:1".getBytes(StandardCharsets.UTF_8),
                        "{\"name\":\"Alice\"}".getBytes(StandardCharsets.UTF_8),
                        Map.of(), SourceOffset.empty()),
                new TransformedRecord(
                        "user:2".getBytes(StandardCharsets.UTF_8),
                        "{\"name\":\"Bob\"}".getBytes(StandardCharsets.UTF_8),
                        Map.of(), SourceOffset.empty())
        ));

        assertEquals("{\"name\":\"Alice\"}", jedis.get("user:1"));
        assertEquals("{\"name\":\"Bob\"}", jedis.get("user:2"));

        sink.stop();
    }

    @Test
    void sourceStreamModeReadsFromRedisStream() throws Exception {
        // Pre-populate a Redis stream
        String streamKey = "test:source:stream";
        jedis.xadd(streamKey, StreamEntryID.NEW_ENTRY, Map.of("data", "{\"event\":\"login\",\"user\":\"alice\"}"));
        jedis.xadd(streamKey, StreamEntryID.NEW_ENTRY, Map.of("data", "{\"event\":\"logout\",\"user\":\"bob\"}"));

        // Create consumer group
        try {
            jedis.xgroupCreate(streamKey, "test-group", new StreamEntryID("0-0"), true);
        } catch (Exception e) {
            // Group may already exist
        }

        RedisSourceAdapter source = new RedisSourceAdapter();
        source.start(Map.of(
                "redis.url", redisUrl,
                "redis.source.mode", "stream",
                "redis.key", streamKey,
                "redis.group", "test-group",
                "redis.consumer", "consumer-1",
                "redis.poll.interval.ms", "0"
        ));

        List<RawRecord> records = source.fetch(SourceOffset.empty(), 100);
        assertNotNull(records);
        assertEquals(2, records.size());

        String firstValue = new String(records.get(0).value(), StandardCharsets.UTF_8);
        assertTrue(firstValue.contains("login") || firstValue.contains("logout"));

        assertTrue(source.isHealthy());
        source.stop();
    }

    @Test
    void sourceResumeFromStreamOffset() throws Exception {
        String streamKey = "test:resume:stream";
        jedis.xadd(streamKey, StreamEntryID.NEW_ENTRY, Map.of("data", "msg1"));
        jedis.xadd(streamKey, StreamEntryID.NEW_ENTRY, Map.of("data", "msg2"));
        jedis.xadd(streamKey, StreamEntryID.NEW_ENTRY, Map.of("data", "msg3"));

        try {
            jedis.xgroupCreate(streamKey, "resume-group", new StreamEntryID("0-0"), true);
        } catch (Exception e) {
            // Group may already exist
        }

        RedisSourceAdapter source = new RedisSourceAdapter();
        source.start(Map.of(
                "redis.url", redisUrl,
                "redis.source.mode", "stream",
                "redis.key", streamKey,
                "redis.group", "resume-group",
                "redis.consumer", "consumer-1",
                "redis.poll.interval.ms", "0"
        ));

        // Fetch first batch
        List<RawRecord> first = source.fetch(SourceOffset.empty(), 2);
        assertEquals(2, first.size());

        // Resume - should get the remaining message
        SourceOffset resumeOffset = first.get(1).sourceOffset();
        List<RawRecord> second = source.fetch(resumeOffset, 100);
        assertEquals(1, second.size());

        source.stop();
    }

    @Test
    void roundTripSinkToSource() throws Exception {
        String streamKey = "test:roundtrip";

        // Write with sink
        RedisSinkAdapter sink = new RedisSinkAdapter();
        sink.start(Map.of(
                "redis.url", redisUrl,
                "redis.sink.mode", "stream",
                "redis.key", streamKey
        ));

        sink.write(List.of(
                new TransformedRecord(
                        "k1".getBytes(StandardCharsets.UTF_8),
                        "{\"processed\":true,\"result\":\"enriched data\"}".getBytes(StandardCharsets.UTF_8),
                        Map.of(), SourceOffset.empty())
        ));
        sink.stop();

        // Read with source
        try {
            jedis.xgroupCreate(streamKey, "roundtrip-group", new StreamEntryID("0-0"), true);
        } catch (Exception ignored) {}

        RedisSourceAdapter source = new RedisSourceAdapter();
        source.start(Map.of(
                "redis.url", redisUrl,
                "redis.source.mode", "stream",
                "redis.key", streamKey,
                "redis.group", "roundtrip-group",
                "redis.consumer", "consumer-1",
                "redis.poll.interval.ms", "0"
        ));

        List<RawRecord> records = source.fetch(SourceOffset.empty(), 10);
        assertEquals(1, records.size());

        String value = new String(records.get(0).value(), StandardCharsets.UTF_8);
        assertTrue(value.contains("enriched data"));

        source.stop();
    }
}
