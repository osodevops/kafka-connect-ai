package sh.oso.connect.ai.adapter.redis;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class RedisSourceAdapterTest {

    @Test
    void typeReturnsRedis() {
        RedisSourceAdapter adapter = new RedisSourceAdapter();
        assertEquals("redis", adapter.type());
    }

    @Test
    void configDefContainsRedisUrl() {
        RedisSourceAdapter adapter = new RedisSourceAdapter();
        assertNotNull(adapter.configDef().configKeys().get("redis.url"));
    }

    @Test
    void configDefContainsSourceMode() {
        RedisSourceAdapter adapter = new RedisSourceAdapter();
        assertNotNull(adapter.configDef().configKeys().get("redis.source.mode"));
    }

    @Test
    void configDefContainsKey() {
        RedisSourceAdapter adapter = new RedisSourceAdapter();
        assertNotNull(adapter.configDef().configKeys().get("redis.key"));
    }

    @Test
    void configDefContainsChannel() {
        RedisSourceAdapter adapter = new RedisSourceAdapter();
        assertNotNull(adapter.configDef().configKeys().get("redis.channel"));
    }

    @Test
    void configDefContainsGroup() {
        RedisSourceAdapter adapter = new RedisSourceAdapter();
        assertNotNull(adapter.configDef().configKeys().get("redis.group"));
    }

    @Test
    void configDefContainsConsumer() {
        RedisSourceAdapter adapter = new RedisSourceAdapter();
        assertNotNull(adapter.configDef().configKeys().get("redis.consumer"));
    }

    @Test
    void configDefContainsPollInterval() {
        RedisSourceAdapter adapter = new RedisSourceAdapter();
        assertNotNull(adapter.configDef().configKeys().get("redis.poll.interval.ms"));
    }

    @Test
    void configDefContainsBatchSize() {
        RedisSourceAdapter adapter = new RedisSourceAdapter();
        assertNotNull(adapter.configDef().configKeys().get("redis.batch.size"));
    }

    @Test
    void isHealthyReturnsFalseBeforeStart() {
        RedisSourceAdapter adapter = new RedisSourceAdapter();
        assertFalse(adapter.isHealthy());
    }

    @Test
    void stopIsIdempotent() {
        RedisSourceAdapter adapter = new RedisSourceAdapter();
        assertDoesNotThrow(() -> {
            adapter.stop();
            adapter.stop();
        });
    }
}
