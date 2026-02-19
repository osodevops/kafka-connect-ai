package sh.oso.connect.ai.adapter.redis;

import org.junit.jupiter.api.Test;
import sh.oso.connect.ai.api.model.TransformedRecord;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class RedisSinkAdapterTest {

    @Test
    void typeReturnsRedis() {
        RedisSinkAdapter adapter = new RedisSinkAdapter();
        assertEquals("redis", adapter.type());
    }

    @Test
    void configDefContainsRedisUrl() {
        RedisSinkAdapter adapter = new RedisSinkAdapter();
        assertNotNull(adapter.configDef().configKeys().get("redis.url"));
    }

    @Test
    void configDefContainsSinkMode() {
        RedisSinkAdapter adapter = new RedisSinkAdapter();
        assertNotNull(adapter.configDef().configKeys().get("redis.sink.mode"));
    }

    @Test
    void configDefContainsKey() {
        RedisSinkAdapter adapter = new RedisSinkAdapter();
        assertNotNull(adapter.configDef().configKeys().get("redis.key"));
    }

    @Test
    void configDefContainsChannel() {
        RedisSinkAdapter adapter = new RedisSinkAdapter();
        assertNotNull(adapter.configDef().configKeys().get("redis.channel"));
    }

    @Test
    void configDefContainsBatchSize() {
        RedisSinkAdapter adapter = new RedisSinkAdapter();
        assertNotNull(adapter.configDef().configKeys().get("redis.batch.size"));
    }

    @Test
    void writeEmptyListIsNoOp() {
        RedisSinkAdapter adapter = new RedisSinkAdapter();
        assertDoesNotThrow(() -> adapter.write(Collections.emptyList()));
    }

    @Test
    void isHealthyReturnsFalseBeforeStart() {
        RedisSinkAdapter adapter = new RedisSinkAdapter();
        assertFalse(adapter.isHealthy());
    }

    @Test
    void stopIsIdempotent() {
        RedisSinkAdapter adapter = new RedisSinkAdapter();
        assertDoesNotThrow(() -> {
            adapter.stop();
            adapter.stop();
        });
    }
}
