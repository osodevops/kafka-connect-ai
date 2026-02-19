package sh.oso.connect.ai.connect.cache;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class SemanticCacheTest {

    @Test
    void cacheResultHoldsValues() {
        CacheResult result = new CacheResult("cached output", 0.98, Duration.ofMinutes(5));

        assertEquals("cached output", result.cachedOutput());
        assertEquals(0.98, result.similarity());
        assertEquals(Duration.ofMinutes(5), result.age());
    }

    @Test
    void cacheResultWithZeroSimilarity() {
        CacheResult result = new CacheResult("output", 0.0, Duration.ZERO);
        assertEquals(0.0, result.similarity());
    }

    @Test
    void cacheResultWithHighSimilarity() {
        CacheResult result = new CacheResult("output", 1.0, Duration.ofSeconds(1));
        assertEquals(1.0, result.similarity());
    }
}
