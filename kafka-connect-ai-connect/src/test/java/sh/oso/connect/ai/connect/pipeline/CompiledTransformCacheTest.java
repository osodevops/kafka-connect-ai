package sh.oso.connect.ai.connect.pipeline;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class CompiledTransformCacheTest {

    @Test
    void putAndGet() {
        CompiledTransformCache cache = new CompiledTransformCache(3600);
        CompiledTransform transform = CompiledTransform.declarative(
                "{\"mappings\":{\"a\":\"$.b\"}}", "fp123", "claude-sonnet");

        cache.put("fp123", transform);

        Optional<CompiledTransform> result = cache.get("fp123");
        assertTrue(result.isPresent());
        assertEquals(CompiledTransform.Type.DECLARATIVE, result.get().type());
        assertEquals("{\"mappings\":{\"a\":\"$.b\"}}", result.get().code());
    }

    @Test
    void getMissReturnsEmpty() {
        CompiledTransformCache cache = new CompiledTransformCache(3600);
        assertTrue(cache.get("nonexistent").isEmpty());
    }

    @Test
    void invalidateRemovesEntry() {
        CompiledTransformCache cache = new CompiledTransformCache(3600);
        cache.put("fp123", CompiledTransform.declarative("{}", "fp123", "model"));

        cache.invalidate("fp123");

        assertTrue(cache.get("fp123").isEmpty());
    }

    @Test
    void clearRemovesAll() {
        CompiledTransformCache cache = new CompiledTransformCache(3600);
        cache.put("fp1", CompiledTransform.declarative("{}", "fp1", "model"));
        cache.put("fp2", CompiledTransform.javascript("return {}", "fp2", "model"));

        assertEquals(2, cache.size());
        cache.clear();
        assertEquals(0, cache.size());
    }

    @Test
    void expiredEntryIsEvicted() {
        // TTL of 0 seconds means everything expires immediately
        CompiledTransformCache cache = new CompiledTransformCache(0);

        // Create a transform with a createdAt in the past
        CompiledTransform old = new CompiledTransform(
                CompiledTransform.Type.DECLARATIVE, "{}", "fp1",
                Instant.now().minusSeconds(10), "model");

        cache.put("fp1", old);
        // With TTL=0, any age > 0 is expired
        assertTrue(cache.get("fp1").isEmpty());
    }

    @Test
    void noTtlMeansNoExpiry() {
        CompiledTransformCache cache = new CompiledTransformCache(-1);

        CompiledTransform old = new CompiledTransform(
                CompiledTransform.Type.DECLARATIVE, "{}", "fp1",
                Instant.now().minusSeconds(999999), "model");

        cache.put("fp1", old);
        assertTrue(cache.get("fp1").isPresent(), "Negative TTL = no expiry");
    }

    @Test
    void storesJavaScriptTransforms() {
        CompiledTransformCache cache = new CompiledTransformCache(3600);
        CompiledTransform transform = CompiledTransform.javascript(
                "return { name: input.userName }", "fp-js", "claude-sonnet");

        cache.put("fp-js", transform);

        Optional<CompiledTransform> result = cache.get("fp-js");
        assertTrue(result.isPresent());
        assertEquals(CompiledTransform.Type.JAVASCRIPT, result.get().type());
    }
}
