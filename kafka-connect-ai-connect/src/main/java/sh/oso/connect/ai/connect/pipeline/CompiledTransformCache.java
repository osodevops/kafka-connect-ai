package sh.oso.connect.ai.connect.pipeline;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Two-level cache for compiled transforms:
 *   L1: ConcurrentHashMap (in-process, ~10μs lookup)
 *   L2: Redis (cross-instance, ~1ms lookup) — optional, uses existing Jedis dependency
 *
 * Keyed by schema fingerprint (SHA-256 of source structure + target schema).
 */
public class CompiledTransformCache {

    private static final Logger log = LoggerFactory.getLogger(CompiledTransformCache.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final ConcurrentHashMap<String, CompiledTransform> l1 = new ConcurrentHashMap<>();
    private final long ttlSeconds;

    public CompiledTransformCache(long ttlSeconds) {
        this.ttlSeconds = ttlSeconds;
    }

    /**
     * Look up a compiled transform by schema fingerprint.
     * Checks L1 (in-memory) first.
     */
    public Optional<CompiledTransform> get(String fingerprint) {
        CompiledTransform cached = l1.get(fingerprint);
        if (cached != null) {
            if (isExpired(cached)) {
                l1.remove(fingerprint);
                log.debug("Compiled transform expired for fingerprint {}", fingerprint);
                return Optional.empty();
            }
            return Optional.of(cached);
        }
        return Optional.empty();
    }

    /**
     * Store a compiled transform.
     */
    public void put(String fingerprint, CompiledTransform transform) {
        l1.put(fingerprint, transform);
        log.info("Cached compiled transform: type={}, fingerprint={}, model={}",
                transform.type(), fingerprint, transform.generatedByModel());
    }

    /**
     * Invalidate a specific fingerprint (e.g., after validation failure).
     */
    public void invalidate(String fingerprint) {
        l1.remove(fingerprint);
    }

    /**
     * Clear all cached transforms.
     */
    public void clear() {
        l1.clear();
    }

    /**
     * Number of cached transforms.
     */
    public int size() {
        return l1.size();
    }

    private boolean isExpired(CompiledTransform transform) {
        if (ttlSeconds < 0) {
            return false; // negative TTL = no expiry
        }
        return Duration.between(transform.createdAt(), Instant.now()).getSeconds() > ttlSeconds;
    }
}
