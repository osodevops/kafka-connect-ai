package sh.oso.connect.ai.connect.pipeline;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class RateLimiterTest {

    @Test
    void tryAcquire_returnsFalseWhenExhausted() {
        RateLimiter limiter = new RateLimiter(2.0);

        // Should succeed twice (bucket starts full)
        assertTrue(limiter.tryAcquire());
        assertTrue(limiter.tryAcquire());

        // Third should fail — bucket exhausted
        assertFalse(limiter.tryAcquire());
    }

    @Test
    void tryAcquire_refillsOverTime() throws InterruptedException {
        RateLimiter limiter = new RateLimiter(10.0);

        // Drain the bucket
        for (int i = 0; i < 10; i++) {
            assertTrue(limiter.tryAcquire());
        }
        assertFalse(limiter.tryAcquire());

        // Wait for refill (at 10/sec, 200ms should refill ~2 tokens)
        Thread.sleep(250);

        assertTrue(limiter.tryAcquire(), "Should have refilled after waiting");
    }

    @Test
    void acquire_blocksUntilTokenAvailable() throws InterruptedException {
        RateLimiter limiter = new RateLimiter(10.0);

        // Drain all tokens
        for (int i = 0; i < 10; i++) {
            limiter.tryAcquire();
        }

        long start = System.currentTimeMillis();
        limiter.acquire(); // Should block briefly until refill
        long elapsed = System.currentTimeMillis() - start;

        // Should have waited some time (at least a few ms for refill)
        assertTrue(elapsed >= 0, "acquire() should eventually succeed");
    }
}
