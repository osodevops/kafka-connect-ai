package sh.oso.connect.ai.adapter.http.ratelimit;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class RateLimiterTest {

    @Test
    void tryAcquireSucceedsWhenTokensAvailable() {
        RateLimiter limiter = new RateLimiter(10);
        assertTrue(limiter.tryAcquire());
    }

    @Test
    void tryAcquireFailsWhenTokensExhausted() {
        RateLimiter limiter = new RateLimiter(1);
        assertTrue(limiter.tryAcquire());
        assertFalse(limiter.tryAcquire());
    }

    @Test
    void acquireBlocksWhenNoTokens() {
        RateLimiter limiter = new RateLimiter(1);
        limiter.tryAcquire(); // exhaust the single token

        assertTimeoutPreemptively(Duration.ofSeconds(3), () -> limiter.acquire(),
                "acquire() should complete within the timeout after token refill");
    }
}
