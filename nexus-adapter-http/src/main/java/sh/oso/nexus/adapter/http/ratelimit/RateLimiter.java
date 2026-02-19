package sh.oso.nexus.adapter.http.ratelimit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.ReentrantLock;

public class RateLimiter {

    private static final Logger log = LoggerFactory.getLogger(RateLimiter.class);

    private final double maxTokens;
    private final double refillRate; // tokens per second
    private double availableTokens;
    private long lastRefillTimestamp;
    private final ReentrantLock lock = new ReentrantLock();

    public RateLimiter(double requestsPerSecond) {
        this.maxTokens = requestsPerSecond;
        this.refillRate = requestsPerSecond;
        this.availableTokens = requestsPerSecond;
        this.lastRefillTimestamp = System.nanoTime();
    }

    public void acquire() throws InterruptedException {
        while (true) {
            lock.lock();
            try {
                refill();
                if (availableTokens >= 1.0) {
                    availableTokens -= 1.0;
                    return;
                }
                double waitSeconds = (1.0 - availableTokens) / refillRate;
                long waitMillis = (long) (waitSeconds * 1000) + 1;
                lock.unlock();
                Thread.sleep(waitMillis);
                lock.lock();
            } finally {
                if (lock.isHeldByCurrentThread()) {
                    lock.unlock();
                }
            }
        }
    }

    public boolean tryAcquire() {
        lock.lock();
        try {
            refill();
            if (availableTokens >= 1.0) {
                availableTokens -= 1.0;
                return true;
            }
            return false;
        } finally {
            lock.unlock();
        }
    }

    private void refill() {
        long now = System.nanoTime();
        double elapsed = (now - lastRefillTimestamp) / 1_000_000_000.0;
        availableTokens = Math.min(maxTokens, availableTokens + elapsed * refillRate);
        lastRefillTimestamp = now;
    }
}
