package sh.oso.connect.ai.connect.resilience;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.connect.ai.api.adapter.SourceAdapter;
import sh.oso.connect.ai.api.config.AiConnectConfig;
import sh.oso.connect.ai.api.model.RawRecord;
import sh.oso.connect.ai.api.model.SourceOffset;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class CircuitBreakingSourceAdapter implements SourceAdapter {

    private static final Logger log = LoggerFactory.getLogger(CircuitBreakingSourceAdapter.class);

    private final SourceAdapter delegate;
    private final CircuitBreaker circuitBreaker;

    public CircuitBreakingSourceAdapter(SourceAdapter delegate, Map<String, String> props) {
        this.delegate = delegate;

        float failureRate = Float.parseFloat(
                props.getOrDefault(AiConnectConfig.CIRCUIT_BREAKER_FAILURE_RATE_THRESHOLD, "50"));
        long waitDurationMs = Long.parseLong(
                props.getOrDefault(AiConnectConfig.CIRCUIT_BREAKER_WAIT_DURATION_MS, "60000"));
        int slidingWindowSize = Integer.parseInt(
                props.getOrDefault(AiConnectConfig.CIRCUIT_BREAKER_SLIDING_WINDOW_SIZE, "10"));

        CircuitBreakerConfig config = CircuitBreakerConfig.custom()
                .failureRateThreshold(failureRate)
                .waitDurationInOpenState(Duration.ofMillis(waitDurationMs))
                .slidingWindowSize(slidingWindowSize)
                .build();

        this.circuitBreaker = CircuitBreaker.of("source-" + delegate.type(), config);

        circuitBreaker.getEventPublisher()
                .onStateTransition(event ->
                        log.warn("Circuit breaker state transition for source adapter '{}': {}",
                                delegate.type(), event.getStateTransition()));
    }

    @Override
    public String type() {
        return delegate.type();
    }

    @Override
    public ConfigDef configDef() {
        return delegate.configDef();
    }

    @Override
    public void start(Map<String, String> props) {
        delegate.start(props);
    }

    @Override
    public List<RawRecord> fetch(SourceOffset currentOffset, int maxRecords) throws InterruptedException {
        try {
            return CircuitBreaker.decorateCheckedSupplier(circuitBreaker,
                    () -> delegate.fetch(currentOffset, maxRecords)).get();
        } catch (InterruptedException e) {
            throw e;
        } catch (Throwable t) {
            if (t instanceof RuntimeException re) {
                throw re;
            }
            throw new RuntimeException("Circuit breaker wrapped exception", t);
        }
    }

    @Override
    public void commitOffset(SourceOffset offset) {
        delegate.commitOffset(offset);
    }

    @Override
    public boolean isHealthy() {
        return delegate.isHealthy()
                && circuitBreaker.getState() != CircuitBreaker.State.OPEN;
    }

    @Override
    public void stop() {
        delegate.stop();
    }
}
