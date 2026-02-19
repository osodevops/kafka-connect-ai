package sh.oso.connect.ai.connect.resilience;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.connect.ai.api.adapter.SinkAdapter;
import sh.oso.connect.ai.api.config.AiConnectConfig;
import sh.oso.connect.ai.api.model.TransformedRecord;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class CircuitBreakingSinkAdapter implements SinkAdapter {

    private static final Logger log = LoggerFactory.getLogger(CircuitBreakingSinkAdapter.class);

    private final SinkAdapter delegate;
    private final CircuitBreaker circuitBreaker;

    public CircuitBreakingSinkAdapter(SinkAdapter delegate, Map<String, String> props) {
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

        this.circuitBreaker = CircuitBreaker.of("sink-" + delegate.type(), config);

        circuitBreaker.getEventPublisher()
                .onStateTransition(event ->
                        log.warn("Circuit breaker state transition for sink adapter '{}': {}",
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
    public void write(List<TransformedRecord> records) {
        try {
            CircuitBreaker.decorateCheckedRunnable(circuitBreaker,
                    () -> delegate.write(records)).run();
        } catch (Throwable t) {
            if (t instanceof RuntimeException re) {
                throw re;
            }
            throw new RuntimeException("Circuit breaker wrapped exception", t);
        }
    }

    @Override
    public void flush() {
        delegate.flush();
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
