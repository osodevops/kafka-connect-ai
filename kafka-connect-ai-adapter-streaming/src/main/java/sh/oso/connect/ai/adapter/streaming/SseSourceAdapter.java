package sh.oso.connect.ai.adapter.streaming;

import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.MessageEvent;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.connect.ai.adapter.streaming.config.SseConfig;
import sh.oso.connect.ai.api.adapter.SourceAdapter;
import sh.oso.connect.ai.api.config.AiConnectConfig;
import sh.oso.connect.ai.api.error.NonRetryableException;
import sh.oso.connect.ai.api.model.RawRecord;
import sh.oso.connect.ai.api.model.SourceOffset;
import sh.oso.connect.ai.api.util.BoundedRecordBuffer;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class SseSourceAdapter implements SourceAdapter {

    private static final Logger log = LoggerFactory.getLogger(SseSourceAdapter.class);
    private static final Duration FETCH_TIMEOUT = Duration.ofSeconds(1);

    private SseConfig config;
    private BoundedRecordBuffer buffer;
    private BackgroundEventSource eventSource;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean connected = new AtomicBoolean(false);
    private final AtomicReference<String> lastEventId = new AtomicReference<>("");

    @Override
    public String type() {
        return "sse";
    }

    @Override
    public ConfigDef configDef() {
        return new ConfigDef()
                .define(AiConnectConfig.SSE_URL,
                        ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                        ConfigDef.Importance.HIGH, "SSE endpoint URL")
                .define(AiConnectConfig.SSE_RECONNECT_MS,
                        ConfigDef.Type.LONG, 3000L,
                        ConfigDef.Importance.MEDIUM, "SSE reconnection interval in milliseconds")
                .define(AiConnectConfig.STREAMING_BUFFER_CAPACITY,
                        ConfigDef.Type.INT, 10000,
                        ConfigDef.Importance.MEDIUM, "Internal message buffer capacity")
                .define(AiConnectConfig.STREAMING_RECONNECT_BACKOFF_MS,
                        ConfigDef.Type.LONG, 1000L,
                        ConfigDef.Importance.MEDIUM, "Initial reconnect backoff in milliseconds")
                .define(AiConnectConfig.STREAMING_MAX_RECONNECT_BACKOFF_MS,
                        ConfigDef.Type.LONG, 60000L,
                        ConfigDef.Importance.MEDIUM, "Maximum reconnect backoff in milliseconds");
    }

    @Override
    public void start(Map<String, String> props) {
        this.config = new SseConfig(props);

        if (config.url().isEmpty()) {
            throw new NonRetryableException("sse.url must be configured");
        }

        this.buffer = new BoundedRecordBuffer(config.bufferCapacity());
        this.running.set(true);

        EventSource.Builder esBuilder = new EventSource.Builder(URI.create(config.url()))
                .retryDelay(config.reconnectMs(), TimeUnit.MILLISECONDS);

        this.eventSource = new BackgroundEventSource.Builder(
                new SseEventHandler(), esBuilder).build();
        this.eventSource.start();

        log.info("SseSourceAdapter started: url={}, reconnectMs={}",
                config.url(), config.reconnectMs());
    }

    @Override
    public List<RawRecord> fetch(SourceOffset currentOffset, int maxRecords) throws InterruptedException {
        return buffer.drain(maxRecords, FETCH_TIMEOUT);
    }

    @Override
    public void commitOffset(SourceOffset offset) {
        // SSE offset tracking is managed via Last-Event-ID; the server handles
        // replay from the last acknowledged event ID on reconnection.
    }

    @Override
    public boolean isHealthy() {
        return running.get() && connected.get();
    }

    @Override
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        log.info("SseSourceAdapter stopping");

        if (eventSource != null) {
            try {
                eventSource.close();
            } catch (Exception e) {
                log.debug("Error closing SSE event source", e);
            }
            eventSource = null;
        }

        connected.set(false);
        buffer.clear();
        log.info("SseSourceAdapter stopped");
    }

    private SourceOffset buildOffset(String eventId) {
        return new SourceOffset(
                Map.of("type", "sse", "url", config.url()),
                Map.of("lastEventId", eventId != null ? eventId : "")
        );
    }

    private class SseEventHandler implements BackgroundEventHandler {

        @Override
        public void onOpen() {
            connected.set(true);
            log.info("SSE connection opened to {}", config.url());
        }

        @Override
        public void onClosed() {
            connected.set(false);
            log.info("SSE connection closed");
        }

        @Override
        public void onMessage(String event, MessageEvent messageEvent) {
            String data = messageEvent.getData();
            String eventId = messageEvent.getLastEventId();
            lastEventId.set(eventId != null ? eventId : "");

            RawRecord record = new RawRecord(
                    eventId != null ? eventId.getBytes(StandardCharsets.UTF_8) : null,
                    data.getBytes(StandardCharsets.UTF_8),
                    Map.of("source.type", "sse",
                            "source.url", config.url(),
                            "sse.event", event != null ? event : "message"),
                    buildOffset(eventId)
            );

            if (!buffer.offer(record)) {
                log.warn("SSE buffer full, dropping event id={}", eventId);
            }
        }

        @Override
        public void onComment(String comment) {
            log.trace("SSE comment: {}", comment);
        }

        @Override
        public void onError(Throwable t) {
            connected.set(false);
            log.error("SSE error", t);
        }
    }
}
