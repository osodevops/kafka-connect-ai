package sh.oso.connect.ai.adapter.streaming;

import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.connect.ai.adapter.streaming.config.WebSocketConfig;
import sh.oso.connect.ai.api.adapter.SourceAdapter;
import sh.oso.connect.ai.api.config.AiConnectConfig;
import sh.oso.connect.ai.api.error.NonRetryableException;
import sh.oso.connect.ai.api.error.RetryableException;
import sh.oso.connect.ai.api.model.RawRecord;
import sh.oso.connect.ai.api.model.SourceOffset;
import sh.oso.connect.ai.api.util.BoundedRecordBuffer;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class WebSocketSourceAdapter implements SourceAdapter {

    private static final Logger log = LoggerFactory.getLogger(WebSocketSourceAdapter.class);
    private static final Duration FETCH_TIMEOUT = Duration.ofSeconds(1);

    private WebSocketConfig config;
    private BoundedRecordBuffer buffer;
    private WebSocket webSocket;
    private HttpClient httpClient;

    private final AtomicBoolean connected = new AtomicBoolean(false);
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicLong sequenceNumber = new AtomicLong(0);
    private volatile long currentBackoffMs;

    @Override
    public String type() {
        return "websocket";
    }

    @Override
    public ConfigDef configDef() {
        return new ConfigDef()
                .define(AiConnectConfig.WEBSOCKET_URL,
                        ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                        ConfigDef.Importance.HIGH, "WebSocket server URL")
                .define(AiConnectConfig.WEBSOCKET_SUBPROTOCOL,
                        ConfigDef.Type.STRING, "",
                        ConfigDef.Importance.MEDIUM, "WebSocket subprotocol")
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
        this.config = new WebSocketConfig(props);

        if (config.url().isEmpty()) {
            throw new NonRetryableException("websocket.url must be configured");
        }

        this.buffer = new BoundedRecordBuffer(config.bufferCapacity());
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(30))
                .build();
        this.currentBackoffMs = config.reconnectBackoffMs();
        this.running.set(true);

        connect();

        log.info("WebSocketSourceAdapter started: url={}, subprotocol={}",
                config.url(), config.subprotocol());
    }

    @Override
    public List<RawRecord> fetch(SourceOffset currentOffset, int maxRecords) throws InterruptedException {
        if (!connected.get() && running.get()) {
            reconnect();
        }
        return buffer.drain(maxRecords, FETCH_TIMEOUT);
    }

    @Override
    public void commitOffset(SourceOffset offset) {
        // WebSocket is a push-based streaming protocol; offsets are tracked
        // internally via the sequence number but there is no server-side
        // acknowledgement mechanism.
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
        log.info("WebSocketSourceAdapter stopping");

        if (webSocket != null) {
            try {
                webSocket.sendClose(WebSocket.NORMAL_CLOSURE, "connector stopping");
            } catch (Exception e) {
                log.debug("Error sending WebSocket close frame", e);
            }
            webSocket = null;
        }

        if (httpClient != null) {
            httpClient = null;
        }

        connected.set(false);
        buffer.clear();
        log.info("WebSocketSourceAdapter stopped");
    }

    private void connect() {
        try {
            WebSocket.Builder wsBuilder = httpClient.newWebSocketBuilder()
                    .connectTimeout(Duration.ofSeconds(30));

            String subprotocol = config.subprotocol();
            if (subprotocol != null && !subprotocol.isEmpty()) {
                wsBuilder.subprotocols(subprotocol);
            }

            webSocket = wsBuilder
                    .buildAsync(URI.create(config.url()), new MessageListener())
                    .join();

            connected.set(true);
            currentBackoffMs = config.reconnectBackoffMs();
            log.info("WebSocket connected to {}", config.url());
        } catch (Exception e) {
            connected.set(false);
            throw new RetryableException("Failed to connect to WebSocket at " + config.url(), e);
        }
    }

    private void reconnect() {
        if (!running.get()) {
            return;
        }
        log.info("Attempting WebSocket reconnection with backoff {}ms", currentBackoffMs);
        try {
            Thread.sleep(currentBackoffMs);
            connect();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            currentBackoffMs = Math.min(currentBackoffMs * 2, config.maxReconnectBackoffMs());
            log.warn("WebSocket reconnection failed, next backoff {}ms", currentBackoffMs, e);
        }
    }

    private SourceOffset buildOffset(long seq) {
        return new SourceOffset(
                Map.of("type", "websocket", "url", config.url()),
                Map.of("sequence", seq)
        );
    }

    private class MessageListener implements WebSocket.Listener {

        private final StringBuilder messageBuilder = new StringBuilder();

        @Override
        public void onOpen(WebSocket webSocket) {
            log.debug("WebSocket onOpen");
            webSocket.request(1);
        }

        @Override
        public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
            messageBuilder.append(data);
            if (last) {
                String message = messageBuilder.toString();
                messageBuilder.setLength(0);

                long seq = sequenceNumber.incrementAndGet();
                RawRecord record = new RawRecord(
                        null,
                        message.getBytes(StandardCharsets.UTF_8),
                        Map.of("source.type", "websocket", "source.url", config.url()),
                        buildOffset(seq)
                );

                if (!buffer.offer(record)) {
                    log.warn("WebSocket buffer full, dropping message seq={}", seq);
                }
            }
            webSocket.request(1);
            return null;
        }

        @Override
        public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
            log.info("WebSocket closed: statusCode={}, reason={}", statusCode, reason);
            connected.set(false);
            return null;
        }

        @Override
        public void onError(WebSocket webSocket, Throwable error) {
            log.error("WebSocket error", error);
            connected.set(false);
        }
    }
}
