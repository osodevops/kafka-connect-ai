package sh.oso.connect.ai.adapter.streaming;

import org.apache.kafka.common.config.ConfigDef;
import org.cometd.bayeux.Channel;
import org.cometd.bayeux.Message;
import org.cometd.bayeux.client.ClientSessionChannel;
import org.cometd.client.BayeuxClient;
import org.cometd.client.http.jetty.JettyHttpClientTransport;
import org.eclipse.jetty.client.HttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.connect.ai.adapter.streaming.config.CometDConfig;
import sh.oso.connect.ai.api.adapter.SourceAdapter;
import sh.oso.connect.ai.api.config.AiConnectConfig;
import sh.oso.connect.ai.api.error.NonRetryableException;
import sh.oso.connect.ai.api.error.RetryableException;
import sh.oso.connect.ai.api.model.RawRecord;
import sh.oso.connect.ai.api.model.SourceOffset;
import sh.oso.connect.ai.api.util.BoundedRecordBuffer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class CometDSourceAdapter implements SourceAdapter {

    private static final Logger log = LoggerFactory.getLogger(CometDSourceAdapter.class);
    private static final Duration FETCH_TIMEOUT = Duration.ofSeconds(1);
    private static final long HANDSHAKE_TIMEOUT_MS = 30_000;

    private CometDConfig config;
    private BoundedRecordBuffer buffer;
    private BayeuxClient bayeuxClient;
    private HttpClient httpClient;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean connected = new AtomicBoolean(false);
    private final AtomicLong lastReplayId = new AtomicLong(-1);
    private volatile long currentBackoffMs;

    @Override
    public String type() {
        return "cometd";
    }

    @Override
    public ConfigDef configDef() {
        return new ConfigDef()
                .define(AiConnectConfig.COMETD_URL,
                        ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                        ConfigDef.Importance.HIGH, "CometD/Bayeux server URL")
                .define(AiConnectConfig.COMETD_CHANNEL,
                        ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                        ConfigDef.Importance.HIGH, "CometD channel to subscribe to")
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
        this.config = new CometDConfig(props);

        if (config.url().isEmpty()) {
            throw new NonRetryableException("cometd.url must be configured");
        }
        if (config.channel().isEmpty()) {
            throw new NonRetryableException("cometd.channel must be configured");
        }

        this.buffer = new BoundedRecordBuffer(config.bufferCapacity());
        this.currentBackoffMs = config.reconnectBackoffMs();
        this.running.set(true);

        connect();

        log.info("CometDSourceAdapter started: url={}, channel={}",
                config.url(), config.channel());
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
        // CometD offset tracking is managed via replayId; the Salesforce
        // streaming API uses replay IDs for event replay on reconnection.
        if (offset != null && offset.offset().containsKey("replayId")) {
            Object replayIdValue = offset.offset().get("replayId");
            if (replayIdValue instanceof Number) {
                lastReplayId.set(((Number) replayIdValue).longValue());
            }
        }
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
        log.info("CometDSourceAdapter stopping");

        if (bayeuxClient != null) {
            try {
                bayeuxClient.disconnect();
                bayeuxClient.waitFor(5000, BayeuxClient.State.DISCONNECTED);
            } catch (Exception e) {
                log.debug("Error disconnecting CometD client", e);
            }
            bayeuxClient = null;
        }

        if (httpClient != null) {
            try {
                httpClient.stop();
            } catch (Exception e) {
                log.debug("Error stopping HTTP client", e);
            }
            httpClient = null;
        }

        connected.set(false);
        buffer.clear();
        log.info("CometDSourceAdapter stopped");
    }

    private void connect() {
        try {
            httpClient = new HttpClient();
            httpClient.start();

            Map<String, Object> transportOptions = new HashMap<>();
            JettyHttpClientTransport transport = new JettyHttpClientTransport(
                    transportOptions, httpClient);

            bayeuxClient = new BayeuxClient(config.url(), transport);

            bayeuxClient.getChannel(Channel.META_CONNECT).addListener(
                    (ClientSessionChannel.MessageListener) (channel, message) -> {
                        if (message.isSuccessful()) {
                            connected.set(true);
                        } else {
                            connected.set(false);
                        }
                    });

            bayeuxClient.handshake();
            boolean handshaken = bayeuxClient.waitFor(HANDSHAKE_TIMEOUT_MS, BayeuxClient.State.CONNECTED);

            if (!handshaken) {
                throw new RetryableException(
                        "CometD handshake timed out connecting to " + config.url());
            }

            connected.set(true);
            currentBackoffMs = config.reconnectBackoffMs();

            bayeuxClient.getChannel(config.channel()).subscribe(this::onMessage);
            log.info("CometD connected and subscribed to {}", config.channel());

        } catch (RetryableException e) {
            connected.set(false);
            throw e;
        } catch (Exception e) {
            connected.set(false);
            throw new RetryableException(
                    "Failed to connect CometD client to " + config.url(), e);
        }
    }

    private void reconnect() {
        if (!running.get()) {
            return;
        }
        log.info("Attempting CometD reconnection with backoff {}ms", currentBackoffMs);
        try {
            Thread.sleep(currentBackoffMs);

            // Clean up existing client
            if (bayeuxClient != null) {
                try {
                    bayeuxClient.disconnect();
                } catch (Exception ignored) {
                    // best-effort cleanup
                }
            }
            if (httpClient != null) {
                try {
                    httpClient.stop();
                } catch (Exception ignored) {
                    // best-effort cleanup
                }
            }

            connect();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            currentBackoffMs = Math.min(currentBackoffMs * 2, config.maxReconnectBackoffMs());
            log.warn("CometD reconnection failed, next backoff {}ms", currentBackoffMs, e);
        }
    }

    private void onMessage(ClientSessionChannel channel, Message message) {
        Map<String, Object> data = message.getDataAsMap();
        if (data == null) {
            return;
        }

        // Message extends Map<String, Object>; serialize the whole message as a string
        String json = message.toString();
        long replayId = extractReplayId(data);
        lastReplayId.set(replayId);

        SourceOffset offset = new SourceOffset(
                Map.of("type", "cometd", "channel", config.channel()),
                Map.of("replayId", replayId)
        );

        RawRecord record = new RawRecord(
                String.valueOf(replayId).getBytes(StandardCharsets.UTF_8),
                json.getBytes(StandardCharsets.UTF_8),
                Map.of("source.type", "cometd",
                        "source.url", config.url(),
                        "source.channel", config.channel()),
                offset
        );

        if (!buffer.offer(record)) {
            log.warn("CometD buffer full, dropping message replayId={}", replayId);
        }
    }

    private long extractReplayId(Map<String, Object> data) {
        Object event = data.get("event");
        if (event instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> eventMap = (Map<String, Object>) event;
            Object replayId = eventMap.get("replayId");
            if (replayId instanceof Number) {
                return ((Number) replayId).longValue();
            }
        }
        // Fall back to incrementing from the last known replay ID
        return lastReplayId.incrementAndGet();
    }
}
