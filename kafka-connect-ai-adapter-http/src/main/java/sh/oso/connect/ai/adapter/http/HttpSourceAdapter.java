package sh.oso.connect.ai.adapter.http;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.connect.ai.adapter.http.auth.AuthStrategy;
import sh.oso.connect.ai.adapter.http.auth.AuthStrategyFactory;
import sh.oso.connect.ai.adapter.http.config.HttpSourceConfig;
import sh.oso.connect.ai.adapter.http.pagination.PaginationStrategy;
import sh.oso.connect.ai.adapter.http.pagination.PaginationStrategyFactory;
import sh.oso.connect.ai.adapter.http.ratelimit.RateLimiter;
import sh.oso.connect.ai.api.adapter.SourceAdapter;
import sh.oso.connect.ai.api.error.RetryableException;
import sh.oso.connect.ai.api.model.RawRecord;
import sh.oso.connect.ai.api.model.SourceOffset;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class HttpSourceAdapter implements SourceAdapter {

    private static final Logger log = LoggerFactory.getLogger(HttpSourceAdapter.class);

    private final ObjectMapper objectMapper = new ObjectMapper();

    private HttpClient httpClient;
    private HttpSourceConfig config;
    private AuthStrategy authStrategy;
    private PaginationStrategy paginationStrategy;
    private RateLimiter rateLimiter;
    private long lastPollTimestamp;

    @Override
    public String type() {
        return "http";
    }

    @Override
    public ConfigDef configDef() {
        return new ConfigDef()
                .define(HttpSourceConfig.URL, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, "HTTP source URL")
                .define(HttpSourceConfig.METHOD, ConfigDef.Type.STRING, "GET", ConfigDef.Importance.MEDIUM, "HTTP method")
                .define(HttpSourceConfig.POLL_INTERVAL_MS, ConfigDef.Type.LONG, 60000L, ConfigDef.Importance.MEDIUM, "Poll interval in ms")
                .define(HttpSourceConfig.AUTH_TYPE, ConfigDef.Type.STRING, "none", ConfigDef.Importance.MEDIUM, "Auth type")
                .define(HttpSourceConfig.PAGINATION_TYPE, ConfigDef.Type.STRING, "none", ConfigDef.Importance.MEDIUM, "Pagination type")
                .define(HttpSourceConfig.RATE_LIMIT_RPS, ConfigDef.Type.DOUBLE, 10.0, ConfigDef.Importance.LOW, "Rate limit (requests/sec)");
    }

    @Override
    public void start(Map<String, String> props) {
        this.config = new HttpSourceConfig(props);
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(config.timeoutMs()))
                .build();

        this.authStrategy = AuthStrategyFactory.create(config.authType());
        this.authStrategy.configure(props);

        this.paginationStrategy = PaginationStrategyFactory.create(config.paginationType());
        this.paginationStrategy.configure(props);

        this.rateLimiter = new RateLimiter(config.rateLimitRps());
        this.lastPollTimestamp = 0;

        log.info("HttpSourceAdapter started: url={}, method={}, auth={}, pagination={}",
                config.url(), config.method(), config.authType(), config.paginationType());
    }

    @Override
    public List<RawRecord> fetch(SourceOffset currentOffset, int maxRecords) throws InterruptedException {
        long now = System.currentTimeMillis();
        long elapsed = now - lastPollTimestamp;
        if (elapsed < config.pollIntervalMs()) {
            Thread.sleep(config.pollIntervalMs() - elapsed);
        }
        lastPollTimestamp = System.currentTimeMillis();

        List<RawRecord> allRecords = new ArrayList<>();
        String url = paginationStrategy.firstPageUrl(config.url());

        do {
            rateLimiter.acquire();

            HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofMillis(config.timeoutMs()));

            applyMethod(requestBuilder, config.method());
            applyHeaders(requestBuilder, config.headers());
            authStrategy.apply(requestBuilder);

            try {
                HttpResponse<String> response = httpClient.send(
                        requestBuilder.build(), HttpResponse.BodyHandlers.ofString());

                if (response.statusCode() >= 400) {
                    throw new RetryableException("HTTP " + response.statusCode() + " from " + url);
                }

                String body = response.body();
                List<RawRecord> pageRecords = parseResponse(body, url);
                allRecords.addAll(pageRecords);

                Optional<String> nextUrl = paginationStrategy.nextPageUrl(url, response, body);
                if (nextUrl.isPresent()) {
                    url = nextUrl.get();
                } else {
                    break;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw e;
            } catch (RetryableException e) {
                throw e;
            } catch (Exception e) {
                throw new RetryableException("Failed to fetch from " + url, e);
            }
        } while (paginationStrategy.hasMore());

        log.debug("Fetched {} records from HTTP source", allRecords.size());
        return allRecords;
    }

    @Override
    public void commitOffset(SourceOffset offset) {
        // HTTP source is stateless by default; pagination handles offsets internally
    }

    @Override
    public boolean isHealthy() {
        return httpClient != null;
    }

    @Override
    public void stop() {
        log.info("HttpSourceAdapter stopped");
    }

    private void applyMethod(HttpRequest.Builder builder, String method) {
        switch (method.toUpperCase()) {
            case "GET" -> builder.GET();
            case "POST" -> builder.POST(HttpRequest.BodyPublishers.noBody());
            case "PUT" -> builder.PUT(HttpRequest.BodyPublishers.noBody());
            default -> builder.method(method, HttpRequest.BodyPublishers.noBody());
        }
    }

    private void applyHeaders(HttpRequest.Builder builder, String headersConfig) {
        if (headersConfig == null || headersConfig.isEmpty()) {
            return;
        }
        String[] pairs = headersConfig.split(",");
        for (String pair : pairs) {
            String[] kv = pair.split(":", 2);
            if (kv.length == 2) {
                builder.header(kv[0].trim(), kv[1].trim());
            }
        }
    }

    private List<RawRecord> parseResponse(String body, String url) {
        List<RawRecord> records = new ArrayList<>();
        try {
            JsonNode root = objectMapper.readTree(body);

            String contentPath = config.responseContentPath();
            JsonNode dataNode = root;
            if (contentPath != null && !contentPath.isEmpty()) {
                for (String segment : contentPath.split("\\.")) {
                    if (dataNode != null) {
                        dataNode = dataNode.get(segment);
                    }
                }
            }

            if (dataNode != null && dataNode.isArray()) {
                for (JsonNode item : dataNode) {
                    String itemJson = objectMapper.writeValueAsString(item);
                    records.add(new RawRecord(
                            null,
                            itemJson.getBytes(StandardCharsets.UTF_8),
                            Map.of("source.url", url),
                            SourceOffset.empty()
                    ));
                }
            } else {
                records.add(new RawRecord(
                        null,
                        body.getBytes(StandardCharsets.UTF_8),
                        Map.of("source.url", url),
                        SourceOffset.empty()
                ));
            }
        } catch (Exception e) {
            records.add(new RawRecord(
                    null,
                    body.getBytes(StandardCharsets.UTF_8),
                    Map.of("source.url", url),
                    SourceOffset.empty()
            ));
        }
        return records;
    }
}
