package sh.oso.nexus.adapter.http;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.nexus.adapter.http.auth.AuthStrategy;
import sh.oso.nexus.adapter.http.auth.AuthStrategyFactory;
import sh.oso.nexus.adapter.http.config.HttpSinkConfig;
import sh.oso.nexus.adapter.http.ratelimit.RateLimiter;
import sh.oso.nexus.api.adapter.SinkAdapter;
import sh.oso.nexus.api.error.RetryableException;
import sh.oso.nexus.api.model.TransformedRecord;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HttpSinkAdapter implements SinkAdapter {

    private static final Logger log = LoggerFactory.getLogger(HttpSinkAdapter.class);
    private static final Pattern URL_TEMPLATE_PATTERN = Pattern.compile("\\{(\\w+)}");
    private static final Duration INITIAL_BACKOFF = Duration.ofSeconds(1);

    private final ObjectMapper objectMapper = new ObjectMapper();

    private HttpClient httpClient;
    private HttpSinkConfig config;
    private AuthStrategy authStrategy;
    private RateLimiter rateLimiter;

    @Override
    public String type() {
        return "http";
    }

    @Override
    public ConfigDef configDef() {
        return new ConfigDef()
                .define(HttpSinkConfig.URL, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, "HTTP sink URL (supports {field} templates)")
                .define(HttpSinkConfig.METHOD, ConfigDef.Type.STRING, "POST", ConfigDef.Importance.MEDIUM, "HTTP method")
                .define(HttpSinkConfig.AUTH_TYPE, ConfigDef.Type.STRING, "none", ConfigDef.Importance.MEDIUM, "Auth type")
                .define(HttpSinkConfig.RATE_LIMIT_RPS, ConfigDef.Type.DOUBLE, 10.0, ConfigDef.Importance.LOW, "Rate limit (requests/sec)")
                .define(HttpSinkConfig.RETRY_MAX, ConfigDef.Type.INT, 3, ConfigDef.Importance.LOW, "Maximum retry attempts");
    }

    @Override
    public void start(Map<String, String> props) {
        this.config = new HttpSinkConfig(props);
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(config.timeoutMs()))
                .build();

        this.authStrategy = AuthStrategyFactory.create(config.authType());
        this.authStrategy.configure(props);

        this.rateLimiter = new RateLimiter(config.rateLimitRps());

        log.info("HttpSinkAdapter started: url={}, method={}, auth={}, retryMax={}",
                config.url(), config.method(), config.authType(), config.retryMax());
    }

    @Override
    public void write(List<TransformedRecord> records) {
        for (TransformedRecord record : records) {
            try {
                rateLimiter.acquire();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RetryableException("Interrupted while waiting for rate limiter", e);
            }

            String body = record.value() != null
                    ? new String(record.value(), StandardCharsets.UTF_8)
                    : "";

            String resolvedUrl = resolveUrlTemplate(config.url(), body);

            sendWithRetry(resolvedUrl, body);
        }
    }

    private void sendWithRetry(String url, String body) {
        int maxRetries = config.retryMax();

        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .header("Content-Type", "application/json")
                    .timeout(Duration.ofMillis(config.timeoutMs()));

            applyHeaders(requestBuilder, config.headers());
            authStrategy.apply(requestBuilder);
            applyMethod(requestBuilder, config.method(), body);

            try {
                HttpResponse<String> response = httpClient.send(
                        requestBuilder.build(), HttpResponse.BodyHandlers.ofString());

                if (response.statusCode() < 400) {
                    log.debug("Successfully wrote record to {}: HTTP {}", url, response.statusCode());
                    return;
                }

                if (response.statusCode() == 429 || response.statusCode() >= 500) {
                    if (attempt >= maxRetries) {
                        throw new RetryableException(
                                "HTTP " + response.statusCode() + " writing to " + url + " after " + (maxRetries + 1) + " attempts");
                    }

                    long backoffMs = calculateBackoff(attempt, response);
                    log.warn("HTTP {} from {}. Retrying in {}ms (attempt {}/{})",
                            response.statusCode(), url, backoffMs, attempt + 1, maxRetries + 1);
                    Thread.sleep(backoffMs);
                    continue;
                }

                throw new RetryableException(
                        "HTTP " + response.statusCode() + " writing to " + url);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RetryableException("Interrupted during HTTP write", e);
            } catch (RetryableException e) {
                throw e;
            } catch (Exception e) {
                if (attempt >= maxRetries) {
                    throw new RetryableException("Failed to write to " + url + " after " + (maxRetries + 1) + " attempts", e);
                }
                long backoffMs = INITIAL_BACKOFF.multipliedBy((long) Math.pow(2, attempt)).toMillis();
                log.warn("HTTP request to {} failed. Retrying in {}ms (attempt {}/{})",
                        url, backoffMs, attempt + 1, maxRetries + 1, e);
                try {
                    Thread.sleep(backoffMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RetryableException("Interrupted during backoff", ie);
                }
            }
        }
    }

    private long calculateBackoff(int attempt, HttpResponse<String> response) {
        // Respect Retry-After header if present
        String retryAfter = response.headers().firstValue("Retry-After").orElse(null);
        if (retryAfter != null) {
            try {
                return Long.parseLong(retryAfter) * 1000;
            } catch (NumberFormatException e) {
                // Not a number, fall through to exponential backoff
            }
        }
        return INITIAL_BACKOFF.multipliedBy((long) Math.pow(2, attempt)).toMillis();
    }

    String resolveUrlTemplate(String urlTemplate, String body) {
        if (!urlTemplate.contains("{")) {
            return urlTemplate;
        }

        try {
            JsonNode node = objectMapper.readTree(body);
            Matcher matcher = URL_TEMPLATE_PATTERN.matcher(urlTemplate);
            StringBuilder result = new StringBuilder();

            while (matcher.find()) {
                String fieldName = matcher.group(1);
                JsonNode fieldValue = node.get(fieldName);
                String replacement = fieldValue != null ? fieldValue.asText() : fieldName;
                matcher.appendReplacement(result, replacement);
            }
            matcher.appendTail(result);

            return result.toString();
        } catch (Exception e) {
            log.warn("Failed to resolve URL template, using as-is: {}", urlTemplate);
            return urlTemplate;
        }
    }

    @Override
    public void flush() {
        // HTTP sink is synchronous; no buffered writes to flush
    }

    @Override
    public boolean isHealthy() {
        return httpClient != null;
    }

    @Override
    public void stop() {
        log.info("HttpSinkAdapter stopped");
    }

    private void applyMethod(HttpRequest.Builder builder, String method, String body) {
        HttpRequest.BodyPublisher publisher = HttpRequest.BodyPublishers.ofString(body);
        switch (method.toUpperCase()) {
            case "POST" -> builder.POST(publisher);
            case "PUT" -> builder.PUT(publisher);
            case "PATCH" -> builder.method("PATCH", publisher);
            default -> builder.method(method, publisher);
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
}
