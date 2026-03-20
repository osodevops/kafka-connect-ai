package sh.oso.connect.ai.connect.llm;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.connect.ai.api.error.RetryableException;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

public class AnthropicClient implements LlmClient {

    private static final Logger log = LoggerFactory.getLogger(AnthropicClient.class);
    private static final String DEFAULT_BASE_URL = "https://api.anthropic.com";
    private static final String API_VERSION = "2023-06-01";
    private static final int MAX_RETRIES = 5;
    private static final Duration INITIAL_BACKOFF = Duration.ofSeconds(1);
    private static final double JITTER_FACTOR = 0.25;

    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final String apiKey;
    private final String baseUrl;

    public AnthropicClient(String apiKey, String baseUrl) {
        this.apiKey = apiKey;
        this.baseUrl = baseUrl.isEmpty() ? DEFAULT_BASE_URL : baseUrl;
        this.objectMapper = new ObjectMapper();
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(30))
                .build();
    }

    @Override
    public LlmResponse call(LlmRequest request) {
        String requestBody = buildRequestBody(request);
        int attempt = 0;

        while (true) {
            try {
                HttpRequest httpRequest = HttpRequest.newBuilder()
                        .uri(URI.create(baseUrl + "/v1/messages"))
                        .header("Content-Type", "application/json")
                        .header("x-api-key", apiKey)
                        .header("anthropic-version", API_VERSION)
                        .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                        .timeout(Duration.ofSeconds(120))
                        .build();

                HttpResponse<String> response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());

                if (response.statusCode() == 429 || response.statusCode() >= 500) {
                    attempt++;
                    if (attempt >= MAX_RETRIES) {
                        throw new RetryableException(
                                "Anthropic API returned " + response.statusCode() + " after " + MAX_RETRIES + " retries");
                    }
                    // Respect Retry-After header if present, otherwise exponential backoff with jitter
                    long backoffMs;
                    var retryAfter = response.headers().firstValue("retry-after");
                    if (retryAfter.isPresent()) {
                        try {
                            backoffMs = (long) (Double.parseDouble(retryAfter.get()) * 1000);
                        } catch (NumberFormatException e) {
                            backoffMs = INITIAL_BACKOFF.multipliedBy((long) Math.pow(2, attempt - 1)).toMillis();
                        }
                    } else {
                        backoffMs = INITIAL_BACKOFF.multipliedBy((long) Math.pow(2, attempt - 1)).toMillis();
                    }
                    // Add random jitter to prevent thundering herd
                    long jitter = (long) (backoffMs * JITTER_FACTOR * ThreadLocalRandom.current().nextDouble());
                    backoffMs += jitter;
                    log.warn("Anthropic API returned {}. Retrying in {}ms (attempt {}/{})",
                            response.statusCode(), backoffMs, attempt, MAX_RETRIES);
                    Thread.sleep(backoffMs);
                    continue;
                }

                if (response.statusCode() != 200) {
                    throw new RuntimeException(
                            "Anthropic API error: " + response.statusCode() + " - " + response.body());
                }

                return parseResponse(response.body());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RetryableException("Interrupted during Anthropic API call", e);
            } catch (IOException e) {
                attempt++;
                if (attempt >= MAX_RETRIES) {
                    throw new RetryableException("Anthropic API call failed after " + MAX_RETRIES + " retries", e);
                }
                log.warn("Anthropic API call failed. Retrying (attempt {}/{})", attempt, MAX_RETRIES, e);
            }
        }
    }

    private String buildRequestBody(LlmRequest request) {
        try {
            ObjectNode root = objectMapper.createObjectNode();
            root.put("model", request.model());
            root.put("max_tokens", request.maxTokens());
            root.put("temperature", request.temperature());
            request.seed().ifPresent(s -> root.put("seed", s));

            String systemPrompt = request.systemPrompt();
            if (request.jsonSchema().isPresent()) {
                String schemaInstruction = "\n\nYou MUST respond with valid JSON that conforms to the following JSON Schema:\n"
                        + request.jsonSchema().get()
                        + "\n\nRespond with ONLY valid JSON. No additional text or explanation.";
                systemPrompt = systemPrompt.isEmpty() ? schemaInstruction : systemPrompt + schemaInstruction;
            }

            if (!systemPrompt.isEmpty()) {
                if (request.enablePromptCaching()) {
                    ArrayNode systemArray = root.putArray("system");
                    ObjectNode systemBlock = systemArray.addObject();
                    systemBlock.put("type", "text");
                    systemBlock.put("text", systemPrompt);
                    ObjectNode cacheControl = systemBlock.putObject("cache_control");
                    cacheControl.put("type", "ephemeral");
                } else {
                    root.put("system", systemPrompt);
                }
            }

            ArrayNode messages = root.putArray("messages");
            ObjectNode userMessage = messages.addObject();
            userMessage.put("role", "user");
            userMessage.put("content", request.userPrompt());

            return objectMapper.writeValueAsString(root);
        } catch (Exception e) {
            throw new RuntimeException("Failed to build Anthropic request body", e);
        }
    }

    private LlmResponse parseResponse(String responseBody) {
        try {
            JsonNode root = objectMapper.readTree(responseBody);

            StringBuilder content = new StringBuilder();
            JsonNode contentArray = root.get("content");
            if (contentArray != null && contentArray.isArray()) {
                for (JsonNode block : contentArray) {
                    if ("text".equals(block.path("type").asText())) {
                        content.append(block.path("text").asText());
                    }
                }
            }

            JsonNode usage = root.path("usage");
            int inputTokens = usage.path("input_tokens").asInt(0);
            int outputTokens = usage.path("output_tokens").asInt(0);
            int cacheCreationTokens = usage.path("cache_creation_input_tokens").asInt(0);
            int cacheReadTokens = usage.path("cache_read_input_tokens").asInt(0);
            String model = root.path("model").asText("");
            String stopReason = root.path("stop_reason").asText("");

            return new LlmResponse(content.toString(), inputTokens, outputTokens, model, stopReason,
                    cacheCreationTokens, cacheReadTokens);
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse Anthropic response: " + responseBody, e);
        }
    }
}
