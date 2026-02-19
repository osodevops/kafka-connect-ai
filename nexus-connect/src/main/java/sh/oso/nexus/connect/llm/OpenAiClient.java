package sh.oso.nexus.connect.llm;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.nexus.api.error.RetryableException;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

public class OpenAiClient implements LlmClient {

    private static final Logger log = LoggerFactory.getLogger(OpenAiClient.class);
    private static final String DEFAULT_BASE_URL = "https://api.openai.com";
    private static final int MAX_RETRIES = 3;
    private static final Duration INITIAL_BACKOFF = Duration.ofSeconds(1);

    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final String apiKey;
    private final String baseUrl;

    public OpenAiClient(String apiKey, String baseUrl) {
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
                        .uri(URI.create(baseUrl + "/v1/chat/completions"))
                        .header("Content-Type", "application/json")
                        .header("Authorization", "Bearer " + apiKey)
                        .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                        .timeout(Duration.ofSeconds(120))
                        .build();

                HttpResponse<String> response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());

                if (response.statusCode() == 429 || response.statusCode() >= 500) {
                    attempt++;
                    if (attempt >= MAX_RETRIES) {
                        throw new RetryableException(
                                "OpenAI API returned " + response.statusCode() + " after " + MAX_RETRIES + " retries");
                    }
                    Duration backoff = INITIAL_BACKOFF.multipliedBy((long) Math.pow(2, attempt - 1));
                    log.warn("OpenAI API returned {}. Retrying in {}ms (attempt {}/{})",
                            response.statusCode(), backoff.toMillis(), attempt, MAX_RETRIES);
                    Thread.sleep(backoff.toMillis());
                    continue;
                }

                if (response.statusCode() != 200) {
                    throw new RuntimeException(
                            "OpenAI API error: " + response.statusCode() + " - " + response.body());
                }

                return parseResponse(response.body());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RetryableException("Interrupted during OpenAI API call", e);
            } catch (IOException e) {
                attempt++;
                if (attempt >= MAX_RETRIES) {
                    throw new RetryableException("OpenAI API call failed after " + MAX_RETRIES + " retries", e);
                }
                log.warn("OpenAI API call failed. Retrying (attempt {}/{})", attempt, MAX_RETRIES, e);
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

            ArrayNode messages = root.putArray("messages");

            if (!request.systemPrompt().isEmpty()) {
                ObjectNode systemMessage = messages.addObject();
                systemMessage.put("role", "system");
                if (request.enablePromptCaching()) {
                    // OpenAI prompt caching: use structured content with cache_control
                    ArrayNode contentArray = systemMessage.putArray("content");
                    ObjectNode textBlock = contentArray.addObject();
                    textBlock.put("type", "text");
                    textBlock.put("text", request.systemPrompt());
                    ObjectNode cacheControl = textBlock.putObject("cache_control");
                    cacheControl.put("type", "ephemeral");
                } else {
                    systemMessage.put("content", request.systemPrompt());
                }
            }

            ObjectNode userMessage = messages.addObject();
            userMessage.put("role", "user");
            userMessage.put("content", request.userPrompt());

            if (request.jsonSchema().isPresent()) {
                ObjectNode responseFormat = root.putObject("response_format");
                responseFormat.put("type", "json_schema");
                ObjectNode jsonSchemaNode = responseFormat.putObject("json_schema");
                jsonSchemaNode.put("name", "transform");
                jsonSchemaNode.put("strict", true);
                jsonSchemaNode.set("schema", objectMapper.readTree(request.jsonSchema().get()));
            }

            return objectMapper.writeValueAsString(root);
        } catch (Exception e) {
            throw new RuntimeException("Failed to build OpenAI request body", e);
        }
    }

    private LlmResponse parseResponse(String responseBody) {
        try {
            JsonNode root = objectMapper.readTree(responseBody);

            String content = "";
            JsonNode choices = root.get("choices");
            if (choices != null && choices.isArray() && !choices.isEmpty()) {
                content = choices.get(0).path("message").path("content").asText("");
            }

            JsonNode usage = root.path("usage");
            int inputTokens = usage.path("prompt_tokens").asInt(0);
            int outputTokens = usage.path("completion_tokens").asInt(0);
            int cachedTokens = usage.path("prompt_tokens_details").path("cached_tokens").asInt(0);
            String model = root.path("model").asText("");
            String stopReason = "";
            if (choices != null && choices.isArray() && !choices.isEmpty()) {
                stopReason = choices.get(0).path("finish_reason").asText("");
            }

            return new LlmResponse(content, inputTokens, outputTokens, model, stopReason,
                    0, cachedTokens);
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse OpenAI response: " + responseBody, e);
        }
    }
}
