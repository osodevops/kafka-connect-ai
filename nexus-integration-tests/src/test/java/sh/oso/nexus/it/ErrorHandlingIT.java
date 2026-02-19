package sh.oso.nexus.it;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class ErrorHandlingIT {

    static final Network network = Network.newNetwork();
    static final ObjectMapper objectMapper = new ObjectMapper();
    static final HttpClient httpClient = HttpClient.newHttpClient();

    @Container
    static final GenericContainer<?> wiremock = new GenericContainer<>(
            DockerImageName.parse("wiremock/wiremock:3.9.2"))
            .withNetwork(network)
            .withNetworkAliases("wiremock")
            .withExposedPorts(8080)
            .withCommand("--verbose")
            .waitingFor(Wait.forHttp("/__admin/mappings").forPort(8080));

    @BeforeAll
    static void setupWireMock() throws Exception {
        String wiremockUrl = "http://localhost:" + wiremock.getMappedPort(8080);

        // Mock LLM API that returns malformed response
        String malformedLlmMapping = """
                {
                  "request": {
                    "method": "POST",
                    "url": "/v1/messages/malformed"
                  },
                  "response": {
                    "status": 200,
                    "headers": {
                      "Content-Type": "application/json"
                    },
                    "jsonBody": {
                      "id": "msg_bad",
                      "type": "message",
                      "role": "assistant",
                      "content": [
                        {
                          "type": "text",
                          "text": "This is not valid JSON array"
                        }
                      ],
                      "model": "claude-sonnet-4-20250514",
                      "stop_reason": "end_turn",
                      "usage": {"input_tokens": 50, "output_tokens": 10}
                    }
                  }
                }
                """;

        httpClient.send(
                HttpRequest.newBuilder()
                        .uri(URI.create(wiremockUrl + "/__admin/mappings"))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(malformedLlmMapping))
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        // Mock source API returning 500
        String errorSourceMapping = """
                {
                  "request": {
                    "method": "GET",
                    "url": "/api/error"
                  },
                  "response": {
                    "status": 500,
                    "headers": {
                      "Content-Type": "application/json"
                    },
                    "jsonBody": {"error": "Internal Server Error"}
                  }
                }
                """;

        httpClient.send(
                HttpRequest.newBuilder()
                        .uri(URI.create(wiremockUrl + "/__admin/mappings"))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(errorSourceMapping))
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        // Mock source API returning 429 (rate limited)
        String rateLimitMapping = """
                {
                  "request": {
                    "method": "GET",
                    "url": "/api/ratelimited"
                  },
                  "response": {
                    "status": 429,
                    "headers": {
                      "Content-Type": "application/json",
                      "Retry-After": "1"
                    },
                    "jsonBody": {"error": "Too Many Requests"}
                  }
                }
                """;

        httpClient.send(
                HttpRequest.newBuilder()
                        .uri(URI.create(wiremockUrl + "/__admin/mappings"))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(rateLimitMapping))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void sourceApiReturns500() throws Exception {
        String wiremockUrl = "http://localhost:" + wiremock.getMappedPort(8080);

        HttpResponse<String> response = httpClient.send(
                HttpRequest.newBuilder()
                        .uri(URI.create(wiremockUrl + "/api/error"))
                        .GET()
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        assertEquals(500, response.statusCode());
        JsonNode body = objectMapper.readTree(response.body());
        assertEquals("Internal Server Error", body.get("error").asText());
    }

    @Test
    void sourceApiReturns429RateLimit() throws Exception {
        String wiremockUrl = "http://localhost:" + wiremock.getMappedPort(8080);

        HttpResponse<String> response = httpClient.send(
                HttpRequest.newBuilder()
                        .uri(URI.create(wiremockUrl + "/api/ratelimited"))
                        .GET()
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        assertEquals(429, response.statusCode());
        assertTrue(response.headers().firstValue("Retry-After").isPresent());
    }

    @Test
    void malformedLlmResponseIsDetectable() throws Exception {
        String wiremockUrl = "http://localhost:" + wiremock.getMappedPort(8080);

        HttpResponse<String> response = httpClient.send(
                HttpRequest.newBuilder()
                        .uri(URI.create(wiremockUrl + "/v1/messages/malformed"))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString("{}"))
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());
        JsonNode body = objectMapper.readTree(response.body());
        String content = body.get("content").get(0).get("text").asText();
        // Content is not valid JSON array - pipeline should detect and retry
        assertFalse(content.startsWith("["));
    }
}
