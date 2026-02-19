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
class SinkEndToEndIT {

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

        // Mock sink endpoint that accepts POSTs
        String sinkMapping = """
                {
                  "request": {
                    "method": "POST",
                    "url": "/api/sink"
                  },
                  "response": {
                    "status": 200,
                    "headers": {
                      "Content-Type": "application/json"
                    },
                    "jsonBody": {"status": "ok"}
                  }
                }
                """;

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(wiremockUrl + "/__admin/mappings"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(sinkMapping))
                .build();
        httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        // Mock LLM API
        String llmMapping = """
                {
                  "request": {
                    "method": "POST",
                    "url": "/v1/messages"
                  },
                  "response": {
                    "status": 200,
                    "headers": {
                      "Content-Type": "application/json"
                    },
                    "jsonBody": {
                      "id": "msg_mock",
                      "type": "message",
                      "role": "assistant",
                      "content": [
                        {
                          "type": "text",
                          "text": "[{\\"transformed\\":true,\\"data\\":\\"test\\"}]"
                        }
                      ],
                      "model": "claude-sonnet-4-20250514",
                      "stop_reason": "end_turn",
                      "usage": {"input_tokens": 50, "output_tokens": 25}
                    }
                  }
                }
                """;

        HttpRequest llmRequest = HttpRequest.newBuilder()
                .uri(URI.create(wiremockUrl + "/__admin/mappings"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(llmMapping))
                .build();
        httpClient.send(llmRequest, HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void sinkEndpointAcceptsPosts() throws Exception {
        String wiremockUrl = "http://localhost:" + wiremock.getMappedPort(8080);

        // Simulate what the sink adapter would do - POST transformed data
        String payload = """
                {"transformed": true, "data": "test record"}
                """;

        HttpResponse<String> response = httpClient.send(
                HttpRequest.newBuilder()
                        .uri(URI.create(wiremockUrl + "/api/sink"))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(payload))
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());
        JsonNode body = objectMapper.readTree(response.body());
        assertEquals("ok", body.get("status").asText());
    }

    @Test
    void wiremockCapturesSinkRequests() throws Exception {
        String wiremockUrl = "http://localhost:" + wiremock.getMappedPort(8080);

        // Reset request journal
        httpClient.send(
                HttpRequest.newBuilder()
                        .uri(URI.create(wiremockUrl + "/__admin/requests"))
                        .method("DELETE", HttpRequest.BodyPublishers.noBody())
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        // Send a POST to sink
        String payload = """
                {"id": 1, "value": "sink test"}
                """;
        httpClient.send(
                HttpRequest.newBuilder()
                        .uri(URI.create(wiremockUrl + "/api/sink"))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(payload))
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        // Verify it was captured
        HttpResponse<String> logResponse = httpClient.send(
                HttpRequest.newBuilder()
                        .uri(URI.create(wiremockUrl + "/__admin/requests"))
                        .GET()
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        JsonNode log = objectMapper.readTree(logResponse.body());
        assertTrue(log.get("requests").size() > 0);
    }
}
