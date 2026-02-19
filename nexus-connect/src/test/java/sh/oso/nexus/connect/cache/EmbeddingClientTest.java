package sh.oso.nexus.connect.cache;

import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import org.junit.jupiter.api.Test;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.junit.jupiter.api.Assertions.*;

@WireMockTest
class EmbeddingClientTest {

    @Test
    void embedReturnsCorrectDimensionVector(WireMockRuntimeInfo wmRuntimeInfo) {
        // Build a mock response with 1536-dimensional embedding
        StringBuilder embeddingArray = new StringBuilder("[");
        for (int i = 0; i < 1536; i++) {
            if (i > 0) embeddingArray.append(",");
            embeddingArray.append(String.format("%.6f", Math.random()));
        }
        embeddingArray.append("]");

        stubFor(post(urlEqualTo("/v1/embeddings"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"object\":\"list\",\"data\":[{\"object\":\"embedding\"," +
                                "\"embedding\":" + embeddingArray + ",\"index\":0}]," +
                                "\"model\":\"text-embedding-3-small\"," +
                                "\"usage\":{\"prompt_tokens\":5,\"total_tokens\":5}}")));

        EmbeddingClient client = new EmbeddingClient("test-key", "text-embedding-3-small",
                wmRuntimeInfo.getHttpBaseUrl());

        float[] result = client.embed("Hello world");

        assertNotNull(result);
        assertEquals(1536, result.length);

        verify(postRequestedFor(urlEqualTo("/v1/embeddings"))
                .withHeader("Authorization", equalTo("Bearer test-key"))
                .withRequestBody(containing("\"model\":\"text-embedding-3-small\""))
                .withRequestBody(containing("\"input\":\"Hello world\"")));
    }

    @Test
    void embedThrowsOnApiError(WireMockRuntimeInfo wmRuntimeInfo) {
        stubFor(post(urlEqualTo("/v1/embeddings"))
                .willReturn(aResponse()
                        .withStatus(500)
                        .withBody("{\"error\":\"Internal server error\"}")));

        EmbeddingClient client = new EmbeddingClient("test-key", "text-embedding-3-small",
                wmRuntimeInfo.getHttpBaseUrl());

        assertThrows(RuntimeException.class, () -> client.embed("test"));
    }

    @Test
    void requestContainsCorrectFormat(WireMockRuntimeInfo wmRuntimeInfo) {
        StringBuilder embeddingArray = new StringBuilder("[");
        for (int i = 0; i < 10; i++) {
            if (i > 0) embeddingArray.append(",");
            embeddingArray.append("0.1");
        }
        embeddingArray.append("]");

        stubFor(post(urlEqualTo("/v1/embeddings"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"data\":[{\"embedding\":" + embeddingArray + "}]}")));

        EmbeddingClient client = new EmbeddingClient("test-key", "text-embedding-3-small",
                wmRuntimeInfo.getHttpBaseUrl());

        float[] result = client.embed("test input");

        assertEquals(10, result.length);
    }
}
