package sh.oso.connect.ai.connect.config;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class AiSourceConfigTest {

    @Test
    void validConfigWithRequiredProperties() {
        Map<String, String> props = new HashMap<>();
        props.put("connect.ai.source.adapter", "http");
        props.put("connect.ai.topic", "my-topic");

        AiSourceConfig config = new AiSourceConfig(props);

        assertEquals("http", config.sourceAdapterType());
        assertEquals("my-topic", config.topic());
    }

    @Test
    void missingSourceAdapterThrowsConfigException() {
        Map<String, String> props = new HashMap<>();
        props.put("connect.ai.topic", "my-topic");

        assertThrows(ConfigException.class, () -> new AiSourceConfig(props));
    }

    @Test
    void missingTopicThrowsConfigException() {
        Map<String, String> props = new HashMap<>();
        props.put("connect.ai.source.adapter", "http");

        assertThrows(ConfigException.class, () -> new AiSourceConfig(props));
    }

    @Test
    void defaultValuesForOptionalProperties() {
        Map<String, String> props = new HashMap<>();
        props.put("connect.ai.source.adapter", "http");
        props.put("connect.ai.topic", "my-topic");

        AiSourceConfig config = new AiSourceConfig(props);

        assertEquals("anthropic", config.llmProvider());
        assertEquals("claude-sonnet-4-20250514", config.llmModel());
        assertEquals("", config.llmBaseUrl());
        assertEquals(0.0, config.llmTemperature());
        assertEquals("", config.agentSystemPrompt());
        assertEquals("", config.agentTargetSchema());
        assertTrue(config.enablePromptCaching());
        assertEquals("", config.dlqTopic());
    }
}
