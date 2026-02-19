package sh.oso.nexus.connect.config;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class NexusSourceConfigTest {

    @Test
    void validConfigWithRequiredProperties() {
        Map<String, String> props = new HashMap<>();
        props.put("nexus.source.adapter", "http");
        props.put("nexus.topic", "my-topic");

        NexusSourceConfig config = new NexusSourceConfig(props);

        assertEquals("http", config.sourceAdapterType());
        assertEquals("my-topic", config.topic());
    }

    @Test
    void missingSourceAdapterThrowsConfigException() {
        Map<String, String> props = new HashMap<>();
        props.put("nexus.topic", "my-topic");

        assertThrows(ConfigException.class, () -> new NexusSourceConfig(props));
    }

    @Test
    void missingTopicThrowsConfigException() {
        Map<String, String> props = new HashMap<>();
        props.put("nexus.source.adapter", "http");

        assertThrows(ConfigException.class, () -> new NexusSourceConfig(props));
    }

    @Test
    void defaultValuesForOptionalProperties() {
        Map<String, String> props = new HashMap<>();
        props.put("nexus.source.adapter", "http");
        props.put("nexus.topic", "my-topic");

        NexusSourceConfig config = new NexusSourceConfig(props);

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
