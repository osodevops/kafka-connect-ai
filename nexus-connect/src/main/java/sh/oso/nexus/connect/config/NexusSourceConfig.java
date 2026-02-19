package sh.oso.nexus.connect.config;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import sh.oso.nexus.api.config.NexusConfig;

import java.util.Map;

public class NexusSourceConfig extends AbstractConfig {

    public static final ConfigDef BASE_CONFIG = new ConfigDef()
            .define(
                    NexusConfig.SOURCE_ADAPTER,
                    ConfigDef.Type.STRING,
                    ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.Importance.HIGH,
                    "Source adapter type (e.g., 'http')")
            .define(
                    NexusConfig.TOPIC,
                    ConfigDef.Type.STRING,
                    ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.Importance.HIGH,
                    "Target Kafka topic for source records")
            .define(
                    NexusConfig.LLM_PROVIDER,
                    ConfigDef.Type.STRING,
                    "anthropic",
                    ConfigDef.Importance.HIGH,
                    "LLM provider: 'anthropic' or 'openai'")
            .define(
                    NexusConfig.LLM_API_KEY,
                    ConfigDef.Type.PASSWORD,
                    "",
                    ConfigDef.Importance.HIGH,
                    "API key for the LLM provider")
            .define(
                    NexusConfig.LLM_MODEL,
                    ConfigDef.Type.STRING,
                    "claude-sonnet-4-20250514",
                    ConfigDef.Importance.MEDIUM,
                    "LLM model identifier")
            .define(
                    NexusConfig.LLM_BASE_URL,
                    ConfigDef.Type.STRING,
                    "",
                    ConfigDef.Importance.LOW,
                    "Override base URL for LLM API (useful for testing)")
            .define(
                    NexusConfig.LLM_TEMPERATURE,
                    ConfigDef.Type.DOUBLE,
                    0.0,
                    ConfigDef.Importance.LOW,
                    "LLM temperature (0.0-1.0)")
            .define(
                    NexusConfig.AGENT_SYSTEM_PROMPT,
                    ConfigDef.Type.STRING,
                    "",
                    ConfigDef.Importance.HIGH,
                    "System prompt describing the transformation task")
            .define(
                    NexusConfig.AGENT_TARGET_SCHEMA,
                    ConfigDef.Type.STRING,
                    "",
                    ConfigDef.Importance.MEDIUM,
                    "JSON Schema for validating LLM output")
            .define(
                    NexusConfig.AGENT_ENABLE_PROMPT_CACHING,
                    ConfigDef.Type.BOOLEAN,
                    true,
                    ConfigDef.Importance.LOW,
                    "Enable prompt caching for supported providers")
            .define(
                    NexusConfig.DLQ_TOPIC,
                    ConfigDef.Type.STRING,
                    "",
                    ConfigDef.Importance.LOW,
                    "Dead-letter queue topic for failed records")
            .define(
                    NexusConfig.LLM_MAX_TOKENS,
                    ConfigDef.Type.INT,
                    4096,
                    ConfigDef.Importance.LOW,
                    "Maximum tokens for LLM response")
            .define(
                    NexusConfig.BATCH_SIZE,
                    ConfigDef.Type.INT,
                    100,
                    ConfigDef.Importance.MEDIUM,
                    "Maximum records per batch")
            .define(
                    NexusConfig.AGENT_SCHEMA_DISCOVERY,
                    ConfigDef.Type.BOOLEAN,
                    false,
                    ConfigDef.Importance.MEDIUM,
                    "Enable LLM-based schema discovery from sample data")
            .define(
                    NexusConfig.AGENT_MAX_RETRIES,
                    ConfigDef.Type.INT,
                    1,
                    ConfigDef.Importance.LOW,
                    "Maximum LLM parse retries on malformed output");

    public NexusSourceConfig(Map<String, String> props) {
        super(configDef(props), props);
    }

    public static ConfigDef configDef(Map<String, String> props) {
        return BASE_CONFIG;
    }

    public String sourceAdapterType() {
        return getString(NexusConfig.SOURCE_ADAPTER);
    }

    public String topic() {
        return getString(NexusConfig.TOPIC);
    }

    public String llmProvider() {
        return getString(NexusConfig.LLM_PROVIDER);
    }

    public String llmApiKey() {
        return getPassword(NexusConfig.LLM_API_KEY).value();
    }

    public String llmModel() {
        return getString(NexusConfig.LLM_MODEL);
    }

    public String llmBaseUrl() {
        return getString(NexusConfig.LLM_BASE_URL);
    }

    public double llmTemperature() {
        return getDouble(NexusConfig.LLM_TEMPERATURE);
    }

    public String agentSystemPrompt() {
        return getString(NexusConfig.AGENT_SYSTEM_PROMPT);
    }

    public String agentTargetSchema() {
        return getString(NexusConfig.AGENT_TARGET_SCHEMA);
    }

    public boolean enablePromptCaching() {
        return getBoolean(NexusConfig.AGENT_ENABLE_PROMPT_CACHING);
    }

    public String dlqTopic() {
        return getString(NexusConfig.DLQ_TOPIC);
    }
}
