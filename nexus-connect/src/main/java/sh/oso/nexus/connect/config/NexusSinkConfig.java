package sh.oso.nexus.connect.config;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import sh.oso.nexus.api.config.NexusConfig;

import java.util.Map;

public class NexusSinkConfig extends AbstractConfig {

    public static final ConfigDef BASE_CONFIG = new ConfigDef()
            .define(
                    NexusConfig.SINK_ADAPTER,
                    ConfigDef.Type.STRING,
                    ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.Importance.HIGH,
                    "Sink adapter type (e.g., 'http')")
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
                    "Override base URL for LLM API")
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
                    "Dead-letter queue topic for failed records");

    public NexusSinkConfig(Map<String, String> props) {
        super(configDef(props), props);
    }

    public static ConfigDef configDef(Map<String, String> props) {
        return BASE_CONFIG;
    }

    public String sinkAdapterType() {
        return getString(NexusConfig.SINK_ADAPTER);
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
