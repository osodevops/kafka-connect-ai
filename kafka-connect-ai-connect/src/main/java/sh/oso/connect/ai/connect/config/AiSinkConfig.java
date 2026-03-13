package sh.oso.connect.ai.connect.config;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import sh.oso.connect.ai.api.config.AiConnectConfig;

import java.util.Map;

public class AiSinkConfig extends AbstractConfig {

    public static final ConfigDef BASE_CONFIG = new ConfigDef()
            .define(
                    AiConnectConfig.SINK_ADAPTER,
                    ConfigDef.Type.STRING,
                    ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.Importance.HIGH,
                    "Sink adapter type (e.g., 'http')")
            .define(
                    AiConnectConfig.LLM_PROVIDER,
                    ConfigDef.Type.STRING,
                    "anthropic",
                    ConfigDef.Importance.HIGH,
                    "LLM provider: 'anthropic' or 'openai'")
            .define(
                    AiConnectConfig.LLM_API_KEY,
                    ConfigDef.Type.PASSWORD,
                    "",
                    ConfigDef.Importance.HIGH,
                    "API key for the LLM provider")
            .define(
                    AiConnectConfig.LLM_MODEL,
                    ConfigDef.Type.STRING,
                    "claude-sonnet-4-20250514",
                    ConfigDef.Importance.MEDIUM,
                    "LLM model identifier")
            .define(
                    AiConnectConfig.LLM_BASE_URL,
                    ConfigDef.Type.STRING,
                    "",
                    ConfigDef.Importance.LOW,
                    "Override base URL for LLM API")
            .define(
                    AiConnectConfig.LLM_TEMPERATURE,
                    ConfigDef.Type.DOUBLE,
                    0.0,
                    ConfigDef.Range.between(0.0, 2.0),
                    ConfigDef.Importance.LOW,
                    "LLM temperature (0.0-2.0)")
            .define(
                    AiConnectConfig.AGENT_SYSTEM_PROMPT,
                    ConfigDef.Type.STRING,
                    "",
                    ConfigDef.Importance.HIGH,
                    "System prompt describing the transformation task")
            .define(
                    AiConnectConfig.AGENT_TARGET_SCHEMA,
                    ConfigDef.Type.STRING,
                    "",
                    ConfigDef.Importance.MEDIUM,
                    "JSON Schema for validating LLM output")
            .define(
                    AiConnectConfig.AGENT_ENABLE_PROMPT_CACHING,
                    ConfigDef.Type.BOOLEAN,
                    true,
                    ConfigDef.Importance.LOW,
                    "Enable prompt caching for supported providers")
            .define(
                    AiConnectConfig.DLQ_TOPIC,
                    ConfigDef.Type.STRING,
                    "",
                    ConfigDef.Importance.LOW,
                    "Dead-letter queue topic for failed records")
            .define(
                    AiConnectConfig.LLM_MAX_TOKENS,
                    ConfigDef.Type.INT,
                    4096,
                    ConfigDef.Range.between(1, 128000),
                    ConfigDef.Importance.LOW,
                    "Maximum tokens for LLM response")
            .define(
                    AiConnectConfig.BATCH_SIZE,
                    ConfigDef.Type.INT,
                    100,
                    ConfigDef.Range.between(1, 100000),
                    ConfigDef.Importance.MEDIUM,
                    "Maximum records per batch")
            .define(
                    AiConnectConfig.BATCH_ACCUMULATOR_SIZE,
                    ConfigDef.Type.INT,
                    50,
                    ConfigDef.Range.between(1, 100000),
                    ConfigDef.Importance.MEDIUM,
                    "Batch accumulator buffer size")
            .define(
                    AiConnectConfig.BATCH_PARALLEL_CALLS,
                    ConfigDef.Type.INT,
                    1,
                    ConfigDef.Range.between(1, 64),
                    ConfigDef.Importance.LOW,
                    "Number of parallel LLM calls per batch")
            .define(
                    AiConnectConfig.AGENT_MAX_RETRIES,
                    ConfigDef.Type.INT,
                    1,
                    ConfigDef.Range.between(0, 10),
                    ConfigDef.Importance.LOW,
                    "Maximum LLM parse retries on malformed output")
            .define(
                    AiConnectConfig.CACHE_SIMILARITY_THRESHOLD,
                    ConfigDef.Type.DOUBLE,
                    0.95,
                    ConfigDef.Range.between(0.0, 1.0),
                    ConfigDef.Importance.LOW,
                    "Semantic cache similarity threshold (0.0-1.0)");

    public AiSinkConfig(Map<String, String> props) {
        super(configDef(props), props);
    }

    public static ConfigDef configDef(Map<String, String> props) {
        return BASE_CONFIG;
    }

    public String sinkAdapterType() {
        return getString(AiConnectConfig.SINK_ADAPTER);
    }

    public String llmProvider() {
        return getString(AiConnectConfig.LLM_PROVIDER);
    }

    public String llmApiKey() {
        return getPassword(AiConnectConfig.LLM_API_KEY).value();
    }

    public String llmModel() {
        return getString(AiConnectConfig.LLM_MODEL);
    }

    public String llmBaseUrl() {
        return getString(AiConnectConfig.LLM_BASE_URL);
    }

    public double llmTemperature() {
        return getDouble(AiConnectConfig.LLM_TEMPERATURE);
    }

    public String agentSystemPrompt() {
        return getString(AiConnectConfig.AGENT_SYSTEM_PROMPT);
    }

    public String agentTargetSchema() {
        return getString(AiConnectConfig.AGENT_TARGET_SCHEMA);
    }

    public boolean enablePromptCaching() {
        return getBoolean(AiConnectConfig.AGENT_ENABLE_PROMPT_CACHING);
    }

    public String dlqTopic() {
        return getString(AiConnectConfig.DLQ_TOPIC);
    }
}
