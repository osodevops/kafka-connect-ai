package sh.oso.nexus.api.config;

public final class NexusConfig {

    private NexusConfig() {}

    // Adapter
    public static final String SOURCE_ADAPTER = "nexus.source.adapter";
    public static final String SINK_ADAPTER = "nexus.sink.adapter";

    // LLM
    public static final String LLM_PROVIDER = "ai.llm.provider";
    public static final String LLM_API_KEY = "ai.llm.api.key";
    public static final String LLM_MODEL = "ai.llm.model";
    public static final String LLM_BASE_URL = "ai.llm.base.url";
    public static final String LLM_TEMPERATURE = "ai.llm.temperature";

    // Agent
    public static final String AGENT_SYSTEM_PROMPT = "ai.agent.system.prompt";
    public static final String AGENT_TARGET_SCHEMA = "ai.agent.target.schema";
    public static final String AGENT_ENABLE_PROMPT_CACHING = "ai.agent.enable.prompt.caching";

    // Agent (extended)
    public static final String AGENT_SCHEMA_DISCOVERY = "ai.schema.discovery";
    public static final String AGENT_MAX_RETRIES = "ai.max.retries";
    public static final String LLM_MAX_TOKENS = "ai.llm.max.tokens";
    public static final String LLM_ENDPOINT = "ai.llm.endpoint";
    public static final String BATCH_SIZE = "nexus.batch.size";
    public static final String STRUCTURED_OUTPUT = "ai.structured.output";
    public static final String RETRY_BACKOFF_MS = "ai.retry.backoff.ms";
    public static final String AGENT_TARGET_TOPIC_FORMAT = "ai.agent.target.topic.format";

    // Kafka
    public static final String TOPIC = "nexus.topic";
    public static final String DLQ_TOPIC = "nexus.dlq.topic";
    public static final String TASKS_MAX = "tasks.max";

    // JDBC
    public static final String JDBC_URL = "jdbc.url";
    public static final String JDBC_USER = "jdbc.user";
    public static final String JDBC_PASSWORD = "jdbc.password";
    public static final String JDBC_TABLE = "jdbc.table";
    public static final String JDBC_TABLES = "jdbc.tables";
    public static final String JDBC_QUERY = "jdbc.query";
    public static final String JDBC_DRIVER_CLASS = "jdbc.driver.class";
    public static final String JDBC_BATCH_SIZE = "jdbc.batch.size";
    public static final String JDBC_QUERY_MODE = "jdbc.query.mode";
    public static final String JDBC_TIMESTAMP_COLUMN = "jdbc.timestamp.column";
    public static final String JDBC_INCREMENTING_COLUMN = "jdbc.incrementing.column";
    public static final String JDBC_POLL_INTERVAL_MS = "jdbc.poll.interval.ms";
    public static final String JDBC_SINK_PK_COLUMNS = "jdbc.sink.pk.columns";
    public static final String JDBC_SINK_INSERT_MODE = "jdbc.sink.insert.mode";
    public static final String JDBC_SINK_AUTO_DDL = "jdbc.sink.auto.ddl";

    // Kafka Source (K2K)
    public static final String KAFKA_SOURCE_BOOTSTRAP_SERVERS = "kafka.source.bootstrap.servers";
    public static final String KAFKA_SOURCE_TOPICS = "kafka.source.topics";
    public static final String KAFKA_SOURCE_TOPICS_REGEX = "kafka.source.topics.regex";
    public static final String KAFKA_SOURCE_GROUP_ID = "kafka.source.group.id";
    public static final String KAFKA_SOURCE_POLL_TIMEOUT_MS = "kafka.source.poll.timeout.ms";
    public static final String KAFKA_SOURCE_SECURITY_PROTOCOL = "kafka.source.security.protocol";
    public static final String KAFKA_SOURCE_CONSUMER_PREFIX = "kafka.source.consumer.";

    // Multi-tier LLM model routing
    public static final String LLM_MODEL_FAST = "ai.llm.model.fast";
    public static final String LLM_MODEL_DEFAULT = "ai.llm.model.default";
    public static final String LLM_MODEL_POWERFUL = "ai.llm.model.powerful";

    // Batch accumulator
    public static final String BATCH_ACCUMULATOR_SIZE = "ai.batch.size";
    public static final String BATCH_ACCUMULATOR_MAX_WAIT_MS = "ai.batch.max.wait.ms";
    public static final String BATCH_PARALLEL_CALLS = "ai.batch.parallel.calls";
    public static final String CALL_TIMEOUT_SECONDS = "ai.call.timeout.seconds";

    // Semantic cache
    public static final String CACHE_ENABLED = "ai.cache.enabled";
    public static final String CACHE_REDIS_URL = "ai.cache.redis.url";
    public static final String CACHE_SIMILARITY_THRESHOLD = "ai.cache.similarity.threshold";
    public static final String CACHE_TTL_SECONDS = "ai.cache.ttl.seconds";
    public static final String CACHE_EMBEDDING_MODEL = "ai.cache.embedding.model";

    // Model router
    public static final String ROUTER_ENABLED = "ai.router.enabled";
    public static final String ROUTER_DETERMINISTIC_PATTERNS = "ai.router.deterministic.patterns";

    // Connector class names (fully-qualified)
    public static final String SOURCE_CONNECTOR_CLASS = "sh.oso.nexus.connect.source.NexusSourceConnector";
    public static final String SINK_CONNECTOR_CLASS = "sh.oso.nexus.connect.sink.NexusSinkConnector";

    // HTTP adapter response format
    public static final String HTTP_RESPONSE_FORMAT = "http.response.format";
}
