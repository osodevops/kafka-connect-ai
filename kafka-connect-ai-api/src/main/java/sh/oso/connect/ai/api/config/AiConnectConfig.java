package sh.oso.connect.ai.api.config;

public final class AiConnectConfig {

    private AiConnectConfig() {}

    // Adapter
    public static final String SOURCE_ADAPTER = "connect.ai.source.adapter";
    public static final String SINK_ADAPTER = "connect.ai.sink.adapter";

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
    public static final String BATCH_SIZE = "connect.ai.batch.size";
    public static final String STRUCTURED_OUTPUT = "ai.structured.output";
    public static final String RETRY_BACKOFF_MS = "ai.retry.backoff.ms";
    public static final String AGENT_TARGET_TOPIC_FORMAT = "ai.agent.target.topic.format";

    // Kafka
    public static final String TOPIC = "connect.ai.topic";
    public static final String DLQ_TOPIC = "connect.ai.dlq.topic";
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

    // Compiled transform (compile-once-execute-forever)
    public static final String COMPILED_TRANSFORM_ENABLED = "ai.compiled.transform.enabled";
    public static final String COMPILED_TRANSFORM_TTL_SECONDS = "ai.compiled.transform.ttl.seconds";
    public static final String COMPILED_TRANSFORM_JS_TIMEOUT_MS = "ai.compiled.transform.js.timeout.ms";
    public static final String COMPILED_TRANSFORM_JS_HEAP_MB = "ai.compiled.transform.js.heap.mb";
    public static final String COMPILED_TRANSFORM_VALIDATION_SAMPLES = "ai.compiled.transform.validation.samples";

    // Connector class names (fully-qualified)
    public static final String SOURCE_CONNECTOR_CLASS = "sh.oso.connect.ai.connect.source.AiSourceConnector";
    public static final String SINK_CONNECTOR_CLASS = "sh.oso.connect.ai.connect.sink.AiSinkConnector";

    // HTTP adapter response format
    public static final String HTTP_RESPONSE_FORMAT = "http.response.format";

    // LLM rate limiting
    public static final String LLM_RATE_LIMIT = "ai.llm.rate.limit";

    // PII masking / privacy
    public static final String PRIVACY_MASK_FIELDS = "ai.privacy.mask.fields";
    public static final String PRIVACY_MASK_PATTERNS = "ai.privacy.mask.patterns";
    public static final String PRIVACY_MASK_REPLACEMENT = "ai.privacy.mask.replacement";
    public static final String PRIVACY_UNMASK_OUTPUT = "ai.privacy.unmask.output";

    // Circuit breaker
    public static final String CIRCUIT_BREAKER_ENABLED = "connect.ai.circuit.breaker.enabled";
    public static final String CIRCUIT_BREAKER_FAILURE_RATE_THRESHOLD = "connect.ai.circuit.breaker.failure.rate.threshold";
    public static final String CIRCUIT_BREAKER_WAIT_DURATION_MS = "connect.ai.circuit.breaker.wait.duration.ms";
    public static final String CIRCUIT_BREAKER_SLIDING_WINDOW_SIZE = "connect.ai.circuit.breaker.sliding.window.size";

    // MongoDB
    public static final String MONGODB_CONNECTION_STRING = "mongodb.connection.string";
    public static final String MONGODB_DATABASE = "mongodb.database";
    public static final String MONGODB_COLLECTION = "mongodb.collection";
    public static final String MONGODB_POLL_MODE = "mongodb.poll.mode";
    public static final String MONGODB_PIPELINE = "mongodb.pipeline";
    public static final String MONGODB_POLL_INTERVAL_MS = "mongodb.poll.interval.ms";
    public static final String MONGODB_WRITE_MODE = "mongodb.write.mode";
    public static final String MONGODB_BATCH_SIZE = "mongodb.batch.size";

    // Warehouse (sink)
    public static final String WAREHOUSE_PROVIDER = "warehouse.provider";
    public static final String WAREHOUSE_BATCH_SIZE = "warehouse.batch.size";

    // Snowflake
    public static final String SNOWFLAKE_URL = "snowflake.url";
    public static final String SNOWFLAKE_USER = "snowflake.user";
    public static final String SNOWFLAKE_PRIVATE_KEY = "snowflake.private.key";
    public static final String SNOWFLAKE_DATABASE = "snowflake.database";
    public static final String SNOWFLAKE_SCHEMA = "snowflake.schema";
    public static final String SNOWFLAKE_TABLE = "snowflake.table";
    public static final String SNOWFLAKE_WAREHOUSE = "snowflake.warehouse";
    public static final String SNOWFLAKE_ROLE = "snowflake.role";

    // Redshift
    public static final String REDSHIFT_JDBC_URL = "redshift.jdbc.url";
    public static final String REDSHIFT_USER = "redshift.user";
    public static final String REDSHIFT_PASSWORD = "redshift.password";
    public static final String REDSHIFT_TABLE = "redshift.table";
    public static final String REDSHIFT_S3_BUCKET = "redshift.s3.bucket";
    public static final String REDSHIFT_S3_REGION = "redshift.s3.region";
    public static final String REDSHIFT_IAM_ROLE = "redshift.iam.role";

    // BigQuery
    public static final String BIGQUERY_PROJECT = "bigquery.project";
    public static final String BIGQUERY_DATASET = "bigquery.dataset";
    public static final String BIGQUERY_TABLE = "bigquery.table";
    public static final String BIGQUERY_CREDENTIALS_PATH = "bigquery.credentials.path";

    // gRPC
    public static final String GRPC_TARGET = "grpc.target";
    public static final String GRPC_SERVICE = "grpc.service";
    public static final String GRPC_METHOD = "grpc.method";
    public static final String GRPC_MODE = "grpc.mode";
    public static final String GRPC_TLS_ENABLED = "grpc.tls.enabled";
    public static final String GRPC_PROTO_DESCRIPTOR_PATH = "grpc.proto.descriptor.path";
    public static final String GRPC_POLL_INTERVAL_MS = "grpc.poll.interval.ms";

    // Kinesis
    public static final String KINESIS_STREAM_NAME = "kinesis.stream.name";
    public static final String KINESIS_REGION = "kinesis.region";
    public static final String KINESIS_ITERATOR_TYPE = "kinesis.iterator.type";
    public static final String KINESIS_POLL_INTERVAL_MS = "kinesis.poll.interval.ms";
    public static final String KINESIS_BATCH_SIZE = "kinesis.batch.size";
    public static final String KINESIS_PARTITION_KEY = "kinesis.partition.key";

    // DynamoDB Streams
    public static final String DYNAMODB_TABLE_NAME = "dynamodb.table.name";
    public static final String DYNAMODB_REGION = "dynamodb.region";
    public static final String DYNAMODB_POLL_INTERVAL_MS = "dynamodb.poll.interval.ms";

    // WebSocket
    public static final String WEBSOCKET_URL = "websocket.url";
    public static final String WEBSOCKET_SUBPROTOCOL = "websocket.subprotocol";

    // SSE
    public static final String SSE_URL = "sse.url";
    public static final String SSE_RECONNECT_MS = "sse.reconnect.ms";

    // CometD
    public static final String COMETD_URL = "cometd.url";
    public static final String COMETD_CHANNEL = "cometd.channel";

    // Streaming common
    public static final String STREAMING_BUFFER_CAPACITY = "streaming.buffer.capacity";
    public static final String STREAMING_RECONNECT_BACKOFF_MS = "streaming.reconnect.backoff.ms";
    public static final String STREAMING_MAX_RECONNECT_BACKOFF_MS = "streaming.max.reconnect.backoff.ms";

    // Redis
    public static final String REDIS_URL = "redis.url";
    public static final String REDIS_SOURCE_MODE = "redis.source.mode";
    public static final String REDIS_SINK_MODE = "redis.sink.mode";
    public static final String REDIS_KEY = "redis.key";
    public static final String REDIS_CHANNEL = "redis.channel";
    public static final String REDIS_GROUP = "redis.group";
    public static final String REDIS_CONSUMER = "redis.consumer";
    public static final String REDIS_POLL_INTERVAL_MS = "redis.poll.interval.ms";
    public static final String REDIS_BATCH_SIZE = "redis.batch.size";

    // Cassandra
    public static final String CASSANDRA_CONTACT_POINTS = "cassandra.contact.points";
    public static final String CASSANDRA_PORT = "cassandra.port";
    public static final String CASSANDRA_DATACENTER = "cassandra.datacenter";
    public static final String CASSANDRA_KEYSPACE = "cassandra.keyspace";
    public static final String CASSANDRA_TABLE = "cassandra.table";
    public static final String CASSANDRA_USERNAME = "cassandra.username";
    public static final String CASSANDRA_PASSWORD = "cassandra.password";
    public static final String CASSANDRA_CONSISTENCY_LEVEL = "cassandra.consistency.level";
    public static final String CASSANDRA_TIMESTAMP_COLUMN = "cassandra.timestamp.column";
    public static final String CASSANDRA_POLL_INTERVAL_MS = "cassandra.poll.interval.ms";
    public static final String CASSANDRA_BATCH_SIZE = "cassandra.batch.size";

    // LDAP
    public static final String LDAP_URL = "ldap.url";
    public static final String LDAP_BIND_DN = "ldap.bind.dn";
    public static final String LDAP_BIND_PASSWORD = "ldap.bind.password";
    public static final String LDAP_BASE_DN = "ldap.base.dn";
    public static final String LDAP_FILTER = "ldap.filter";
    public static final String LDAP_SOURCE_MODE = "ldap.source.mode";
    public static final String LDAP_POLL_INTERVAL_MS = "ldap.poll.interval.ms";
    public static final String LDAP_ATTRIBUTES = "ldap.attributes";

    // SAP
    public static final String SAP_ASHOST = "sap.ashost";
    public static final String SAP_SYSNR = "sap.sysnr";
    public static final String SAP_CLIENT = "sap.client";
    public static final String SAP_USER = "sap.user";
    public static final String SAP_PASSWORD = "sap.password";
    public static final String SAP_LANG = "sap.lang";
    public static final String SAP_MODE = "sap.mode";
    public static final String SAP_FUNCTION = "sap.function";
    public static final String SAP_POLL_INTERVAL_MS = "sap.poll.interval.ms";
    public static final String SAP_GATEWAY_HOST = "sap.gateway.host";
    public static final String SAP_GATEWAY_SERVICE = "sap.gateway.service";
    public static final String SAP_PROGRAM_ID = "sap.program.id";

    // Cloud storage (utility)
    public static final String CLOUD_STORAGE_PROVIDER = "cloud.storage.provider";
    public static final String CLOUD_STORAGE_BUCKET = "cloud.storage.bucket";
    public static final String CLOUD_STORAGE_REGION = "cloud.storage.region";
}
