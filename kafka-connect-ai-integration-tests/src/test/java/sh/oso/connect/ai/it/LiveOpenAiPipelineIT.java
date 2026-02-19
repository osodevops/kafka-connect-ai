package sh.oso.connect.ai.it;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.StreamEntryID;
import sh.oso.connect.ai.adapter.http.HttpSourceAdapter;
import sh.oso.connect.ai.adapter.jdbc.JdbcSinkAdapter;
import sh.oso.connect.ai.adapter.mongodb.MongoSinkAdapter;
import sh.oso.connect.ai.adapter.mongodb.MongoSourceAdapter;
import sh.oso.connect.ai.adapter.redis.RedisSinkAdapter;
import sh.oso.connect.ai.adapter.redis.RedisSourceAdapter;
import sh.oso.connect.ai.api.model.RawRecord;
import sh.oso.connect.ai.api.model.SourceOffset;
import sh.oso.connect.ai.api.model.TransformedRecord;
import sh.oso.connect.ai.connect.pipeline.BasicAgentPipeline;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end integration tests exercising the full pipeline with a LIVE OpenAI API key.
 *
 * Only runs when the OPENAI_API_KEY environment variable is set.
 * In CI, add the secret and it will be passed to the failsafe plugin.
 *
 * Pipelines tested:
 *   1. HTTP Source  → OpenAI → JDBC Sink (PostgreSQL)  — verify rows in PG
 *   2. MongoDB Source → OpenAI → MongoDB Sink           — verify docs in Mongo
 *   3. Redis Source → OpenAI → Redis Sink               — verify entries in Redis stream
 *   4. HTTP Source  → OpenAI → fan-out to all 3 sinks   — verify output in PG + Mongo + Redis
 */
@Testcontainers
@EnabledIfEnvironmentVariable(named = "OPENAI_API_KEY", matches = ".+")
class LiveOpenAiPipelineIT {

    static final Network network = Network.newNetwork();
    static final ObjectMapper objectMapper = new ObjectMapper();
    static HttpClient httpClient = HttpClient.newHttpClient();

    @Container
    static final GenericContainer<?> wiremock = new GenericContainer<>(
            DockerImageName.parse("wiremock/wiremock:3.9.2"))
            .withNetwork(network)
            .withNetworkAliases("wiremock")
            .withExposedPorts(8080)
            .withCommand("--verbose")
            .waitingFor(Wait.forHttp("/__admin/mappings").forPort(8080));

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse("postgres:16-alpine"))
            .withNetwork(network)
            .withNetworkAliases("postgres")
            .withDatabaseName("e2e_test")
            .withUsername("test")
            .withPassword("test");

    @Container
    static final MongoDBContainer mongo = new MongoDBContainer(
            DockerImageName.parse("mongo:7.0"))
            .withNetwork(network)
            .withNetworkAliases("mongo");

    @Container
    static final GenericContainer<?> redis = new GenericContainer<>(
            DockerImageName.parse("redis:7-alpine"))
            .withNetwork(network)
            .withNetworkAliases("redis")
            .withExposedPorts(6379);

    static MongoClient mongoClient;
    static JedisPooled jedis;
    static String redisUrl;

    @BeforeAll
    static void setup() throws Exception {
        mongoClient = MongoClients.create(mongo.getConnectionString());
        redisUrl = "redis://" + redis.getHost() + ":" + redis.getMappedPort(6379);
        jedis = new JedisPooled(redis.getHost(), redis.getMappedPort(6379));

        // Seed WireMock with simulated source APIs
        String wiremockUrl = "http://localhost:" + wiremock.getMappedPort(8080);

        // Product catalog API
        httpClient.send(HttpRequest.newBuilder()
                .uri(URI.create(wiremockUrl + "/__admin/mappings"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("""
                        {
                          "request": { "method": "GET", "url": "/api/products" },
                          "response": {
                            "status": 200,
                            "headers": { "Content-Type": "application/json" },
                            "jsonBody": [
                              {"sku": "LAPTOP-001", "raw_name": "laptop gaming 15inch rtx4070", "price_cents": 129999, "category_raw": "electronics/computers"},
                              {"sku": "SHOE-042", "raw_name": "mens running shoe size 10 blue", "price_cents": 8999, "category_raw": "apparel/footwear"},
                              {"sku": "BOOK-789", "raw_name": "intro to machine learning hardcover 2024", "price_cents": 4599, "category_raw": "books/technical"}
                            ]
                          }
                        }
                        """))
                .build(), HttpResponse.BodyHandlers.ofString());

        // Support tickets API
        httpClient.send(HttpRequest.newBuilder()
                .uri(URI.create(wiremockUrl + "/__admin/mappings"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("""
                        {
                          "request": { "method": "GET", "url": "/api/tickets" },
                          "response": {
                            "status": 200,
                            "headers": { "Content-Type": "application/json" },
                            "jsonBody": [
                              {"ticket_id": "T-1001", "subject": "cant login to my account", "body": "i forgot my password and the reset email never came", "priority_raw": "unknown"},
                              {"ticket_id": "T-1002", "subject": "billing is wrong", "body": "i was charged twice for my subscription last month", "priority_raw": "unknown"},
                              {"ticket_id": "T-1003", "subject": "feature request dark mode", "body": "please add dark mode to the mobile app it hurts my eyes at night", "priority_raw": "unknown"}
                            ]
                          }
                        }
                        """))
                .build(), HttpResponse.BodyHandlers.ofString());

        // Seed MongoDB with web analytics events
        MongoDatabase db = mongoClient.getDatabase("e2e_test");
        MongoCollection<Document> events = db.getCollection("raw_events");
        events.drop();
        events.insertMany(List.of(
                Document.parse("{\"event_type\":\"page_view\",\"url\":\"/products/laptop\",\"user_agent\":\"Mozilla/5.0\",\"timestamp\":\"2024-01-15T10:30:00Z\"}"),
                Document.parse("{\"event_type\":\"add_to_cart\",\"url\":\"/cart/add\",\"product_id\":\"LAPTOP-001\",\"timestamp\":\"2024-01-15T10:31:00Z\"}"),
                Document.parse("{\"event_type\":\"purchase\",\"url\":\"/checkout/complete\",\"order_id\":\"ORD-5678\",\"amount_cents\":129999,\"timestamp\":\"2024-01-15T10:35:00Z\"}")
        ));

        // Seed Redis with IoT sensor readings
        String sensorStream = "e2e:sensors:raw";
        jedis.xadd(sensorStream, StreamEntryID.NEW_ENTRY,
                Map.of("data", "{\"sensor_id\":\"temp-01\",\"reading\":22.5,\"unit\":\"celsius\",\"location\":\"warehouse-A\"}"));
        jedis.xadd(sensorStream, StreamEntryID.NEW_ENTRY,
                Map.of("data", "{\"sensor_id\":\"humidity-01\",\"reading\":65.2,\"unit\":\"percent\",\"location\":\"warehouse-A\"}"));
        jedis.xadd(sensorStream, StreamEntryID.NEW_ENTRY,
                Map.of("data", "{\"sensor_id\":\"temp-02\",\"reading\":38.1,\"unit\":\"celsius\",\"location\":\"server-room-B\"}"));
        try {
            jedis.xgroupCreate(sensorStream, "e2e-group", new StreamEntryID("0-0"), true);
        } catch (Exception ignored) {}
    }

    @AfterAll
    static void tearDown() {
        if (mongoClient != null) mongoClient.close();
        if (jedis != null) jedis.close();
    }

    // ───────────────────────────────────────────────────────────────────────────
    // Test 1: HTTP Source → OpenAI (product enrichment) → JDBC Sink (PostgreSQL)
    // ───────────────────────────────────────────────────────────────────────────

    @Test
    void httpSourceToOpenAiToJdbcSink() throws Exception {
        String wiremockUrl = "http://localhost:" + wiremock.getMappedPort(8080);

        // Source: fetch products from HTTP API
        HttpSourceAdapter source = new HttpSourceAdapter();
        source.start(Map.of(
                "http.source.url", wiremockUrl + "/api/products",
                "http.source.method", "GET",
                "http.source.poll.interval.ms", "0"
        ));
        List<RawRecord> raw = source.fetch(SourceOffset.empty(), 100);
        assertEquals(3, raw.size());
        source.stop();

        // Transform: enrich product data via OpenAI
        List<TransformedRecord> transformed = transformWithOpenAi(raw,
                "You are a product data enrichment agent. " +
                "For each product record, produce a cleaned version with exactly these fields: " +
                "sku (string, keep original), display_name (proper title case product name), " +
                "price (decimal number in dollars, convert from cents), " +
                "category (single word: Electronics/Apparel/Books). " +
                "Respond with ONLY a JSON array of objects.");

        assertFalse(transformed.isEmpty(), "OpenAI should return transformed products");

        // Sink: write to PostgreSQL
        try (Connection conn = DriverManager.getConnection(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE IF NOT EXISTS products (" +
                    "sku TEXT PRIMARY KEY, display_name TEXT, price NUMERIC, category TEXT)");
        }

        JdbcSinkAdapter sink = new JdbcSinkAdapter();
        sink.start(Map.of(
                "jdbc.url", postgres.getJdbcUrl(),
                "jdbc.user", postgres.getUsername(),
                "jdbc.password", postgres.getPassword(),
                "jdbc.table", "products",
                "jdbc.sink.insert.mode", "upsert",
                "jdbc.sink.pk.columns", "sku",
                "jdbc.sink.auto.ddl", "false"
        ));
        sink.write(transformed);
        sink.stop();

        // Verify: check rows in PostgreSQL
        try (Connection conn = DriverManager.getConnection(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT sku, display_name, price, category FROM products ORDER BY sku")) {
            int count = 0;
            while (rs.next()) {
                count++;
                assertNotNull(rs.getString("sku"), "sku should not be null");
                assertNotNull(rs.getString("display_name"), "display_name should not be null");
            }
            assertTrue(count > 0, "PostgreSQL should have product rows");
        }
    }

    // ───────────────────────────────────────────────────────────────────────────
    // Test 2: MongoDB Source → OpenAI (event classification) → MongoDB Sink
    // ───────────────────────────────────────────────────────────────────────────

    @Test
    void mongoSourceToOpenAiToMongoSink() throws Exception {
        // Source: fetch web analytics events from MongoDB
        MongoSourceAdapter source = new MongoSourceAdapter();
        source.start(Map.of(
                "mongodb.connection.string", mongo.getConnectionString(),
                "mongodb.database", "e2e_test",
                "mongodb.collection", "raw_events",
                "mongodb.poll.mode", "polling",
                "mongodb.poll.interval.ms", "0"
        ));
        List<RawRecord> raw = source.fetch(SourceOffset.empty(), 100);
        assertEquals(3, raw.size());
        source.stop();

        // Transform: classify events via OpenAI
        List<TransformedRecord> transformed = transformWithOpenAi(raw,
                "You are a web analytics event classifier. " +
                "For each event, produce a classified version with exactly these fields: " +
                "event_type (keep original), category (one of: browsing, conversion, engagement), " +
                "purchase_intent (one of: low, medium, high), " +
                "summary (one brief sentence describing the user action). " +
                "Respond with ONLY a JSON array of objects.");

        assertFalse(transformed.isEmpty(), "OpenAI should return classified events");

        // Sink: write classified events to a new MongoDB collection
        MongoSinkAdapter sink = new MongoSinkAdapter();
        sink.start(Map.of(
                "mongodb.connection.string", mongo.getConnectionString(),
                "mongodb.database", "e2e_test",
                "mongodb.collection", "classified_events",
                "mongodb.write.mode", "insert",
                "mongodb.batch.size", "100"
        ));
        sink.write(transformed);
        sink.stop();

        // Verify: check documents in MongoDB
        MongoCollection<Document> classified = mongoClient
                .getDatabase("e2e_test")
                .getCollection("classified_events");
        long docCount = classified.countDocuments();
        assertTrue(docCount > 0, "MongoDB should have classified event documents, got " + docCount);

        for (Document doc : classified.find()) {
            // Each classified event should have the enrichment fields
            assertTrue(doc.containsKey("event_type") || doc.containsKey("category") || doc.containsKey("purchase_intent"),
                    "Classified event should have enrichment fields: " + doc.toJson());
        }
    }

    // ───────────────────────────────────────────────────────────────────────────
    // Test 3: Redis Source → OpenAI (IoT anomaly detection) → Redis Sink
    // ───────────────────────────────────────────────────────────────────────────

    @Test
    void redisSourceToOpenAiToRedisSink() throws Exception {
        // Source: fetch IoT sensor readings from Redis stream
        RedisSourceAdapter source = new RedisSourceAdapter();
        source.start(Map.of(
                "redis.url", redisUrl,
                "redis.source.mode", "stream",
                "redis.stream.key", "e2e:sensors:raw",
                "redis.consumer.group", "e2e-group",
                "redis.consumer.name", "e2e-consumer",
                "redis.poll.interval.ms", "0"
        ));
        List<RawRecord> raw = source.fetch(SourceOffset.empty(), 100);
        assertEquals(3, raw.size(), "Should read 3 sensor readings from Redis stream");
        source.stop();

        // Transform: detect anomalies via OpenAI
        List<TransformedRecord> transformed = transformWithOpenAi(raw,
                "You are an IoT sensor monitoring agent. " +
                "For each sensor reading, produce an analysis with exactly these fields: " +
                "sensor_id (keep original), reading (keep original number), unit (keep original), " +
                "location (keep original), status (one of: normal, warning, critical), " +
                "alert (brief reason if warning/critical, empty string if normal). " +
                "A temperature above 35C in a server room is critical. " +
                "Respond with ONLY a JSON array of objects.");

        assertFalse(transformed.isEmpty(), "OpenAI should return sensor analyses");

        // Sink: write analysed readings to a new Redis stream
        RedisSinkAdapter sink = new RedisSinkAdapter();
        sink.start(Map.of(
                "redis.url", redisUrl,
                "redis.sink.mode", "stream",
                "redis.stream.key", "e2e:sensors:analysed"
        ));
        sink.write(transformed);
        sink.stop();

        // Verify: check entries in Redis output stream
        long streamLen = jedis.xlen("e2e:sensors:analysed");
        assertTrue(streamLen > 0, "Redis output stream should have analysed entries, got " + streamLen);
    }

    // ───────────────────────────────────────────────────────────────────────────
    // Test 4: HTTP Source → OpenAI → fan-out to ALL 3 sinks (PG + Mongo + Redis)
    // ───────────────────────────────────────────────────────────────────────────

    @Test
    void httpSourceToOpenAiFanOutToAllSinks() throws Exception {
        String wiremockUrl = "http://localhost:" + wiremock.getMappedPort(8080);

        // Source: fetch support tickets from HTTP API
        HttpSourceAdapter source = new HttpSourceAdapter();
        source.start(Map.of(
                "http.source.url", wiremockUrl + "/api/tickets",
                "http.source.method", "GET",
                "http.source.poll.interval.ms", "0"
        ));
        List<RawRecord> raw = source.fetch(SourceOffset.empty(), 100);
        assertEquals(3, raw.size());
        source.stop();

        // Transform: triage tickets via OpenAI
        List<TransformedRecord> transformed = transformWithOpenAi(raw,
                "You are a support ticket triage agent. " +
                "For each ticket, produce a triaged version with exactly these fields: " +
                "ticket_id (keep original), subject (cleaned up, proper case), " +
                "priority (one of: low, medium, high, critical), " +
                "department (one of: account, billing, product), " +
                "sentiment (one of: frustrated, neutral, positive). " +
                "Respond with ONLY a JSON array of objects.");

        assertFalse(transformed.isEmpty(), "OpenAI should return triaged tickets");

        // --- Sink 1: PostgreSQL ---
        try (Connection conn = DriverManager.getConnection(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE IF NOT EXISTS tickets (" +
                    "ticket_id TEXT PRIMARY KEY, subject TEXT, priority TEXT, " +
                    "department TEXT, sentiment TEXT)");
        }

        JdbcSinkAdapter jdbcSink = new JdbcSinkAdapter();
        jdbcSink.start(Map.of(
                "jdbc.url", postgres.getJdbcUrl(),
                "jdbc.user", postgres.getUsername(),
                "jdbc.password", postgres.getPassword(),
                "jdbc.table", "tickets",
                "jdbc.sink.insert.mode", "upsert",
                "jdbc.sink.pk.columns", "ticket_id",
                "jdbc.sink.auto.ddl", "false"
        ));
        jdbcSink.write(transformed);
        jdbcSink.stop();

        // --- Sink 2: MongoDB ---
        MongoSinkAdapter mongoSink = new MongoSinkAdapter();
        mongoSink.start(Map.of(
                "mongodb.connection.string", mongo.getConnectionString(),
                "mongodb.database", "e2e_test",
                "mongodb.collection", "triaged_tickets",
                "mongodb.write.mode", "insert",
                "mongodb.batch.size", "100"
        ));
        mongoSink.write(transformed);
        mongoSink.stop();

        // --- Sink 3: Redis ---
        RedisSinkAdapter redisSink = new RedisSinkAdapter();
        redisSink.start(Map.of(
                "redis.url", redisUrl,
                "redis.sink.mode", "stream",
                "redis.stream.key", "e2e:tickets:triaged"
        ));
        redisSink.write(transformed);
        redisSink.stop();

        // === Verify all 3 sinks received the data ===

        // PostgreSQL
        try (Connection conn = DriverManager.getConnection(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM tickets")) {
            assertTrue(rs.next());
            int pgCount = rs.getInt(1);
            assertTrue(pgCount > 0, "PostgreSQL should have triaged tickets, got " + pgCount);
        }

        // MongoDB
        long mongoCount = mongoClient.getDatabase("e2e_test")
                .getCollection("triaged_tickets")
                .countDocuments();
        assertTrue(mongoCount > 0, "MongoDB should have triaged tickets, got " + mongoCount);

        // Redis
        long redisStreamLen = jedis.xlen("e2e:tickets:triaged");
        assertTrue(redisStreamLen > 0, "Redis should have triaged ticket entries, got " + redisStreamLen);
    }

    // ───────────────────────────────────────────────────────────────────────────
    // Helper: shared OpenAI transform
    // ───────────────────────────────────────────────────────────────────────────

    private List<TransformedRecord> transformWithOpenAi(List<RawRecord> records, String systemPrompt) {
        String openAiKey = System.getenv("OPENAI_API_KEY");
        BasicAgentPipeline pipeline = new BasicAgentPipeline();
        pipeline.configure(Map.of(
                "connect.ai.llm.provider", "openai",
                "connect.ai.llm.api.key", openAiKey,
                "connect.ai.llm.model", "gpt-4o-mini",
                "connect.ai.llm.temperature", "0.0",
                "connect.ai.llm.max.tokens", "2048",
                "connect.ai.agent.system.prompt", systemPrompt,
                "connect.ai.structured.output", "false",
                "connect.ai.agent.enable.prompt.caching", "false"
        ));
        return pipeline.process(records);
    }
}
