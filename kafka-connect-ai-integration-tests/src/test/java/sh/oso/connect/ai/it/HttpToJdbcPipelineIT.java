package sh.oso.connect.ai.it;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import sh.oso.connect.ai.adapter.http.HttpSourceAdapter;
import sh.oso.connect.ai.adapter.jdbc.JdbcSinkAdapter;
import sh.oso.connect.ai.api.model.RawRecord;
import sh.oso.connect.ai.api.model.SourceOffset;
import sh.oso.connect.ai.api.model.TransformedRecord;

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

@Testcontainers
class HttpToJdbcPipelineIT {

    static final Network network = Network.newNetwork();
    static final ObjectMapper objectMapper = new ObjectMapper();

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
            .withDatabaseName("connect_ai_test")
            .withUsername("test")
            .withPassword("test");

    static HttpClient httpClient = HttpClient.newHttpClient();

    @BeforeAll
    static void setupWireMock() throws Exception {
        String wiremockUrl = "http://localhost:" + wiremock.getMappedPort(8080);

        String sourceMapping = """
                {
                  "request": {
                    "method": "GET",
                    "url": "/api/users"
                  },
                  "response": {
                    "status": 200,
                    "headers": { "Content-Type": "application/json" },
                    "jsonBody": [
                      {"id": 1, "name": "Alice", "email": "alice@test.com"},
                      {"id": 2, "name": "Bob", "email": "bob@test.com"},
                      {"id": 3, "name": "Charlie", "email": "charlie@test.com"}
                    ]
                  }
                }
                """;

        httpClient.send(
                HttpRequest.newBuilder()
                        .uri(URI.create(wiremockUrl + "/__admin/mappings"))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(sourceMapping))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void httpSourceFetchesRecords() throws Exception {
        String wiremockUrl = "http://localhost:" + wiremock.getMappedPort(8080);

        HttpSourceAdapter source = new HttpSourceAdapter();
        source.start(Map.of(
                "http.source.url", wiremockUrl + "/api/users",
                "http.source.method", "GET",
                "http.source.poll.interval.ms", "0"
        ));

        List<RawRecord> records = source.fetch(SourceOffset.empty(), 100);
        assertNotNull(records);
        assertEquals(3, records.size());

        JsonNode first = objectMapper.readTree(
                new String(records.get(0).value(), StandardCharsets.UTF_8));
        assertEquals("Alice", first.get("name").asText());

        source.stop();
    }

    @Test
    void jdbcSinkWritesRecords() throws Exception {
        // Create test table first
        try (Connection conn = DriverManager.getConnection(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE IF NOT EXISTS sink_test (id INTEGER PRIMARY KEY, name TEXT, email TEXT)");
        }

        JdbcSinkAdapter sink = new JdbcSinkAdapter();
        sink.start(Map.of(
                "jdbc.url", postgres.getJdbcUrl(),
                "jdbc.user", postgres.getUsername(),
                "jdbc.password", postgres.getPassword(),
                "jdbc.table", "sink_test",
                "jdbc.sink.insert.mode", "insert",
                "jdbc.sink.pk.columns", "id"
        ));

        List<TransformedRecord> records = List.of(
                new TransformedRecord(null,
                        "{\"id\":1,\"name\":\"Alice\",\"email\":\"alice@test.com\"}".getBytes(StandardCharsets.UTF_8),
                        Map.of(), SourceOffset.empty()),
                new TransformedRecord(null,
                        "{\"id\":2,\"name\":\"Bob\",\"email\":\"bob@test.com\"}".getBytes(StandardCharsets.UTF_8),
                        Map.of(), SourceOffset.empty())
        );

        sink.write(records);

        // Verify data was written
        try (Connection conn = DriverManager.getConnection(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM sink_test")) {
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
        }

        sink.stop();
    }

    @Test
    void jdbcSinkUpsertUpdatesExistingRecords() throws Exception {
        try (Connection conn = DriverManager.getConnection(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE IF NOT EXISTS upsert_test (id INTEGER PRIMARY KEY, name TEXT)");
            stmt.execute("INSERT INTO upsert_test (id, name) VALUES (1, 'Old Name')");
        }

        JdbcSinkAdapter sink = new JdbcSinkAdapter();
        sink.start(Map.of(
                "jdbc.url", postgres.getJdbcUrl(),
                "jdbc.user", postgres.getUsername(),
                "jdbc.password", postgres.getPassword(),
                "jdbc.table", "upsert_test",
                "jdbc.sink.insert.mode", "upsert",
                "jdbc.sink.pk.columns", "id"
        ));

        sink.write(List.of(
                new TransformedRecord(null,
                        "{\"id\":1,\"name\":\"New Name\"}".getBytes(StandardCharsets.UTF_8),
                        Map.of(), SourceOffset.empty())
        ));

        try (Connection conn = DriverManager.getConnection(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT name FROM upsert_test WHERE id = 1")) {
            assertTrue(rs.next());
            assertEquals("New Name", rs.getString("name"));
        }

        sink.stop();
    }

    @Test
    void jdbcSinkAutoDdlCreatesTable() throws Exception {
        JdbcSinkAdapter sink = new JdbcSinkAdapter();
        sink.start(Map.of(
                "jdbc.url", postgres.getJdbcUrl(),
                "jdbc.user", postgres.getUsername(),
                "jdbc.password", postgres.getPassword(),
                "jdbc.table", "auto_ddl_test",
                "jdbc.sink.insert.mode", "insert",
                "jdbc.sink.auto.ddl", "true"
        ));

        sink.write(List.of(
                new TransformedRecord(null,
                        "{\"id\":1,\"label\":\"test\",\"count\":42}".getBytes(StandardCharsets.UTF_8),
                        Map.of(), SourceOffset.empty())
        ));

        try (Connection conn = DriverManager.getConnection(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM auto_ddl_test")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals("test", rs.getString("label"));
            assertEquals(42, rs.getInt("count"));
        }

        sink.stop();
    }
}
