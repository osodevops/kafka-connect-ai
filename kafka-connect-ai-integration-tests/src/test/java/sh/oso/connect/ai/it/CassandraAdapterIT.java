package sh.oso.connect.ai.it;

import com.datastax.oss.driver.api.core.CqlSession;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import sh.oso.connect.ai.adapter.cassandra.CassandraSinkAdapter;
import sh.oso.connect.ai.adapter.cassandra.CassandraSourceAdapter;
import sh.oso.connect.ai.api.model.RawRecord;
import sh.oso.connect.ai.api.model.SourceOffset;
import sh.oso.connect.ai.api.model.TransformedRecord;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for Cassandra source and sink adapters using Testcontainers.
 */
@Testcontainers
class CassandraAdapterIT {

    static final ObjectMapper objectMapper = new ObjectMapper();

    @Container
    static final CassandraContainer<?> cassandra = new CassandraContainer<>(
            DockerImageName.parse("cassandra:4.1"))
            .withExposedPorts(9042);

    static CqlSession directSession;
    static String contactPoint;

    @BeforeAll
    static void setUp() {
        contactPoint = cassandra.getHost() + ":" + cassandra.getMappedPort(9042);

        directSession = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(cassandra.getHost(), cassandra.getMappedPort(9042)))
                .withLocalDatacenter("datacenter1")
                .build();

        // Create keyspace and table
        directSession.execute(
                "CREATE KEYSPACE IF NOT EXISTS test_ks " +
                "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
        directSession.execute(
                "CREATE TABLE IF NOT EXISTS test_ks.users (" +
                "id text PRIMARY KEY, name text, email text, score int)");
        directSession.execute(
                "CREATE TABLE IF NOT EXISTS test_ks.events (" +
                "id text PRIMARY KEY, event_type text, payload text, created_at timestamp)");
    }

    @AfterAll
    static void tearDown() {
        if (directSession != null) {
            directSession.close();
        }
    }

    @Test
    void sinkWritesToCassandraTable() throws Exception {
        CassandraSinkAdapter sink = new CassandraSinkAdapter();
        sink.start(Map.of(
                "cassandra.contact.points", contactPoint,
                "cassandra.datacenter", "datacenter1",
                "cassandra.keyspace", "test_ks",
                "cassandra.table", "users",
                "cassandra.consistency.level", "LOCAL_ONE"
        ));

        sink.write(List.of(
                new TransformedRecord(null,
                        "{\"id\":\"u1\",\"name\":\"Alice\",\"email\":\"alice@test.com\",\"score\":95}"
                                .getBytes(StandardCharsets.UTF_8),
                        Map.of(), SourceOffset.empty()),
                new TransformedRecord(null,
                        "{\"id\":\"u2\",\"name\":\"Bob\",\"email\":\"bob@test.com\",\"score\":87}"
                                .getBytes(StandardCharsets.UTF_8),
                        Map.of(), SourceOffset.empty())
        ));
        sink.flush();

        // Verify data was written
        long count = directSession.execute("SELECT count(*) FROM test_ks.users")
                .one().getLong(0);
        assertEquals(2, count);

        String aliceName = directSession.execute(
                "SELECT name FROM test_ks.users WHERE id = 'u1'")
                .one().getString("name");
        assertEquals("Alice", aliceName);

        assertTrue(sink.isHealthy());
        sink.stop();
    }

    @Test
    void sourceReadsFromCassandraTable() throws Exception {
        // Insert test data directly
        directSession.execute(
                "INSERT INTO test_ks.events (id, event_type, payload) VALUES ('e1', 'login', '{\"user\":\"alice\"}')");
        directSession.execute(
                "INSERT INTO test_ks.events (id, event_type, payload) VALUES ('e2', 'purchase', '{\"item\":\"laptop\"}')");
        directSession.execute(
                "INSERT INTO test_ks.events (id, event_type, payload) VALUES ('e3', 'logout', '{\"user\":\"alice\"}')");

        CassandraSourceAdapter source = new CassandraSourceAdapter();
        source.start(Map.of(
                "cassandra.contact.points", contactPoint,
                "cassandra.datacenter", "datacenter1",
                "cassandra.keyspace", "test_ks",
                "cassandra.table", "events",
                "cassandra.poll.interval.ms", "0"
        ));

        List<RawRecord> records = source.fetch(SourceOffset.empty(), 100);
        assertNotNull(records);
        assertTrue(records.size() >= 3, "Expected at least 3 records but got " + records.size());

        // Verify records contain valid JSON
        for (RawRecord record : records) {
            String json = new String(record.value(), StandardCharsets.UTF_8);
            JsonNode node = objectMapper.readTree(json);
            assertTrue(node.has("id") || node.has("event_type"),
                    "Record should contain id or event_type: " + json);
        }

        assertTrue(source.isHealthy());
        source.stop();
    }

    @Test
    void sinkUpsertUpdatesExistingRows() throws Exception {
        // Pre-insert a row
        directSession.execute(
                "INSERT INTO test_ks.users (id, name, email, score) VALUES ('u10', 'Old Name', 'old@test.com', 50)");

        CassandraSinkAdapter sink = new CassandraSinkAdapter();
        sink.start(Map.of(
                "cassandra.contact.points", contactPoint,
                "cassandra.datacenter", "datacenter1",
                "cassandra.keyspace", "test_ks",
                "cassandra.table", "users",
                "cassandra.consistency.level", "LOCAL_ONE"
        ));

        // Cassandra INSERT is an upsert by default
        sink.write(List.of(
                new TransformedRecord(null,
                        "{\"id\":\"u10\",\"name\":\"Updated Name\",\"email\":\"new@test.com\",\"score\":99}"
                                .getBytes(StandardCharsets.UTF_8),
                        Map.of(), SourceOffset.empty())
        ));
        sink.flush();

        String name = directSession.execute(
                "SELECT name FROM test_ks.users WHERE id = 'u10'")
                .one().getString("name");
        assertEquals("Updated Name", name);

        sink.stop();
    }

    @Test
    void roundTripSinkToSource() throws Exception {
        // Create a separate table for round-trip test
        directSession.execute(
                "CREATE TABLE IF NOT EXISTS test_ks.roundtrip (" +
                "id text PRIMARY KEY, data text)");

        CassandraSinkAdapter sink = new CassandraSinkAdapter();
        sink.start(Map.of(
                "cassandra.contact.points", contactPoint,
                "cassandra.datacenter", "datacenter1",
                "cassandra.keyspace", "test_ks",
                "cassandra.table", "roundtrip"
        ));

        sink.write(List.of(
                new TransformedRecord(null,
                        "{\"id\":\"rt1\",\"data\":\"enriched-by-ai\"}".getBytes(StandardCharsets.UTF_8),
                        Map.of(), SourceOffset.empty()),
                new TransformedRecord(null,
                        "{\"id\":\"rt2\",\"data\":\"also-enriched\"}".getBytes(StandardCharsets.UTF_8),
                        Map.of(), SourceOffset.empty())
        ));
        sink.flush();
        sink.stop();

        CassandraSourceAdapter source = new CassandraSourceAdapter();
        source.start(Map.of(
                "cassandra.contact.points", contactPoint,
                "cassandra.datacenter", "datacenter1",
                "cassandra.keyspace", "test_ks",
                "cassandra.table", "roundtrip",
                "cassandra.poll.interval.ms", "0"
        ));

        List<RawRecord> records = source.fetch(SourceOffset.empty(), 100);
        assertEquals(2, records.size());

        source.stop();
    }
}
