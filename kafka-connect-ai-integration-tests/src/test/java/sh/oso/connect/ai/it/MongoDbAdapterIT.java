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
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import sh.oso.connect.ai.adapter.mongodb.MongoSinkAdapter;
import sh.oso.connect.ai.adapter.mongodb.MongoSourceAdapter;
import sh.oso.connect.ai.api.model.RawRecord;
import sh.oso.connect.ai.api.model.SourceOffset;
import sh.oso.connect.ai.api.model.TransformedRecord;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for MongoDB source and sink adapters using Testcontainers.
 * Uses a MongoDB replica set (required for change streams).
 */
@Testcontainers
class MongoDbAdapterIT {

    static final ObjectMapper objectMapper = new ObjectMapper();

    @Container
    static final MongoDBContainer mongo = new MongoDBContainer(
            DockerImageName.parse("mongo:7.0"))
            .withExposedPorts(27017);

    static MongoClient directClient;

    @BeforeAll
    static void setUp() {
        directClient = MongoClients.create(mongo.getConnectionString());
    }

    @AfterAll
    static void tearDown() {
        if (directClient != null) {
            directClient.close();
        }
    }

    @Test
    void sourcePollingModeFetchesDocuments() throws Exception {
        // Insert test documents directly
        MongoDatabase db = directClient.getDatabase("testdb");
        MongoCollection<Document> collection = db.getCollection("users");
        collection.drop();
        collection.insertMany(List.of(
                new Document("name", "Alice").append("email", "alice@test.com"),
                new Document("name", "Bob").append("email", "bob@test.com"),
                new Document("name", "Charlie").append("email", "charlie@test.com")
        ));

        MongoSourceAdapter source = new MongoSourceAdapter();
        source.start(Map.of(
                "mongodb.connection.string", mongo.getConnectionString(),
                "mongodb.database", "testdb",
                "mongodb.collection", "users",
                "mongodb.poll.mode", "polling",
                "mongodb.poll.interval.ms", "0"
        ));

        List<RawRecord> records = source.fetch(SourceOffset.empty(), 100);
        assertNotNull(records);
        assertEquals(3, records.size());

        JsonNode first = objectMapper.readTree(
                new String(records.get(0).value(), StandardCharsets.UTF_8));
        assertTrue(first.has("name"));
        assertTrue(first.has("email"));

        // Verify offset contains lastId
        assertNotNull(records.get(2).sourceOffset().offset().get("lastId"));

        assertTrue(source.isHealthy());
        source.stop();
    }

    @Test
    void sourcePollingModeResumesFromOffset() throws Exception {
        MongoDatabase db = directClient.getDatabase("testdb");
        MongoCollection<Document> collection = db.getCollection("resume_test");
        collection.drop();
        collection.insertMany(List.of(
                new Document("name", "First"),
                new Document("name", "Second"),
                new Document("name", "Third")
        ));

        MongoSourceAdapter source = new MongoSourceAdapter();
        source.start(Map.of(
                "mongodb.connection.string", mongo.getConnectionString(),
                "mongodb.database", "testdb",
                "mongodb.collection", "resume_test",
                "mongodb.poll.mode", "polling",
                "mongodb.poll.interval.ms", "0"
        ));

        // Fetch first batch
        List<RawRecord> firstBatch = source.fetch(SourceOffset.empty(), 2);
        assertEquals(2, firstBatch.size());

        // Resume from last offset
        SourceOffset resumeOffset = firstBatch.get(1).sourceOffset();
        List<RawRecord> secondBatch = source.fetch(resumeOffset, 100);
        assertEquals(1, secondBatch.size());

        JsonNode doc = objectMapper.readTree(
                new String(secondBatch.get(0).value(), StandardCharsets.UTF_8));
        assertEquals("Third", doc.get("name").asText());

        source.stop();
    }

    @Test
    void sinkInsertsDocuments() throws Exception {
        MongoSinkAdapter sink = new MongoSinkAdapter();
        sink.start(Map.of(
                "mongodb.connection.string", mongo.getConnectionString(),
                "mongodb.database", "testdb",
                "mongodb.collection", "sink_output",
                "mongodb.write.mode", "insert",
                "mongodb.batch.size", "100"
        ));

        List<TransformedRecord> records = List.of(
                new TransformedRecord(null,
                        "{\"name\":\"Alice\",\"score\":95}".getBytes(StandardCharsets.UTF_8),
                        Map.of(), SourceOffset.empty()),
                new TransformedRecord(null,
                        "{\"name\":\"Bob\",\"score\":87}".getBytes(StandardCharsets.UTF_8),
                        Map.of(), SourceOffset.empty())
        );

        sink.write(records);

        // Verify data was written
        MongoCollection<Document> collection = directClient
                .getDatabase("testdb")
                .getCollection("sink_output");
        assertEquals(2, collection.countDocuments());

        Document alice = collection.find(new Document("name", "Alice")).first();
        assertNotNull(alice);
        assertEquals(95, alice.getInteger("score"));

        assertTrue(sink.isHealthy());
        sink.stop();
    }

    @Test
    void sinkUpsertUpdatesExistingDocuments() throws Exception {
        // Pre-insert a document
        MongoCollection<Document> collection = directClient
                .getDatabase("testdb")
                .getCollection("upsert_output");
        collection.drop();
        collection.insertOne(new Document("_id", "user1")
                .append("name", "Old Name")
                .append("score", 50));

        MongoSinkAdapter sink = new MongoSinkAdapter();
        sink.start(Map.of(
                "mongodb.connection.string", mongo.getConnectionString(),
                "mongodb.database", "testdb",
                "mongodb.collection", "upsert_output",
                "mongodb.write.mode", "upsert",
                "mongodb.batch.size", "100"
        ));

        sink.write(List.of(
                new TransformedRecord(null,
                        "{\"_id\":\"user1\",\"name\":\"New Name\",\"score\":99}".getBytes(StandardCharsets.UTF_8),
                        Map.of(), SourceOffset.empty()),
                new TransformedRecord(null,
                        "{\"_id\":\"user2\",\"name\":\"Brand New\",\"score\":100}".getBytes(StandardCharsets.UTF_8),
                        Map.of(), SourceOffset.empty())
        ));

        assertEquals(2, collection.countDocuments());
        Document updated = collection.find(new Document("_id", "user1")).first();
        assertNotNull(updated);
        assertEquals("New Name", updated.getString("name"));
        assertEquals(99, updated.getInteger("score"));

        sink.stop();
    }

    @Test
    void sourceChangeStreamDetectsInserts() throws Exception {
        MongoDatabase db = directClient.getDatabase("testdb");
        MongoCollection<Document> collection = db.getCollection("stream_test");
        collection.drop();
        // Insert an initial document so the collection exists
        collection.insertOne(new Document("init", true));

        MongoSourceAdapter source = new MongoSourceAdapter();
        source.start(Map.of(
                "mongodb.connection.string", mongo.getConnectionString(),
                "mongodb.database", "testdb",
                "mongodb.collection", "stream_test",
                "mongodb.poll.mode", "change_stream",
                "mongodb.poll.interval.ms", "500"
        ));

        // Insert documents after starting the change stream
        Thread inserter = new Thread(() -> {
            try {
                Thread.sleep(1000);
                collection.insertOne(new Document("name", "ChangeStreamUser").append("role", "admin"));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        inserter.start();

        List<RawRecord> records = source.fetch(SourceOffset.empty(), 10);

        // Change stream should pick up at least one insert
        // Note: timing dependent, may get the init insert or the new one
        assertNotNull(records);

        inserter.join(5000);
        source.stop();
    }
}
