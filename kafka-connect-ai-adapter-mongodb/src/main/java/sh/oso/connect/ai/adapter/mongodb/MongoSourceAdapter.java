package sh.oso.connect.ai.adapter.mongodb;

import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.apache.kafka.common.config.ConfigDef;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.connect.ai.adapter.mongodb.config.MongoSourceConfig;
import sh.oso.connect.ai.api.adapter.SourceAdapter;
import sh.oso.connect.ai.api.config.AiConnectConfig;
import sh.oso.connect.ai.api.error.RetryableException;
import sh.oso.connect.ai.api.model.RawRecord;
import sh.oso.connect.ai.api.model.SourceOffset;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class MongoSourceAdapter implements SourceAdapter {

    private static final Logger log = LoggerFactory.getLogger(MongoSourceAdapter.class);

    private MongoClient client;
    private MongoSourceConfig config;
    private MongoChangeStreamCursor<ChangeStreamDocument<Document>> changeStreamCursor;
    private long lastPollTimestamp;

    @Override
    public String type() {
        return "mongodb";
    }

    @Override
    public ConfigDef configDef() {
        return new ConfigDef()
                .define(AiConnectConfig.MONGODB_CONNECTION_STRING, ConfigDef.Type.STRING,
                        "mongodb://localhost:27017", ConfigDef.Importance.HIGH,
                        "MongoDB connection string")
                .define(AiConnectConfig.MONGODB_DATABASE, ConfigDef.Type.STRING,
                        ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH,
                        "MongoDB database name")
                .define(AiConnectConfig.MONGODB_COLLECTION, ConfigDef.Type.STRING,
                        ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH,
                        "MongoDB collection name")
                .define(AiConnectConfig.MONGODB_POLL_MODE, ConfigDef.Type.STRING,
                        "change_stream", ConfigDef.Importance.MEDIUM,
                        "Poll mode: change_stream or polling")
                .define(AiConnectConfig.MONGODB_PIPELINE, ConfigDef.Type.STRING,
                        "[]", ConfigDef.Importance.LOW,
                        "Change stream aggregation pipeline (JSON array)")
                .define(AiConnectConfig.MONGODB_POLL_INTERVAL_MS, ConfigDef.Type.LONG,
                        1000L, ConfigDef.Importance.MEDIUM,
                        "Poll interval in milliseconds (polling mode)");
    }

    @Override
    public void start(Map<String, String> props) {
        this.config = new MongoSourceConfig(props);
        this.client = MongoClients.create(config.connectionString());
        this.lastPollTimestamp = 0;
        log.info("MongoSourceAdapter started: database={}, collection={}, mode={}",
                config.database(), config.collection(), config.pollMode());
    }

    @Override
    public List<RawRecord> fetch(SourceOffset currentOffset, int maxRecords) throws InterruptedException {
        try {
            if ("change_stream".equals(config.pollMode())) {
                return fetchChangeStream(currentOffset, maxRecords);
            } else {
                return fetchPolling(currentOffset, maxRecords);
            }
        } catch (Exception e) {
            if (e instanceof InterruptedException ie) {
                throw ie;
            }
            throw new RetryableException("Failed to fetch from MongoDB: " + e.getMessage(), e);
        }
    }

    private List<RawRecord> fetchChangeStream(SourceOffset currentOffset, int maxRecords) {
        if (changeStreamCursor == null) {
            MongoCollection<Document> collection = client
                    .getDatabase(config.database())
                    .getCollection(config.collection());

            List<Bson> pipeline = parsePipeline(config.pipeline());
            ChangeStreamIterable<Document> stream = collection.watch(pipeline);

            String resumeTokenJson = (String) currentOffset.offset().get("resumeToken");
            if (resumeTokenJson != null && !resumeTokenJson.isEmpty()) {
                BsonDocument resumeToken = BsonDocument.parse(resumeTokenJson);
                stream = stream.resumeAfter(resumeToken);
            }

            stream = stream.maxAwaitTime(config.pollIntervalMs(), TimeUnit.MILLISECONDS);
            changeStreamCursor = stream.cursor();
        }

        List<RawRecord> records = new ArrayList<>();
        int count = 0;
        ChangeStreamDocument<Document> event;
        while (count < maxRecords && (event = changeStreamCursor.tryNext()) != null) {
            Document fullDoc = event.getFullDocument();
            if (fullDoc == null) {
                continue;
            }

            byte[] value = fullDoc.toJson().getBytes(StandardCharsets.UTF_8);
            byte[] key = null;
            BsonDocument docKey = event.getDocumentKey();
            if (docKey != null) {
                key = docKey.toJson().getBytes(StandardCharsets.UTF_8);
            }

            Map<String, String> partition = Map.of(
                    "adapter", "mongodb",
                    "database", config.database(),
                    "collection", config.collection());
            Map<String, Object> offset = new HashMap<>();
            BsonDocument resumeToken = event.getResumeToken();
            if (resumeToken != null) {
                offset.put("resumeToken", resumeToken.toJson());
            }

            Map<String, String> metadata = Map.of(
                    "source.database", config.database(),
                    "source.collection", config.collection(),
                    "operation", event.getOperationType() != null ? event.getOperationType().getValue() : "unknown");

            records.add(new RawRecord(key, value, metadata, new SourceOffset(partition, offset)));
            count++;
        }

        return records;
    }

    private List<RawRecord> fetchPolling(SourceOffset currentOffset, int maxRecords) throws InterruptedException {
        long now = System.currentTimeMillis();
        long elapsed = now - lastPollTimestamp;
        if (elapsed < config.pollIntervalMs()) {
            Thread.sleep(config.pollIntervalMs() - elapsed);
        }
        lastPollTimestamp = System.currentTimeMillis();

        MongoCollection<Document> collection = client
                .getDatabase(config.database())
                .getCollection(config.collection());

        Bson filter = new Document();
        String lastId = (String) currentOffset.offset().get("lastId");
        if (lastId != null && !lastId.isEmpty()) {
            filter = Filters.gt("_id", new ObjectId(lastId));
        }

        List<RawRecord> records = new ArrayList<>();
        try (MongoCursor<Document> cursor = collection.find(filter)
                .sort(new Document("_id", 1))
                .limit(maxRecords)
                .iterator()) {

            while (cursor.hasNext()) {
                Document doc = cursor.next();
                byte[] value = doc.toJson().getBytes(StandardCharsets.UTF_8);
                ObjectId id = doc.getObjectId("_id");
                byte[] key = id != null ? id.toHexString().getBytes(StandardCharsets.UTF_8) : null;

                Map<String, String> partition = Map.of(
                        "adapter", "mongodb",
                        "database", config.database(),
                        "collection", config.collection());
                Map<String, Object> offset = new HashMap<>();
                if (id != null) {
                    offset.put("lastId", id.toHexString());
                }

                Map<String, String> metadata = Map.of(
                        "source.database", config.database(),
                        "source.collection", config.collection());

                records.add(new RawRecord(key, value, metadata, new SourceOffset(partition, offset)));
            }
        }

        log.debug("Fetched {} records from {}.{}", records.size(), config.database(), config.collection());
        return records;
    }

    private List<Bson> parsePipeline(String pipelineJson) {
        if (pipelineJson == null || pipelineJson.isBlank() || "[]".equals(pipelineJson.trim())) {
            return Collections.emptyList();
        }
        List<Bson> stages = new ArrayList<>();
        List<?> rawList = Document.parse("{\"p\":" + pipelineJson + "}").getList("p", Document.class);
        for (Object item : rawList) {
            if (item instanceof Document doc) {
                stages.add(doc);
            }
        }
        return stages;
    }

    @Override
    public void commitOffset(SourceOffset offset) {
        // Change stream tracks position via resume token
        // Polling mode tracks position via lastId in offset
    }

    @Override
    public boolean isHealthy() {
        if (client == null) {
            return false;
        }
        try {
            client.getDatabase("admin").runCommand(new Document("ping", 1));
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public void stop() {
        if (changeStreamCursor != null) {
            changeStreamCursor.close();
        }
        if (client != null) {
            client.close();
        }
        log.info("MongoSourceAdapter stopped");
    }
}
