package sh.oso.connect.ai.adapter.mongodb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.WriteModel;
import org.apache.kafka.common.config.ConfigDef;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.connect.ai.adapter.mongodb.config.MongoSinkConfig;
import sh.oso.connect.ai.api.adapter.SinkAdapter;
import sh.oso.connect.ai.api.config.AiConnectConfig;
import sh.oso.connect.ai.api.error.RetryableException;
import sh.oso.connect.ai.api.model.TransformedRecord;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MongoSinkAdapter implements SinkAdapter {

    private static final Logger log = LoggerFactory.getLogger(MongoSinkAdapter.class);

    private MongoClient client;
    private MongoSinkConfig config;

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
                .define(AiConnectConfig.MONGODB_WRITE_MODE, ConfigDef.Type.STRING,
                        "insert", ConfigDef.Importance.MEDIUM,
                        "Write mode: insert or upsert")
                .define(AiConnectConfig.MONGODB_BATCH_SIZE, ConfigDef.Type.INT,
                        1000, ConfigDef.Importance.MEDIUM,
                        "Batch write size");
    }

    @Override
    public void start(Map<String, String> props) {
        this.config = new MongoSinkConfig(props);
        this.client = MongoClients.create(config.connectionString());
        log.info("MongoSinkAdapter started: database={}, collection={}, mode={}",
                config.database(), config.collection(), config.writeMode());
    }

    @Override
    public void write(List<TransformedRecord> records) {
        if (records.isEmpty()) {
            return;
        }

        try {
            MongoCollection<Document> collection = client
                    .getDatabase(config.database())
                    .getCollection(config.collection());

            List<WriteModel<Document>> operations = new ArrayList<>();
            for (TransformedRecord record : records) {
                String json = new String(record.value(), StandardCharsets.UTF_8);
                Document doc = Document.parse(json);

                if ("upsert".equals(config.writeMode()) && doc.containsKey("_id")) {
                    Document filter = new Document("_id", doc.get("_id"));
                    operations.add(new ReplaceOneModel<>(filter, doc,
                            new ReplaceOptions().upsert(true)));
                } else {
                    operations.add(new InsertOneModel<>(doc));
                }

                if (operations.size() >= config.batchSize()) {
                    collection.bulkWrite(operations);
                    operations.clear();
                }
            }

            if (!operations.isEmpty()) {
                collection.bulkWrite(operations);
            }

            log.debug("Wrote {} records to {}.{}", records.size(),
                    config.database(), config.collection());
        } catch (Exception e) {
            throw new RetryableException("Failed to write to MongoDB: " + e.getMessage(), e);
        }
    }

    @Override
    public void flush() {
        // Writes are committed within write()
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
        if (client != null) {
            client.close();
        }
        log.info("MongoSinkAdapter stopped");
    }
}
