package sh.oso.connect.ai.connect.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.search.*;
import redis.clients.jedis.search.schemafields.SchemaField;
import redis.clients.jedis.search.schemafields.TextField;
import redis.clients.jedis.search.schemafields.VectorField;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

public class SemanticCache {

    private static final Logger log = LoggerFactory.getLogger(SemanticCache.class);
    private static final String INDEX_NAME = "connect_ai_cache_idx";
    private static final String KEY_PREFIX = "connect.ai:cache:";
    private static final int VECTOR_DIM = 1536;

    private final JedisPooled jedis;
    private final EmbeddingClient embeddingClient;
    private final double similarityThreshold;
    private final int ttlSeconds;

    public SemanticCache(String redisUrl, EmbeddingClient embeddingClient,
                         double similarityThreshold, int ttlSeconds) {
        this.jedis = new JedisPooled(redisUrl);
        this.embeddingClient = embeddingClient;
        this.similarityThreshold = similarityThreshold;
        this.ttlSeconds = ttlSeconds;
        ensureIndex();
    }

    // Visible for testing
    SemanticCache(JedisPooled jedis, EmbeddingClient embeddingClient,
                  double similarityThreshold, int ttlSeconds) {
        this.jedis = jedis;
        this.embeddingClient = embeddingClient;
        this.similarityThreshold = similarityThreshold;
        this.ttlSeconds = ttlSeconds;
        ensureIndex();
    }

    private void ensureIndex() {
        try {
            jedis.ftInfo(INDEX_NAME);
            log.debug("Redis search index {} already exists", INDEX_NAME);
        } catch (Exception e) {
            try {
                Map<String, Object> vectorAttrs = new HashMap<>();
                vectorAttrs.put("TYPE", "FLOAT32");
                vectorAttrs.put("DIM", VECTOR_DIM);
                vectorAttrs.put("DISTANCE_METRIC", "COSINE");

                SchemaField[] schema = new SchemaField[]{
                        TextField.of("$.output").as("output"),
                        TextField.of("$.created_at").as("created_at"),
                        VectorField.builder()
                                .fieldName("$.embedding")
                                .algorithm(VectorField.VectorAlgorithm.FLAT)
                                .addAttribute("TYPE", "FLOAT32")
                                .addAttribute("DIM", VECTOR_DIM)
                                .addAttribute("DISTANCE_METRIC", "COSINE")
                                .as("embedding")
                                .build()
                };

                jedis.ftCreate(INDEX_NAME,
                        FTCreateParams.createParams()
                                .on(IndexDataType.JSON)
                                .addPrefix(KEY_PREFIX),
                        schema);
                log.info("Created Redis search index {}", INDEX_NAME);
            } catch (Exception ex) {
                log.warn("Failed to create Redis search index: {}", ex.getMessage());
            }
        }
    }

    public Optional<CacheResult> lookup(String recordBatchJson) {
        try {
            float[] embedding = embeddingClient.embed(recordBatchJson);
            byte[] queryVector = floatArrayToBytes(embedding);

            Query query = new Query("*=>[KNN 1 @embedding $BLOB AS score]")
                    .addParam("BLOB", queryVector)
                    .returnFields("output", "created_at", "score")
                    .limit(0, 1)
                    .dialect(2);

            SearchResult result = jedis.ftSearch(INDEX_NAME, query);

            if (result.getTotalResults() == 0) {
                return Optional.empty();
            }

            Document doc = result.getDocuments().get(0);
            double distance = Double.parseDouble(doc.getString("score"));
            double similarity = 1.0 - distance;

            if (similarity < similarityThreshold) {
                return Optional.empty();
            }

            String output = doc.getString("output");
            String createdAt = doc.getString("created_at");
            Duration age = Duration.between(Instant.parse(createdAt), Instant.now());

            return Optional.of(new CacheResult(output, similarity, age));
        } catch (Exception e) {
            log.warn("Cache lookup failed: {}", e.getMessage());
            return Optional.empty();
        }
    }

    public void store(String recordBatchJson, String llmOutput) {
        try {
            float[] embedding = embeddingClient.embed(recordBatchJson);
            String key = KEY_PREFIX + UUID.randomUUID();

            Map<String, Object> doc = new HashMap<>();
            doc.put("output", llmOutput);
            doc.put("created_at", Instant.now().toString());
            doc.put("embedding", floatArrayToList(embedding));

            com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
            jedis.jsonSet(key, mapper.writeValueAsString(doc));
            jedis.expire(key, ttlSeconds);
        } catch (Exception e) {
            log.warn("Cache store failed: {}", e.getMessage());
        }
    }

    public void close() {
        jedis.close();
    }

    private static byte[] floatArrayToBytes(float[] array) {
        ByteBuffer buffer = ByteBuffer.allocate(array.length * 4).order(ByteOrder.LITTLE_ENDIAN);
        for (float f : array) {
            buffer.putFloat(f);
        }
        return buffer.array();
    }

    private static List<Double> floatArrayToList(float[] array) {
        List<Double> list = new ArrayList<>(array.length);
        for (float f : array) {
            list.add((double) f);
        }
        return list;
    }
}
