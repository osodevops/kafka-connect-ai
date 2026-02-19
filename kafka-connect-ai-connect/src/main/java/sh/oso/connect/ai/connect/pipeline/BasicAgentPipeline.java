package sh.oso.connect.ai.connect.pipeline;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.connect.ai.api.config.AiConnectConfig;
import sh.oso.connect.ai.api.error.NonRetryableException;
import sh.oso.connect.ai.api.model.RawRecord;
import sh.oso.connect.ai.api.model.TransformedRecord;
import sh.oso.connect.ai.api.pipeline.AgentPipeline;
import sh.oso.connect.ai.connect.cache.EmbeddingClient;
import sh.oso.connect.ai.connect.cache.CacheResult;
import sh.oso.connect.ai.connect.cache.SemanticCache;
import sh.oso.connect.ai.connect.llm.LlmClient;
import sh.oso.connect.ai.connect.llm.LlmClientFactory;
import sh.oso.connect.ai.connect.llm.LlmRequest;
import sh.oso.connect.ai.connect.llm.LlmResponse;
import sh.oso.connect.ai.connect.metrics.LogContext;
import sh.oso.connect.ai.connect.metrics.AiConnectMetrics;

import java.nio.charset.StandardCharsets;
import java.util.*;

public class BasicAgentPipeline implements AgentPipeline {

    private static final Logger log = LoggerFactory.getLogger(BasicAgentPipeline.class);
    private static final int DEFAULT_MAX_RETRIES = 3;
    private static final long DEFAULT_RETRY_BACKOFF_MS = 1000;

    private final ObjectMapper objectMapper = new ObjectMapper();

    private LlmClient llmClient;
    private SchemaEnforcer schemaEnforcer;
    private String systemPrompt;
    private String model;
    private boolean enablePromptCaching;
    private double temperature;
    private int maxRetries;
    private long retryBackoffMs;
    private int maxTokens;
    private String targetSchema;
    private boolean structuredOutputEnabled;

    // Phase 3 features
    private AiConnectMetrics metrics;
    private ModelRouter modelRouter;
    private DeterministicTransformer deterministicTransformer;
    private ParallelLlmExecutor parallelExecutor;
    private SemanticCache semanticCache;
    private boolean routerEnabled;
    private boolean cacheEnabled;
    private int parallelCalls;

    @Override
    public void configure(Map<String, String> props) {
        String provider = props.getOrDefault(AiConnectConfig.LLM_PROVIDER, "anthropic");
        String apiKey = props.getOrDefault(AiConnectConfig.LLM_API_KEY, "");
        String baseUrl = props.getOrDefault(AiConnectConfig.LLM_BASE_URL,
                props.getOrDefault(AiConnectConfig.LLM_ENDPOINT, ""));
        this.model = props.getOrDefault(AiConnectConfig.LLM_MODEL, "claude-sonnet-4-20250514");
        this.systemPrompt = props.getOrDefault(AiConnectConfig.AGENT_SYSTEM_PROMPT, "");
        this.enablePromptCaching = Boolean.parseBoolean(
                props.getOrDefault(AiConnectConfig.AGENT_ENABLE_PROMPT_CACHING, "true"));
        this.temperature = Double.parseDouble(
                props.getOrDefault(AiConnectConfig.LLM_TEMPERATURE, "0.0"));
        this.maxRetries = Integer.parseInt(
                props.getOrDefault(AiConnectConfig.AGENT_MAX_RETRIES, String.valueOf(DEFAULT_MAX_RETRIES)));
        this.retryBackoffMs = Long.parseLong(
                props.getOrDefault(AiConnectConfig.RETRY_BACKOFF_MS, String.valueOf(DEFAULT_RETRY_BACKOFF_MS)));
        this.maxTokens = Integer.parseInt(
                props.getOrDefault(AiConnectConfig.LLM_MAX_TOKENS, "4096"));
        this.structuredOutputEnabled = Boolean.parseBoolean(
                props.getOrDefault(AiConnectConfig.STRUCTURED_OUTPUT, "true"));

        this.llmClient = LlmClientFactory.create(provider, apiKey, baseUrl);

        this.targetSchema = props.getOrDefault(AiConnectConfig.AGENT_TARGET_SCHEMA, "");
        this.schemaEnforcer = new SchemaEnforcer(targetSchema);

        this.metrics = AiConnectMetrics.getInstance();

        // Parallel execution
        this.parallelCalls = Integer.parseInt(
                props.getOrDefault(AiConnectConfig.BATCH_PARALLEL_CALLS, "1"));
        int callTimeout = Integer.parseInt(
                props.getOrDefault(AiConnectConfig.CALL_TIMEOUT_SECONDS, "30"));
        if (parallelCalls > 1) {
            this.parallelExecutor = new ParallelLlmExecutor(parallelCalls, callTimeout);
        }

        // Model router
        this.routerEnabled = Boolean.parseBoolean(
                props.getOrDefault(AiConnectConfig.ROUTER_ENABLED, "false"));
        if (routerEnabled) {
            String fastModel = props.getOrDefault(AiConnectConfig.LLM_MODEL_FAST, "claude-haiku-4-5-20251001");
            String defaultModel = props.getOrDefault(AiConnectConfig.LLM_MODEL_DEFAULT, model);
            String powerfulModel = props.getOrDefault(AiConnectConfig.LLM_MODEL_POWERFUL, "claude-opus-4-6");
            String patternsStr = props.getOrDefault(AiConnectConfig.ROUTER_DETERMINISTIC_PATTERNS, "");
            Set<String> patterns = patternsStr.isEmpty() ? Set.of()
                    : new LinkedHashSet<>(Arrays.asList(patternsStr.split(",")));
            this.modelRouter = new ModelRouter(fastModel, defaultModel, powerfulModel, patterns);
            this.deterministicTransformer = new DeterministicTransformer(patterns);
        }

        // Semantic cache
        this.cacheEnabled = Boolean.parseBoolean(
                props.getOrDefault(AiConnectConfig.CACHE_ENABLED, "false"));
        if (cacheEnabled) {
            String redisUrl = props.getOrDefault(AiConnectConfig.CACHE_REDIS_URL, "redis://localhost:6379");
            double threshold = Double.parseDouble(
                    props.getOrDefault(AiConnectConfig.CACHE_SIMILARITY_THRESHOLD, "0.95"));
            int ttl = Integer.parseInt(
                    props.getOrDefault(AiConnectConfig.CACHE_TTL_SECONDS, "3600"));
            String embeddingModel = props.getOrDefault(AiConnectConfig.CACHE_EMBEDDING_MODEL, "text-embedding-3-small");
            EmbeddingClient embeddingClient = new EmbeddingClient(apiKey, embeddingModel);
            this.semanticCache = new SemanticCache(redisUrl, embeddingClient, threshold, ttl);
        }
    }

    // Visible for testing
    BasicAgentPipeline(LlmClient llmClient, SchemaEnforcer schemaEnforcer,
                       String systemPrompt, String model, boolean enablePromptCaching, double temperature) {
        this.llmClient = llmClient;
        this.schemaEnforcer = schemaEnforcer;
        this.systemPrompt = systemPrompt;
        this.model = model;
        this.enablePromptCaching = enablePromptCaching;
        this.temperature = temperature;
        this.maxRetries = DEFAULT_MAX_RETRIES;
        this.retryBackoffMs = DEFAULT_RETRY_BACKOFF_MS;
        this.maxTokens = 4096;
        this.targetSchema = schemaEnforcer.hasSchema() ? schemaEnforcer.getSchemaJson() : "";
        this.structuredOutputEnabled = true;
        this.routerEnabled = false;
        this.cacheEnabled = false;
        this.parallelCalls = 1;
    }

    public BasicAgentPipeline() {}

    // Visible for testing — set optional components
    void setMetrics(AiConnectMetrics metrics) {
        this.metrics = metrics;
    }

    void setModelRouter(ModelRouter modelRouter) {
        this.modelRouter = modelRouter;
        this.routerEnabled = modelRouter != null;
    }

    void setDeterministicTransformer(DeterministicTransformer deterministicTransformer) {
        this.deterministicTransformer = deterministicTransformer;
    }

    void setSemanticCache(SemanticCache semanticCache) {
        this.semanticCache = semanticCache;
        this.cacheEnabled = semanticCache != null;
    }

    void setParallelExecutor(ParallelLlmExecutor parallelExecutor) {
        this.parallelExecutor = parallelExecutor;
        this.parallelCalls = parallelExecutor != null ? parallelExecutor.getMaxConcurrent() : 1;
    }

    @Override
    public List<TransformedRecord> process(List<RawRecord> records) {
        if (records.isEmpty()) {
            return List.of();
        }

        String batchJson = serializeBatch(records);

        // Step 1: Semantic cache lookup (before routing, per PRD)
        if (cacheEnabled && semanticCache != null) {
            Optional<CacheResult> cached = semanticCache.lookup(batchJson);
            if (cached.isPresent()) {
                if (metrics != null) {
                    metrics.recordCacheHit();
                }
                LogContext.withBatch(records.size(), true, -1, 0, 0, 0);
                log.debug("Cache hit (similarity={}) for batch of {} records",
                        cached.get().similarity(), records.size());
                LogContext.clearBatch();
                return parseTransformedRecords(cached.get().cachedOutput(), records);
            }
            if (metrics != null) {
                metrics.recordCacheMiss();
            }
        }

        // Step 2: Model routing
        String effectiveModel = this.model;
        int routerTier = 2;
        if (routerEnabled && modelRouter != null) {
            ModelRouter.RoutingDecision decision = modelRouter.route(records);
            routerTier = decision.tier();
            if (metrics != null) {
                metrics.recordRouterTier(routerTier);
            }

            // Tier 0: deterministic transform, skip LLM
            if (routerTier == 0 && deterministicTransformer != null) {
                Optional<List<TransformedRecord>> deterministicResult = deterministicTransformer.transform(records);
                if (deterministicResult.isPresent()) {
                    log.debug("Tier 0 deterministic transform applied for {} records", records.size());
                    if (metrics != null) {
                        metrics.recordProcessed(deterministicResult.get().size());
                    }
                    return deterministicResult.get();
                }
                // Fall through to LLM if deterministic fails
            }

            if (decision.modelName() != null) {
                effectiveModel = decision.modelName();
            }
            log.debug("Router: tier={}, model={}, reason={}", routerTier, effectiveModel, decision.reason());
        }

        // Step 3: LLM call (possibly parallel)
        long startTime = System.currentTimeMillis();
        String llmOutput;
        final String modelForCall = effectiveModel;
        final int finalRouterTier = routerTier;

        if (parallelExecutor != null && parallelCalls > 1 && records.size() > parallelCalls) {
            int subBatchSize = Math.max(1, records.size() / parallelCalls);
            List<List<RawRecord>> subBatches = new ArrayList<>();
            for (int i = 0; i < records.size(); i += subBatchSize) {
                int end = Math.min(i + subBatchSize, records.size());
                subBatches.add(records.subList(i, end));
            }

            List<TransformedRecord> results = parallelExecutor.execute(subBatches,
                    subBatch -> {
                        String subJson = serializeBatch(subBatch);
                        String output = callLlmWithRetry(subJson, modelForCall);
                        return parseTransformedRecords(output, subBatch);
                    });

            long latency = System.currentTimeMillis() - startTime;
            if (metrics != null) {
                metrics.recordLlmCall();
                metrics.recordLlmLatency(latency);
                metrics.recordProcessed(results.size());
            }
            return results;
        }

        llmOutput = callLlmWithRetry(batchJson, modelForCall);
        long latency = System.currentTimeMillis() - startTime;

        if (metrics != null) {
            metrics.recordLlmLatency(latency);
        }

        // Cache store
        if (cacheEnabled && semanticCache != null) {
            semanticCache.store(batchJson, llmOutput);
        }

        LogContext.withBatch(records.size(), false, finalRouterTier, latency, 0, 0);
        LogContext.clearBatch();

        List<TransformedRecord> result = parseTransformedRecords(llmOutput, records);
        if (metrics != null) {
            metrics.recordProcessed(result.size());
        }

        // Schema drift detection: warn if LLM output record count differs from input
        if (result.size() != records.size()) {
            log.warn("Schema drift detected: input {} records, output {} records", records.size(), result.size());
        }

        return result;
    }

    private String buildEffectiveSystemPrompt() {
        StringBuilder prompt = new StringBuilder();
        if (!systemPrompt.isEmpty()) {
            prompt.append(systemPrompt);
        }
        if (schemaEnforcer.hasSchema()) {
            if (!prompt.isEmpty()) {
                prompt.append("\n\n");
            }
            prompt.append("You MUST produce output that conforms to the following JSON Schema:\n");
            prompt.append(targetSchema);
            prompt.append("\n\nRespond with ONLY a JSON array where each object matches the schema above. ");
            prompt.append("No additional text, explanation, or markdown formatting.");
        }
        return prompt.toString();
    }

    String serializeBatch(List<RawRecord> records) {
        try {
            ArrayNode array = objectMapper.createArrayNode();
            for (RawRecord record : records) {
                var node = objectMapper.createObjectNode();
                if (record.key() != null) {
                    node.put("key", new String(record.key(), StandardCharsets.UTF_8));
                }
                if (record.value() != null) {
                    node.put("value", new String(record.value(), StandardCharsets.UTF_8));
                }
                if (!record.metadata().isEmpty()) {
                    var metaNode = node.putObject("metadata");
                    record.metadata().forEach(metaNode::put);
                }
                array.add(node);
            }
            return objectMapper.writeValueAsString(array);
        } catch (JsonProcessingException e) {
            throw new NonRetryableException("Failed to serialize batch to JSON", e);
        }
    }

    private String callLlmWithRetry(String batchJson) {
        return callLlmWithRetry(batchJson, this.model);
    }

    private String callLlmWithRetry(String batchJson, String modelToUse) {
        String effectiveSystemPrompt = buildEffectiveSystemPrompt();
        String userPrompt = "Transform the following records:\n" + batchJson;

        Optional<String> schema = (structuredOutputEnabled && schemaEnforcer.hasSchema())
                ? Optional.of(targetSchema) : Optional.empty();

        LlmRequest request = LlmRequest.builder()
                .systemPrompt(effectiveSystemPrompt)
                .userPrompt(userPrompt)
                .model(modelToUse)
                .jsonSchema(schema.orElse(null))
                .enablePromptCaching(enablePromptCaching)
                .temperature(temperature)
                .maxTokens(maxTokens)
                .build();

        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            if (metrics != null) {
                metrics.recordLlmCall();
            }

            LlmResponse response = llmClient.call(request);

            if (metrics != null) {
                metrics.recordLlmTokens(response.inputTokens(), response.outputTokens());
            }

            log.debug("LLM response (attempt {}): tokens in={}, out={}, cache_create={}, cache_read={}, stop={}",
                    attempt, response.inputTokens(), response.outputTokens(),
                    response.cacheCreationInputTokens(), response.cacheReadInputTokens(),
                    response.stopReason());

            String content = response.content().trim();

            if (isValidJsonArray(content)) {
                return content;
            }

            log.warn("LLM returned malformed output (attempt {}). Retrying with correction prompt.", attempt);
            if (attempt < maxRetries) {
                try {
                    long backoff = retryBackoffMs * (long) Math.pow(2, attempt);
                    Thread.sleep(backoff);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new NonRetryableException("Interrupted during retry backoff", e);
                }
            }
            request = LlmRequest.builder()
                    .systemPrompt(effectiveSystemPrompt)
                    .userPrompt("Your previous response was not valid JSON array. " +
                            "Please respond with ONLY a JSON array of transformed records. Input:\n" + batchJson)
                    .model(modelToUse)
                    .jsonSchema(schema.orElse(null))
                    .enablePromptCaching(enablePromptCaching)
                    .temperature(temperature)
                    .maxTokens(maxTokens)
                    .build();
        }

        throw new NonRetryableException("LLM failed to produce valid JSON after retries");
    }

    private boolean isValidJsonArray(String json) {
        try {
            JsonNode node = objectMapper.readTree(json);
            return node.isArray();
        } catch (JsonProcessingException e) {
            return false;
        }
    }

    List<TransformedRecord> parseTransformedRecords(String llmOutput, List<RawRecord> originalRecords) {
        try {
            JsonNode array = objectMapper.readTree(llmOutput);
            List<TransformedRecord> results = new ArrayList<>();

            for (int i = 0; i < array.size(); i++) {
                JsonNode node = array.get(i);
                String nodeJson = objectMapper.writeValueAsString(node);

                List<String> errors = schemaEnforcer.validateWithErrors(nodeJson);
                if (!errors.isEmpty()) {
                    log.warn("Record {} failed schema validation: {}", i, errors);
                    if (metrics != null) {
                        metrics.recordFailed(1);
                    }
                    continue;
                }

                byte[] key = node.has("key")
                        ? node.get("key").asText().getBytes(StandardCharsets.UTF_8)
                        : (i < originalRecords.size() ? originalRecords.get(i).key() : null);

                byte[] value = nodeJson.getBytes(StandardCharsets.UTF_8);

                var sourceOffset = i < originalRecords.size()
                        ? originalRecords.get(i).sourceOffset()
                        : sh.oso.connect.ai.api.model.SourceOffset.empty();

                results.add(new TransformedRecord(key, value, Map.of(), sourceOffset));
            }

            return results;
        } catch (JsonProcessingException e) {
            throw new NonRetryableException("Failed to parse LLM output as JSON array", e);
        }
    }
}
