package sh.oso.connect.ai.connect.pipeline;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.connect.ai.api.config.AiConnectConfig;
import sh.oso.connect.ai.api.error.NonRetryableException;
import sh.oso.connect.ai.api.error.RetryableException;
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

import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Pattern;

public class BasicAgentPipeline implements AgentPipeline, Closeable {

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

    // Phase 4: Compiled transforms (compile-once-execute-forever)
    private boolean compiledTransformEnabled;
    private CompiledTransformCache compiledTransformCache;
    private TransformCompiler transformCompiler;
    private GraalJsTransformExecutor jsExecutor;

    // Phase 5: Circuit breaker on LLM calls
    private CircuitBreaker llmCircuitBreaker;

    // Phase 6: Rate limiter, PII masking
    private RateLimiter llmRateLimiter;
    private PiiMasker piiMasker;

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

        // Compiled transforms (compile-once-execute-forever)
        this.compiledTransformEnabled = Boolean.parseBoolean(
                props.getOrDefault(AiConnectConfig.COMPILED_TRANSFORM_ENABLED, "false"));
        if (compiledTransformEnabled) {
            long compiledTtl = Long.parseLong(
                    props.getOrDefault(AiConnectConfig.COMPILED_TRANSFORM_TTL_SECONDS, "86400"));
            long jsTimeoutMs = Long.parseLong(
                    props.getOrDefault(AiConnectConfig.COMPILED_TRANSFORM_JS_TIMEOUT_MS, "100"));
            int jsHeapMb = Integer.parseInt(
                    props.getOrDefault(AiConnectConfig.COMPILED_TRANSFORM_JS_HEAP_MB, "10"));
            int validationSamples = Integer.parseInt(
                    props.getOrDefault(AiConnectConfig.COMPILED_TRANSFORM_VALIDATION_SAMPLES, "3"));

            this.compiledTransformCache = new CompiledTransformCache(compiledTtl);
            if (this.metrics != null) {
                this.metrics.registerCompiledTransformCacheSize(this.compiledTransformCache::size);
            }
            this.jsExecutor = new GraalJsTransformExecutor(jsTimeoutMs, jsHeapMb);
            this.transformCompiler = new TransformCompiler(
                    this.llmClient, this.model, this.enablePromptCaching,
                    validationSamples, this.jsExecutor, this.schemaEnforcer, this.targetSchema);
            log.info("Compiled transforms enabled: ttl={}s, jsTimeout={}ms, jsHeap={}MB",
                    compiledTtl, jsTimeoutMs, jsHeapMb);
        }

        // Circuit breaker on LLM calls
        boolean cbEnabled = Boolean.parseBoolean(
                props.getOrDefault(AiConnectConfig.CIRCUIT_BREAKER_ENABLED, "false"));
        if (cbEnabled) {
            float failureRate = Float.parseFloat(
                    props.getOrDefault(AiConnectConfig.CIRCUIT_BREAKER_FAILURE_RATE_THRESHOLD, "50"));
            long waitDurationMs = Long.parseLong(
                    props.getOrDefault(AiConnectConfig.CIRCUIT_BREAKER_WAIT_DURATION_MS, "60000"));
            int slidingWindowSize = Integer.parseInt(
                    props.getOrDefault(AiConnectConfig.CIRCUIT_BREAKER_SLIDING_WINDOW_SIZE, "10"));

            CircuitBreakerConfig cbConfig = CircuitBreakerConfig.custom()
                    .failureRateThreshold(failureRate)
                    .waitDurationInOpenState(Duration.ofMillis(waitDurationMs))
                    .slidingWindowSize(slidingWindowSize)
                    .build();

            this.llmCircuitBreaker = CircuitBreaker.of("llm-pipeline", cbConfig);
            this.llmCircuitBreaker.getEventPublisher()
                    .onStateTransition(event ->
                            log.warn("LLM circuit breaker state transition: {}", event.getStateTransition()));
            log.info("LLM circuit breaker enabled: failureRate={}%, waitDuration={}ms, window={}",
                    failureRate, waitDurationMs, slidingWindowSize);
        }

        // LLM rate limiting
        double rateLimit = Double.parseDouble(
                props.getOrDefault(AiConnectConfig.LLM_RATE_LIMIT, "0"));
        if (rateLimit > 0) {
            this.llmRateLimiter = new RateLimiter(rateLimit);
            log.info("LLM rate limiter enabled: {} requests/sec", rateLimit);
        }

        // PII masking
        String maskFieldsStr = props.getOrDefault(AiConnectConfig.PRIVACY_MASK_FIELDS, "");
        String maskPatternsStr = props.getOrDefault(AiConnectConfig.PRIVACY_MASK_PATTERNS, "");
        String maskReplacement = props.getOrDefault(AiConnectConfig.PRIVACY_MASK_REPLACEMENT, "[MASKED]");
        boolean unmaskOutput = Boolean.parseBoolean(
                props.getOrDefault(AiConnectConfig.PRIVACY_UNMASK_OUTPUT, "true"));

        Set<String> maskFields = maskFieldsStr.isEmpty() ? Set.of()
                : new LinkedHashSet<>(Arrays.asList(maskFieldsStr.split(",")));
        List<Pattern> maskPatterns = new ArrayList<>();
        if (!maskPatternsStr.isEmpty()) {
            for (String p : maskPatternsStr.split(",")) {
                maskPatterns.add(Pattern.compile(p.trim()));
            }
        }
        if (!maskFields.isEmpty() || !maskPatterns.isEmpty()) {
            this.piiMasker = new PiiMasker(maskFields, maskPatterns, maskReplacement, unmaskOutput);
            log.info("PII masking enabled: fields={}, patterns={}", maskFields.size(), maskPatterns.size());
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
        this.compiledTransformEnabled = false;
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

    void setCompiledTransformEnabled(boolean enabled) {
        this.compiledTransformEnabled = enabled;
    }

    void setCompiledTransformCache(CompiledTransformCache cache) {
        this.compiledTransformCache = cache;
    }

    void setTransformCompiler(TransformCompiler compiler) {
        this.transformCompiler = compiler;
    }

    void setJsExecutor(GraalJsTransformExecutor executor) {
        this.jsExecutor = executor;
    }

    void setLlmCircuitBreaker(CircuitBreaker circuitBreaker) {
        this.llmCircuitBreaker = circuitBreaker;
    }

    void setLlmRateLimiter(RateLimiter rateLimiter) {
        this.llmRateLimiter = rateLimiter;
    }

    void setPiiMasker(PiiMasker piiMasker) {
        this.piiMasker = piiMasker;
    }

    @Override
    public void close() {
        if (jsExecutor != null) {
            try {
                jsExecutor.close();
            } catch (Exception e) {
                log.warn("Error closing JS executor: {}", e.getMessage());
            }
        }
        if (semanticCache != null) {
            try {
                semanticCache.close();
            } catch (Exception e) {
                log.warn("Error closing semantic cache: {}", e.getMessage());
            }
        }
    }

    @Override
    public List<TransformedRecord> process(List<RawRecord> records) {
        if (records.isEmpty()) {
            return List.of();
        }

        // Step 0: Compiled transform lookup (fastest path — no LLM, no network)
        if (compiledTransformEnabled && compiledTransformCache != null) {
            Optional<List<TransformedRecord>> compiledResult = tryCompiledTransform(records);
            if (compiledResult.isPresent()) {
                if (metrics != null) {
                    metrics.recordProcessed(compiledResult.get().size());
                }
                return compiledResult.get();
            }
        }

        String batchJson = serializeBatch(records);

        // Step 0b: PII masking (before cache lookup — PII never hits Redis)
        PiiMasker.MaskResult maskResult = null;
        if (piiMasker != null && piiMasker.isEnabled()) {
            maskResult = piiMasker.mask(batchJson);
            batchJson = maskResult.maskedJson();
        }

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
                        PiiMasker.MaskResult subMask = null;
                        if (piiMasker != null && piiMasker.isEnabled()) {
                            subMask = piiMasker.mask(subJson);
                            subJson = subMask.maskedJson();
                        }
                        String output = callLlmWithRetry(subJson, modelForCall, subBatch.size());
                        if (subMask != null) {
                            output = piiMasker.unmask(output, subMask.maskMap());
                        }
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

        llmOutput = callLlmWithRetry(batchJson, modelForCall, records.size());
        long latency = System.currentTimeMillis() - startTime;

        // Unmask PII in LLM output
        if (maskResult != null) {
            llmOutput = piiMasker.unmask(llmOutput, maskResult.maskMap());
        }

        if (metrics != null) {
            metrics.recordLlmLatency(latency);
        }

        // Cache store
        if (cacheEnabled && semanticCache != null) {
            semanticCache.store(batchJson, llmOutput);
        }

        // Trigger async compilation for future batches with the same schema
        if (compiledTransformEnabled && transformCompiler != null && compiledTransformCache != null) {
            triggerCompilation(records);
        }

        LogContext.withBatch(records.size(), false, finalRouterTier, latency, 0, 0);
        LogContext.clearBatch();

        List<TransformedRecord> result = parseTransformedRecords(llmOutput, records);
        if (metrics != null) {
            metrics.recordProcessed(result.size());
        }

        // Schema drift detection: fall back to individual processing if count mismatch
        if (result.size() != records.size()) {
            log.warn("Schema drift detected: input {} records, output {} records. "
                    + "Falling back to individual record processing.", records.size(), result.size());
            result = processIndividually(records, modelForCall);
            if (metrics != null) {
                metrics.recordProcessed(result.size());
            }
        }

        return result;
    }

    /**
     * Try to execute records through a compiled transform (Tier 0 or Tier 1).
     * Returns empty if no compiled transform exists for this schema.
     */
    private Optional<List<TransformedRecord>> tryCompiledTransform(List<RawRecord> records) {
        // Fingerprint the first record as representative of the batch
        RawRecord sample = records.get(0);
        if (sample.value() == null) {
            return Optional.empty();
        }

        String fingerprint = SchemaFingerprinter.fingerprint(sample.value(), targetSchema);
        Optional<CompiledTransform> cached = compiledTransformCache.get(fingerprint);

        if (cached.isEmpty()) {
            if (metrics != null) {
                metrics.recordCompiledTransformMiss();
            }
            return Optional.empty();
        }

        if (metrics != null) {
            metrics.recordCompiledTransformHit();
        }

        CompiledTransform transform = cached.get();
        try {
            List<TransformedRecord> results = new ArrayList<>();
            JsonNode mappingSpec = null;

            if (transform.type() == CompiledTransform.Type.DECLARATIVE) {
                mappingSpec = DeclarativeMappingExecutor.parseMappingSpec(transform.code());
            }

            for (RawRecord record : records) {
                if (record.value() == null) {
                    results.add(new TransformedRecord(record.key(), record.value(),
                            Map.of(), record.sourceOffset()));
                    continue;
                }

                String inputJson = new String(record.value(), StandardCharsets.UTF_8);
                JsonNode inputNode = objectMapper.readTree(inputJson);
                JsonNode outputNode;

                if (transform.type() == CompiledTransform.Type.DECLARATIVE) {
                    outputNode = DeclarativeMappingExecutor.execute(inputNode, mappingSpec);
                } else {
                    outputNode = jsExecutor.execute(transform.code(), inputNode);
                }

                String outputJson = objectMapper.writeValueAsString(outputNode);

                // Validate against target schema
                List<String> errors = schemaEnforcer.validateWithErrors(outputJson);
                if (!errors.isEmpty()) {
                    log.warn("Compiled transform produced invalid output for fingerprint {}: {}",
                            fingerprint, errors);
                    // Invalidate this transform — it's producing bad results
                    compiledTransformCache.invalidate(fingerprint);
                    if (metrics != null) {
                        metrics.recordFailed(1);
                        metrics.recordCompiledTransformInvalidation();
                    }
                    return Optional.empty(); // Fall through to LLM path
                }

                byte[] key = record.key();
                results.add(new TransformedRecord(key, outputJson.getBytes(StandardCharsets.UTF_8),
                        Map.of(), record.sourceOffset()));
            }

            log.debug("Compiled transform hit: type={}, fingerprint={}, records={}",
                    transform.type(), fingerprint, results.size());
            return Optional.of(results);

        } catch (GraalJsTransformExecutor.TransformExecutionException e) {
            log.warn("Compiled JS transform execution failed, invalidating: {}", e.getMessage());
            compiledTransformCache.invalidate(fingerprint);
            if (metrics != null) {
                metrics.recordCompiledTransformInvalidation();
            }
            return Optional.empty();
        } catch (Exception e) {
            log.warn("Compiled transform failed, falling back to LLM: {}", e.getMessage());
            return Optional.empty();
        }
    }

    /**
     * Trigger compilation of a transform for the current batch's schema.
     * This runs synchronously on the first LLM call for a new schema,
     * so that subsequent batches use the compiled path.
     */
    private void triggerCompilation(List<RawRecord> records) {
        RawRecord sample = records.get(0);
        if (sample.value() == null) return;

        String fingerprint = SchemaFingerprinter.fingerprint(sample.value(), targetSchema);

        // Already compiled? Skip.
        if (compiledTransformCache.get(fingerprint).isPresent()) {
            return;
        }

        try {
            if (metrics != null) {
                metrics.recordCompiledTransformCompilation();
            }
            Optional<CompiledTransform> compiled = transformCompiler.compile(records, fingerprint);
            compiled.ifPresent(ct -> compiledTransformCache.put(fingerprint, ct));
        } catch (Exception e) {
            log.warn("Transform compilation failed for fingerprint {}: {}", fingerprint, e.getMessage());
            // Not fatal — we'll keep using the LLM path for this schema
        }
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

    private List<TransformedRecord> processIndividually(List<RawRecord> records, String modelToUse) {
        List<TransformedRecord> results = new ArrayList<>();
        for (int i = 0; i < records.size(); i++) {
            RawRecord record = records.get(i);
            String singleJson = serializeBatch(List.of(record));
            try {
                String output = callLlmWithRetry(singleJson, modelToUse, 1);
                List<TransformedRecord> parsed = parseTransformedRecords(output, List.of(record));
                results.addAll(parsed);
            } catch (Exception e) {
                log.error("Failed to process record {} individually: {}", i, e.getMessage());
                if (metrics != null) {
                    metrics.recordFailed(1);
                }
            }
        }
        return results;
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

    private String callLlmWithRetry(String batchJson, String modelToUse) {
        return callLlmWithRetry(batchJson, modelToUse, -1);
    }

    private String callLlmWithRetry(String batchJson, String modelToUse, int expectedRecordCount) {
        String effectiveSystemPrompt = buildEffectiveSystemPrompt();
        String userPrompt;
        if (expectedRecordCount > 0) {
            userPrompt = "Transform the following " + expectedRecordCount
                    + " records. You MUST return a JSON array with EXACTLY "
                    + expectedRecordCount + " transformed objects, one per input record. "
                    + "Do NOT merge, skip, or combine records.\n" + batchJson;
        } else {
            userPrompt = "Transform the following records:\n" + batchJson;
        }

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
            // Rate limiting (before circuit breaker)
            if (llmRateLimiter != null) {
                if (!llmRateLimiter.tryAcquire()) {
                    if (metrics != null) {
                        metrics.recordLlmRateLimited();
                    }
                    try {
                        llmRateLimiter.acquire();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new NonRetryableException("Interrupted during rate limit wait", e);
                    }
                }
            }

            if (metrics != null) {
                metrics.recordLlmCall();
            }

            LlmResponse response;
            final LlmRequest currentRequest = request;
            try {
                if (llmCircuitBreaker != null) {
                    response = llmCircuitBreaker.executeSupplier(() -> llmClient.call(currentRequest));
                } else {
                    response = llmClient.call(currentRequest);
                }
            } catch (CallNotPermittedException e) {
                if (metrics != null) {
                    metrics.recordCircuitBreakerOpen();
                }
                throw new RetryableException("LLM circuit breaker is OPEN, retry later", e);
            }

            if (metrics != null) {
                metrics.recordLlmTokens(response.inputTokens(), response.outputTokens());
                double costUsd = LlmCostCalculator.calculateCostUsd(response);
                if (costUsd > 0) {
                    metrics.recordLlmCost(costUsd);
                }
            }

            log.debug("LLM response (attempt {}): tokens in={}, out={}, cache_create={}, cache_read={}, stop={}",
                    attempt, response.inputTokens(), response.outputTokens(),
                    response.cacheCreationInputTokens(), response.cacheReadInputTokens(),
                    response.stopReason());

            String content = stripMarkdownFences(response.content().trim());

            if (isValidJsonArray(content)) {
                return content;
            }

            // Also try extracting JSON array from within a JSON object wrapper
            // (OpenAI json_object mode may wrap arrays in {"results": [...]})
            String extracted = extractJsonArray(content);
            if (extracted != null) {
                return extracted;
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

    static String stripMarkdownFences(String text) {
        if (text == null) return "";
        String stripped = text.strip();
        // Remove ```json ... ``` or ``` ... ``` wrapping
        if (stripped.startsWith("```")) {
            int firstNewline = stripped.indexOf('\n');
            if (firstNewline > 0) {
                stripped = stripped.substring(firstNewline + 1);
            }
            if (stripped.endsWith("```")) {
                stripped = stripped.substring(0, stripped.length() - 3);
            }
            return stripped.strip();
        }
        return stripped;
    }

    private String extractJsonArray(String json) {
        try {
            JsonNode node = objectMapper.readTree(json);
            if (node.isObject()) {
                // OpenAI json_object mode wraps arrays in an object — find the first array field
                var fields = node.fields();
                while (fields.hasNext()) {
                    JsonNode value = fields.next().getValue();
                    if (value.isArray()) {
                        return objectMapper.writeValueAsString(value);
                    }
                }
            }
        } catch (JsonProcessingException e) {
            // not valid JSON at all
        }
        return null;
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
