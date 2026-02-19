package sh.oso.nexus.connect.pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.nexus.api.model.RawRecord;
import sh.oso.nexus.api.model.TransformedRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Function;

public class ParallelLlmExecutor {

    private static final Logger log = LoggerFactory.getLogger(ParallelLlmExecutor.class);

    private final int maxConcurrent;
    private final int callTimeoutSeconds;
    private final Semaphore semaphore;

    public ParallelLlmExecutor(int maxConcurrent, int callTimeoutSeconds) {
        this.maxConcurrent = maxConcurrent;
        this.callTimeoutSeconds = callTimeoutSeconds;
        this.semaphore = new Semaphore(maxConcurrent);
    }

    public List<TransformedRecord> execute(
            List<List<RawRecord>> subBatches,
            Function<List<RawRecord>, List<TransformedRecord>> processor) {

        if (subBatches.size() == 1) {
            return processor.apply(subBatches.get(0));
        }

        ExecutorService executor = Executors.newFixedThreadPool(maxConcurrent);
        try {
            List<Future<SubBatchResult>> futures = new ArrayList<>();

            for (int i = 0; i < subBatches.size(); i++) {
                final int index = i;
                final List<RawRecord> subBatch = subBatches.get(i);

                futures.add(executor.submit(() -> {
                    semaphore.acquire();
                    try {
                        List<TransformedRecord> result = processor.apply(subBatch);
                        return new SubBatchResult(index, result, null);
                    } catch (Exception e) {
                        log.warn("Sub-batch {} failed: {}", index, e.getMessage());
                        return new SubBatchResult(index, List.of(), e);
                    } finally {
                        semaphore.release();
                    }
                }));
            }

            // Collect results, preserving order
            SubBatchResult[] results = new SubBatchResult[subBatches.size()];
            for (Future<SubBatchResult> future : futures) {
                try {
                    SubBatchResult result = future.get(callTimeoutSeconds, TimeUnit.SECONDS);
                    results[result.index()] = result;
                } catch (TimeoutException e) {
                    log.warn("Sub-batch timed out after {}s", callTimeoutSeconds);
                    future.cancel(true);
                } catch (ExecutionException e) {
                    log.warn("Sub-batch execution failed: {}", e.getCause().getMessage());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted during parallel LLM execution", e);
                }
            }

            List<TransformedRecord> allResults = new ArrayList<>();
            for (SubBatchResult result : results) {
                if (result != null && result.error() == null) {
                    allResults.addAll(result.records());
                }
            }
            return allResults;
        } finally {
            executor.shutdown();
        }
    }

    public int getMaxConcurrent() {
        return maxConcurrent;
    }

    private record SubBatchResult(int index, List<TransformedRecord> records, Exception error) {}
}
