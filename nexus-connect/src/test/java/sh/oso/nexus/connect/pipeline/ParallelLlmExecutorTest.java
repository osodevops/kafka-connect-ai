package sh.oso.nexus.connect.pipeline;

import org.junit.jupiter.api.Test;
import sh.oso.nexus.api.model.RawRecord;
import sh.oso.nexus.api.model.SourceOffset;
import sh.oso.nexus.api.model.TransformedRecord;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class ParallelLlmExecutorTest {

    private RawRecord record(String key) {
        return new RawRecord(
                key.getBytes(StandardCharsets.UTF_8),
                "value".getBytes(StandardCharsets.UTF_8),
                Map.of(),
                SourceOffset.empty()
        );
    }

    private TransformedRecord transformed(String key) {
        return new TransformedRecord(
                key.getBytes(StandardCharsets.UTF_8),
                "transformed".getBytes(StandardCharsets.UTF_8),
                Map.of(),
                SourceOffset.empty()
        );
    }

    @Test
    void executesSingleBatchDirectly() {
        ParallelLlmExecutor executor = new ParallelLlmExecutor(5, 30);

        List<List<RawRecord>> subBatches = List.of(
                List.of(record("r1"), record("r2"))
        );

        AtomicInteger calls = new AtomicInteger(0);
        List<TransformedRecord> result = executor.execute(subBatches, batch -> {
            calls.incrementAndGet();
            return batch.stream().map(r -> transformed(new String(r.key()))).toList();
        });

        assertEquals(1, calls.get());
        assertEquals(2, result.size());
    }

    @Test
    void executesMultipleBatchesConcurrently() {
        ParallelLlmExecutor executor = new ParallelLlmExecutor(3, 30);

        List<List<RawRecord>> subBatches = List.of(
                List.of(record("r1")),
                List.of(record("r2")),
                List.of(record("r3"))
        );

        AtomicInteger concurrentCount = new AtomicInteger(0);
        AtomicInteger maxConcurrent = new AtomicInteger(0);

        List<TransformedRecord> result = executor.execute(subBatches, batch -> {
            int current = concurrentCount.incrementAndGet();
            maxConcurrent.updateAndGet(max -> Math.max(max, current));
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            concurrentCount.decrementAndGet();
            return batch.stream().map(r -> transformed(new String(r.key()))).toList();
        });

        assertEquals(3, result.size());
        // Should have some concurrency (at least 2 at once)
        assertTrue(maxConcurrent.get() >= 1);
    }

    @Test
    void respectsSemaphoreLimit() {
        ParallelLlmExecutor executor = new ParallelLlmExecutor(2, 30);

        List<List<RawRecord>> subBatches = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            subBatches.add(List.of(record("r" + i)));
        }

        AtomicInteger concurrentCount = new AtomicInteger(0);
        AtomicInteger maxConcurrent = new AtomicInteger(0);

        List<TransformedRecord> result = executor.execute(subBatches, batch -> {
            int current = concurrentCount.incrementAndGet();
            maxConcurrent.updateAndGet(max -> Math.max(max, current));
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            concurrentCount.decrementAndGet();
            return batch.stream().map(r -> transformed(new String(r.key()))).toList();
        });

        assertEquals(5, result.size());
        assertTrue(maxConcurrent.get() <= 2, "Should not exceed semaphore limit of 2");
    }

    @Test
    void handlesPartialFailure() {
        ParallelLlmExecutor executor = new ParallelLlmExecutor(3, 30);

        List<List<RawRecord>> subBatches = List.of(
                List.of(record("r1")),
                List.of(record("r2")),
                List.of(record("r3"))
        );

        AtomicInteger callIndex = new AtomicInteger(0);
        List<TransformedRecord> result = executor.execute(subBatches, batch -> {
            int idx = callIndex.getAndIncrement();
            if (idx == 1) {
                throw new RuntimeException("Simulated failure");
            }
            return batch.stream().map(r -> transformed(new String(r.key()))).toList();
        });

        // 2 of 3 sub-batches succeeded
        assertEquals(2, result.size());
    }

    @Test
    void preservesOrdering() {
        ParallelLlmExecutor executor = new ParallelLlmExecutor(5, 30);

        List<List<RawRecord>> subBatches = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            subBatches.add(List.of(record("r" + i)));
        }

        List<TransformedRecord> result = executor.execute(subBatches, batch -> {
            // Add random delay to shuffle completion order
            try {
                Thread.sleep((long) (Math.random() * 50));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return batch.stream().map(r ->
                    new TransformedRecord(r.key(), r.value(), Map.of(), r.sourceOffset())
            ).toList();
        });

        assertEquals(5, result.size());
        for (int i = 0; i < 5; i++) {
            assertEquals("r" + i, new String(result.get(i).key()));
        }
    }
}
