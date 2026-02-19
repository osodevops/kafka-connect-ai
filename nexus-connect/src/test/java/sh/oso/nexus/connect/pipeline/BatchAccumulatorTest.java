package sh.oso.nexus.connect.pipeline;

import org.junit.jupiter.api.Test;
import sh.oso.nexus.api.model.RawRecord;
import sh.oso.nexus.api.model.SourceOffset;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;

class BatchAccumulatorTest {

    private RawRecord record(String key) {
        return new RawRecord(
                key.getBytes(StandardCharsets.UTF_8),
                "value".getBytes(StandardCharsets.UTF_8),
                Map.of(),
                SourceOffset.empty()
        );
    }

    @Test
    void flushOnSizeTrigger() {
        BatchAccumulator acc = new BatchAccumulator(3, 60000);

        acc.add(List.of(record("r1"), record("r2"), record("r3")));

        assertTrue(acc.shouldFlush());
        List<List<RawRecord>> batches = acc.flush();
        assertEquals(1, batches.size());
        assertEquals(3, batches.get(0).size());
        assertEquals(0, acc.size());
    }

    @Test
    void flushOnTimeTrigger() throws InterruptedException {
        BatchAccumulator acc = new BatchAccumulator(100, 50);

        acc.add(List.of(record("r1")));
        assertFalse(acc.shouldFlush());

        Thread.sleep(100);
        assertTrue(acc.shouldFlush());

        List<List<RawRecord>> batches = acc.flush();
        assertEquals(1, batches.size());
        assertEquals(1, batches.get(0).size());
    }

    @Test
    void emptyFlushReturnsEmptyList() {
        BatchAccumulator acc = new BatchAccumulator(10, 5000);
        List<List<RawRecord>> result = acc.flush();
        assertTrue(result.isEmpty());
    }

    @Test
    void shouldFlushReturnsFalseWhenEmpty() {
        BatchAccumulator acc = new BatchAccumulator(10, 5000);
        assertFalse(acc.shouldFlush());
    }

    @Test
    void multiBatchSplit() {
        BatchAccumulator acc = new BatchAccumulator(2, 60000);

        acc.add(List.of(record("r1"), record("r2"), record("r3"), record("r4"), record("r5")));

        assertTrue(acc.shouldFlush());
        List<List<RawRecord>> batches = acc.flush();
        assertEquals(3, batches.size());
        assertEquals(2, batches.get(0).size());
        assertEquals(2, batches.get(1).size());
        assertEquals(1, batches.get(2).size());
    }

    @Test
    void addNullOrEmptyIsNoOp() {
        BatchAccumulator acc = new BatchAccumulator(10, 5000);
        acc.add(null);
        assertEquals(0, acc.size());
        acc.add(List.of());
        assertEquals(0, acc.size());
    }

    @Test
    void threadSafety() throws InterruptedException {
        BatchAccumulator acc = new BatchAccumulator(1000, 60000);
        int threadCount = 10;
        int recordsPerThread = 100;
        CountDownLatch latch = new CountDownLatch(threadCount);

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        for (int t = 0; t < threadCount; t++) {
            executor.submit(() -> {
                List<RawRecord> records = new ArrayList<>();
                for (int i = 0; i < recordsPerThread; i++) {
                    records.add(record("r" + i));
                }
                acc.add(records);
                latch.countDown();
            });
        }

        latch.await();
        assertEquals(threadCount * recordsPerThread, acc.size());
        executor.shutdown();
    }

    @Test
    void constructorRejectsInvalidArguments() {
        assertThrows(IllegalArgumentException.class, () -> new BatchAccumulator(0, 1000));
        assertThrows(IllegalArgumentException.class, () -> new BatchAccumulator(10, 0));
    }
}
