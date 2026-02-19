package sh.oso.nexus.connect.pipeline;

import sh.oso.nexus.api.model.RawRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public class BatchAccumulator {

    private final int batchSize;
    private final long maxWaitMs;
    private final ReentrantLock lock = new ReentrantLock();

    private final List<RawRecord> buffer = new ArrayList<>();
    private long firstRecordTimestamp = -1;

    public BatchAccumulator(int batchSize, long maxWaitMs) {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be positive");
        }
        if (maxWaitMs <= 0) {
            throw new IllegalArgumentException("maxWaitMs must be positive");
        }
        this.batchSize = batchSize;
        this.maxWaitMs = maxWaitMs;
    }

    public void add(List<RawRecord> records) {
        if (records == null || records.isEmpty()) {
            return;
        }
        lock.lock();
        try {
            if (buffer.isEmpty()) {
                firstRecordTimestamp = System.currentTimeMillis();
            }
            buffer.addAll(records);
        } finally {
            lock.unlock();
        }
    }

    public boolean shouldFlush() {
        lock.lock();
        try {
            if (buffer.isEmpty()) {
                return false;
            }
            if (buffer.size() >= batchSize) {
                return true;
            }
            return firstRecordTimestamp > 0
                    && (System.currentTimeMillis() - firstRecordTimestamp) >= maxWaitMs;
        } finally {
            lock.unlock();
        }
    }

    public List<List<RawRecord>> flush() {
        lock.lock();
        try {
            if (buffer.isEmpty()) {
                return List.of();
            }

            List<List<RawRecord>> subBatches = new ArrayList<>();
            for (int i = 0; i < buffer.size(); i += batchSize) {
                int end = Math.min(i + batchSize, buffer.size());
                subBatches.add(new ArrayList<>(buffer.subList(i, end)));
            }

            buffer.clear();
            firstRecordTimestamp = -1;
            return subBatches;
        } finally {
            lock.unlock();
        }
    }

    public int size() {
        lock.lock();
        try {
            return buffer.size();
        } finally {
            lock.unlock();
        }
    }
}
