package sh.oso.connect.ai.api.util;

import sh.oso.connect.ai.api.model.RawRecord;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

public class BoundedRecordBuffer {

    private static final int DEFAULT_CAPACITY = 10_000;

    private final ArrayBlockingQueue<RawRecord> queue;

    public BoundedRecordBuffer() {
        this(DEFAULT_CAPACITY);
    }

    public BoundedRecordBuffer(int capacity) {
        this.queue = new ArrayBlockingQueue<>(capacity);
    }

    public boolean offer(RawRecord record) {
        return queue.offer(record);
    }

    public List<RawRecord> drain(int maxRecords, Duration timeout) throws InterruptedException {
        List<RawRecord> records = new ArrayList<>();
        RawRecord first = queue.poll(timeout.toMillis(), TimeUnit.MILLISECONDS);
        if (first == null) {
            return records;
        }
        records.add(first);
        queue.drainTo(records, maxRecords - 1);
        return records;
    }

    public int size() {
        return queue.size();
    }

    public boolean isEmpty() {
        return queue.isEmpty();
    }

    public void clear() {
        queue.clear();
    }
}
