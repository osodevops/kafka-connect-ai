package sh.oso.connect.ai.api.adapter;

import org.apache.kafka.common.config.ConfigDef;
import sh.oso.connect.ai.api.model.RawRecord;
import sh.oso.connect.ai.api.model.SourceOffset;

import java.util.List;
import java.util.Map;

public interface SourceAdapter extends AutoCloseable {

    String type();

    ConfigDef configDef();

    void start(Map<String, String> props);

    List<RawRecord> fetch(SourceOffset currentOffset, int maxRecords) throws InterruptedException;

    void commitOffset(SourceOffset offset);

    boolean isHealthy();

    default void stop() {}

    @Override
    default void close() {
        stop();
    }
}
