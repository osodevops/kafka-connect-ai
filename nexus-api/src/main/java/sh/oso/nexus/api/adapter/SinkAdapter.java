package sh.oso.nexus.api.adapter;

import org.apache.kafka.common.config.ConfigDef;
import sh.oso.nexus.api.model.TransformedRecord;

import java.util.List;
import java.util.Map;

public interface SinkAdapter extends AutoCloseable {

    String type();

    ConfigDef configDef();

    void start(Map<String, String> props);

    void write(List<TransformedRecord> records);

    void flush();

    boolean isHealthy();

    default void stop() {}

    @Override
    default void close() {
        stop();
    }
}
