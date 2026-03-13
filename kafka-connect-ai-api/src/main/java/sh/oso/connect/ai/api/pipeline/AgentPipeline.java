package sh.oso.connect.ai.api.pipeline;

import sh.oso.connect.ai.api.model.RawRecord;
import sh.oso.connect.ai.api.model.TransformedRecord;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface AgentPipeline extends Closeable {

    void configure(Map<String, String> props);

    List<TransformedRecord> process(List<RawRecord> records);

    @Override
    default void close() throws IOException {
        // Default no-op for implementations that don't need cleanup
    }
}
