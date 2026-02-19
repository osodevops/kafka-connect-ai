package sh.oso.nexus.connect.spi;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.nexus.api.adapter.SinkAdapter;
import sh.oso.nexus.api.adapter.SourceAdapter;

import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

public final class AdapterRegistry {

    private static final Logger log = LoggerFactory.getLogger(AdapterRegistry.class);

    private final Map<String, SourceAdapter> sourceAdapters = new ConcurrentHashMap<>();
    private final Map<String, SinkAdapter> sinkAdapters = new ConcurrentHashMap<>();

    public AdapterRegistry() {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();

        ServiceLoader.load(SourceAdapter.class, cl).forEach(adapter -> {
            log.info("Registered source adapter: {}", adapter.type());
            sourceAdapters.put(adapter.type(), adapter);
        });

        ServiceLoader.load(SinkAdapter.class, cl).forEach(adapter -> {
            log.info("Registered sink adapter: {}", adapter.type());
            sinkAdapters.put(adapter.type(), adapter);
        });
    }

    public SourceAdapter getSourceAdapter(String type) {
        SourceAdapter adapter = sourceAdapters.get(type);
        if (adapter == null) {
            throw new IllegalArgumentException(
                    "No source adapter found for type: " + type + ". Available: " + sourceAdapters.keySet());
        }
        return adapter;
    }

    public SinkAdapter getSinkAdapter(String type) {
        SinkAdapter adapter = sinkAdapters.get(type);
        if (adapter == null) {
            throw new IllegalArgumentException(
                    "No sink adapter found for type: " + type + ". Available: " + sinkAdapters.keySet());
        }
        return adapter;
    }

    public Map<String, SourceAdapter> sourceAdapters() {
        return Map.copyOf(sourceAdapters);
    }

    public Map<String, SinkAdapter> sinkAdapters() {
        return Map.copyOf(sinkAdapters);
    }
}
