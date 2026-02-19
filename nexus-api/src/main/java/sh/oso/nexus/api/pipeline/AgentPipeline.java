package sh.oso.nexus.api.pipeline;

import sh.oso.nexus.api.model.RawRecord;
import sh.oso.nexus.api.model.TransformedRecord;

import java.util.List;
import java.util.Map;

public interface AgentPipeline {

    void configure(Map<String, String> props);

    List<TransformedRecord> process(List<RawRecord> records);
}
