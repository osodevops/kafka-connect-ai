package sh.oso.connect.ai.api.pipeline;

import sh.oso.connect.ai.api.model.RawRecord;
import sh.oso.connect.ai.api.model.TransformedRecord;

import java.util.List;
import java.util.Map;

public interface AgentPipeline {

    void configure(Map<String, String> props);

    List<TransformedRecord> process(List<RawRecord> records);
}
