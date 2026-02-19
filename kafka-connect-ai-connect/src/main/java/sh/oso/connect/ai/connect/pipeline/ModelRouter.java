package sh.oso.connect.ai.connect.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.connect.ai.api.model.RawRecord;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;

public class ModelRouter {

    private static final Logger log = LoggerFactory.getLogger(ModelRouter.class);
    private static final int SIMPLE_FIELD_THRESHOLD = 5;
    private static final int COMPLEX_NESTING_THRESHOLD = 2;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final String fastModel;
    private final String defaultModel;
    private final String powerfulModel;
    private final Set<String> deterministicPatterns;

    public ModelRouter(String fastModel, String defaultModel, String powerfulModel,
                       Set<String> deterministicPatterns) {
        this.fastModel = fastModel;
        this.defaultModel = defaultModel;
        this.powerfulModel = powerfulModel;
        this.deterministicPatterns = deterministicPatterns;
    }

    public RoutingDecision route(List<RawRecord> batch) {
        if (batch.isEmpty()) {
            return new RoutingDecision(2, defaultModel, "empty batch");
        }

        // Analyze batch characteristics
        int totalFields = 0;
        int maxNestingDepth = 0;
        int totalRecordSize = 0;
        boolean allParseable = true;

        for (RawRecord record : batch) {
            if (record.value() == null) continue;
            try {
                JsonNode node = objectMapper.readTree(new String(record.value(), StandardCharsets.UTF_8));
                totalFields += countFields(node);
                maxNestingDepth = Math.max(maxNestingDepth, maxDepth(node));
                totalRecordSize += record.value().length;
            } catch (Exception e) {
                allParseable = false;
            }
        }

        int avgFields = batch.isEmpty() ? 0 : totalFields / batch.size();

        // Tier 0: deterministic — if patterns are configured and records are simple
        if (!deterministicPatterns.isEmpty() && allParseable && avgFields <= SIMPLE_FIELD_THRESHOLD
                && maxNestingDepth <= 1) {
            return new RoutingDecision(0, null,
                    "deterministic: simple flat records match patterns");
        }

        // Tier 1: fast model — simple format conversion, low field count, flat structure
        if (allParseable && avgFields <= SIMPLE_FIELD_THRESHOLD && maxNestingDepth <= 1) {
            return new RoutingDecision(1, fastModel,
                    "simple flat records with few fields");
        }

        // Tier 3: powerful model — very complex/large records or unparseable
        if (!allParseable || maxNestingDepth > COMPLEX_NESTING_THRESHOLD + 1
                || avgFields > SIMPLE_FIELD_THRESHOLD * 4) {
            return new RoutingDecision(3, powerfulModel,
                    "complex records requiring multi-step reasoning");
        }

        // Tier 2: default model — everything else
        return new RoutingDecision(2, defaultModel,
                "moderate complexity, nested structures");
    }

    private int countFields(JsonNode node) {
        if (node.isObject()) {
            int count = node.size();
            for (JsonNode child : node) {
                if (child.isObject() || child.isArray()) {
                    count += countFields(child);
                }
            }
            return count;
        } else if (node.isArray()) {
            int count = 0;
            for (JsonNode child : node) {
                count += countFields(child);
            }
            return count;
        }
        return 0;
    }

    private int maxDepth(JsonNode node) {
        if (!node.isObject() && !node.isArray()) {
            return 0;
        }
        int max = 0;
        for (JsonNode child : node) {
            max = Math.max(max, maxDepth(child));
        }
        return 1 + max;
    }

    public record RoutingDecision(int tier, String modelName, String reason) {}
}
