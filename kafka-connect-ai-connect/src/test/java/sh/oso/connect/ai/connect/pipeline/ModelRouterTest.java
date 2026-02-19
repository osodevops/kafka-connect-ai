package sh.oso.connect.ai.connect.pipeline;

import org.junit.jupiter.api.Test;
import sh.oso.connect.ai.api.model.RawRecord;
import sh.oso.connect.ai.api.model.SourceOffset;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class ModelRouterTest {

    private RawRecord record(String json) {
        return new RawRecord(
                null,
                json.getBytes(StandardCharsets.UTF_8),
                Map.of(),
                SourceOffset.empty()
        );
    }

    @Test
    void tier0DeterministicForSimpleFlatRecords() {
        ModelRouter router = new ModelRouter("fast", "default", "powerful",
                Set.of("field_rename"));

        List<RawRecord> batch = List.of(
                record("{\"firstName\":\"John\",\"age\":30}"),
                record("{\"firstName\":\"Jane\",\"age\":25}")
        );

        ModelRouter.RoutingDecision decision = router.route(batch);
        assertEquals(0, decision.tier());
        assertNull(decision.modelName());
    }

    @Test
    void tier1FastForSimpleFlatRecordsNoDeterministicPatterns() {
        ModelRouter router = new ModelRouter("fast", "default", "powerful", Set.of());

        List<RawRecord> batch = List.of(
                record("{\"name\":\"John\",\"age\":30}"),
                record("{\"name\":\"Jane\",\"age\":25}")
        );

        ModelRouter.RoutingDecision decision = router.route(batch);
        assertEquals(1, decision.tier());
        assertEquals("fast", decision.modelName());
    }

    @Test
    void tier2DefaultForNestedStructures() {
        ModelRouter router = new ModelRouter("fast", "default", "powerful", Set.of());

        List<RawRecord> batch = List.of(
                record("{\"user\":{\"name\":\"John\",\"address\":{\"city\":\"NYC\"}},\"age\":30}")
        );

        ModelRouter.RoutingDecision decision = router.route(batch);
        assertEquals(2, decision.tier());
        assertEquals("default", decision.modelName());
    }

    @Test
    void tier3PowerfulForVeryComplexRecords() {
        ModelRouter router = new ModelRouter("fast", "default", "powerful", Set.of());

        // Many fields, deeply nested
        String complex = "{\"a\":{\"b\":{\"c\":{\"d\":{\"e\":1}}}},\"f1\":1,\"f2\":2," +
                "\"f3\":3,\"f4\":4,\"f5\":5,\"f6\":6,\"f7\":7,\"f8\":8,\"f9\":9,\"f10\":10," +
                "\"f11\":11,\"f12\":12,\"f13\":13,\"f14\":14,\"f15\":15,\"f16\":16,\"f17\":17," +
                "\"f18\":18,\"f19\":19,\"f20\":20,\"f21\":21}";
        List<RawRecord> batch = List.of(record(complex));

        ModelRouter.RoutingDecision decision = router.route(batch);
        assertEquals(3, decision.tier());
        assertEquals("powerful", decision.modelName());
    }

    @Test
    void tier3ForUnparseableRecords() {
        ModelRouter router = new ModelRouter("fast", "default", "powerful", Set.of());

        List<RawRecord> batch = List.of(
                record("not valid json at all")
        );

        ModelRouter.RoutingDecision decision = router.route(batch);
        assertEquals(3, decision.tier());
        assertEquals("powerful", decision.modelName());
    }

    @Test
    void emptyBatchReturnsTier2Default() {
        ModelRouter router = new ModelRouter("fast", "default", "powerful", Set.of());

        ModelRouter.RoutingDecision decision = router.route(List.of());
        assertEquals(2, decision.tier());
        assertEquals("default", decision.modelName());
    }
}
