package sh.oso.nexus.api.model;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class SourceOffsetTest {

    @Test
    void emptyFactoryReturnsEmptyMaps() {
        SourceOffset empty = SourceOffset.empty();
        assertTrue(empty.partition().isEmpty());
        assertTrue(empty.offset().isEmpty());
    }

    @Test
    void constructorCreatesDefensiveCopies() {
        HashMap<String, String> partition = new HashMap<>();
        partition.put("topic", "t1");
        HashMap<String, Object> offset = new HashMap<>();
        offset.put("pos", 42);

        SourceOffset so = new SourceOffset(partition, offset);

        // Mutating the original maps must not affect the record
        partition.put("topic", "changed");
        offset.put("pos", 99);

        assertEquals("t1", so.partition().get("topic"));
        assertEquals(42, so.offset().get("pos"));

        // The stored maps themselves must be unmodifiable
        assertThrows(UnsupportedOperationException.class, () -> so.partition().put("x", "y"));
        assertThrows(UnsupportedOperationException.class, () -> so.offset().put("x", "y"));
    }

    @Test
    void equalityBetweenIdenticalInstances() {
        Map<String, String> partition = Map.of("topic", "t1");
        Map<String, Object> offset = Map.of("pos", 1L);

        SourceOffset a = new SourceOffset(partition, offset);
        SourceOffset b = new SourceOffset(partition, offset);

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    void partitionAndOffsetAccessors() {
        Map<String, String> partition = Map.of("db", "mydb");
        Map<String, Object> offset = Map.of("lsn", 100L);

        SourceOffset so = new SourceOffset(partition, offset);

        assertEquals(Map.of("db", "mydb"), so.partition());
        assertEquals(Map.of("lsn", 100L), so.offset());
    }
}
