package sh.oso.connect.ai.api.model;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class RawRecordTest {

    @Test
    void creationWithAllFields() {
        byte[] key = "k".getBytes(StandardCharsets.UTF_8);
        byte[] value = "v".getBytes(StandardCharsets.UTF_8);
        Map<String, String> metadata = Map.of("src", "test");
        SourceOffset offset = new SourceOffset(Map.of("p", "1"), Map.of("o", 2));

        RawRecord record = new RawRecord(key, value, metadata, offset);

        assertArrayEquals(key, record.key());
        assertArrayEquals(value, record.value());
        assertEquals(Map.of("src", "test"), record.metadata());
        assertEquals(offset, record.sourceOffset());
    }

    @Test
    void nullMetadataDefaultsToEmptyMap() {
        RawRecord record = new RawRecord(new byte[0], new byte[0], null, null);
        assertNotNull(record.metadata());
        assertTrue(record.metadata().isEmpty());
    }

    @Test
    void nullSourceOffsetDefaultsToEmpty() {
        RawRecord record = new RawRecord(new byte[0], new byte[0], Map.of(), null);
        assertNotNull(record.sourceOffset());
        assertEquals(SourceOffset.empty(), record.sourceOffset());
    }

    @Test
    void metadataIsImmutable() {
        RawRecord record = new RawRecord(new byte[0], new byte[0], Map.of("a", "b"), null);
        assertThrows(UnsupportedOperationException.class, () -> record.metadata().put("x", "y"));
    }
}
