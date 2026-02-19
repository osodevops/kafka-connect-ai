package sh.oso.connect.ai.api.model;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class TransformedRecordTest {

    @Test
    void creationWithAllFields() {
        byte[] key = "k".getBytes(StandardCharsets.UTF_8);
        byte[] value = "v".getBytes(StandardCharsets.UTF_8);
        Map<String, String> headers = Map.of("h1", "val1");
        SourceOffset offset = new SourceOffset(Map.of("p", "1"), Map.of("o", 2));

        TransformedRecord record = new TransformedRecord(key, value, headers, offset);

        assertArrayEquals(key, record.key());
        assertArrayEquals(value, record.value());
        assertEquals(Map.of("h1", "val1"), record.headers());
        assertEquals(offset, record.sourceOffset());
    }

    @Test
    void nullHeadersDefaultsToEmptyMap() {
        TransformedRecord record = new TransformedRecord(new byte[0], new byte[0], null, null);
        assertNotNull(record.headers());
        assertTrue(record.headers().isEmpty());
    }

    @Test
    void nullSourceOffsetDefaultsToEmpty() {
        TransformedRecord record = new TransformedRecord(new byte[0], new byte[0], Map.of(), null);
        assertNotNull(record.sourceOffset());
        assertEquals(SourceOffset.empty(), record.sourceOffset());
    }

    @Test
    void headersAreImmutable() {
        TransformedRecord record = new TransformedRecord(new byte[0], new byte[0], Map.of("a", "b"), null);
        assertThrows(UnsupportedOperationException.class, () -> record.headers().put("x", "y"));
    }
}
