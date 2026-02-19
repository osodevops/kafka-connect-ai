package sh.oso.nexus.it;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import sh.oso.nexus.api.model.SourceOffset;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class OffsetManagementIT {

    @Test
    void sourceOffsetRoundTrip() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();

        // Create an offset representing a position in an HTTP paginated source
        SourceOffset offset = new SourceOffset(
                Map.of("source", "http://api.example.com/data"),
                Map.of("page", 3, "cursor", "abc123")
        );

        // Verify partition and offset are preserved
        assertEquals("http://api.example.com/data", offset.partition().get("source"));
        assertEquals(3, offset.offset().get("page"));
        assertEquals("abc123", offset.offset().get("cursor"));
    }

    @Test
    void emptyOffsetIsValid() {
        SourceOffset empty = SourceOffset.empty();

        assertTrue(empty.partition().isEmpty());
        assertTrue(empty.offset().isEmpty());
    }

    @Test
    void offsetMapsAreImmutable() {
        SourceOffset offset = new SourceOffset(
                Map.of("key", "value"),
                Map.of("pos", 1)
        );

        assertThrows(UnsupportedOperationException.class, () ->
                offset.partition().put("new", "entry"));
        assertThrows(UnsupportedOperationException.class, () ->
                offset.offset().put("new", 2));
    }

    @Test
    void offsetEqualityAndHashCode() {
        SourceOffset offset1 = new SourceOffset(
                Map.of("source", "test"),
                Map.of("page", 1)
        );
        SourceOffset offset2 = new SourceOffset(
                Map.of("source", "test"),
                Map.of("page", 1)
        );

        assertEquals(offset1, offset2);
        assertEquals(offset1.hashCode(), offset2.hashCode());
    }
}
