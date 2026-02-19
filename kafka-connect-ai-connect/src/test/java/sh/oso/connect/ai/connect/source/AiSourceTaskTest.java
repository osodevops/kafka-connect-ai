package sh.oso.connect.ai.connect.source;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AiSourceTaskTest {

    @Test
    void versionReturnsExpectedValue() {
        AiSourceTask task = new AiSourceTask();

        assertEquals("0.1.0", task.version());
    }
}
