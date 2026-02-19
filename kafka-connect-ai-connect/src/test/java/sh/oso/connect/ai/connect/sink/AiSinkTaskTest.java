package sh.oso.connect.ai.connect.sink;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AiSinkTaskTest {

    @Test
    void versionReturnsExpectedValue() {
        AiSinkTask task = new AiSinkTask();

        assertEquals("0.1.0", task.version());
    }
}
