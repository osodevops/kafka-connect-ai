package sh.oso.nexus.connect.sink;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class NexusSinkTaskTest {

    @Test
    void versionReturnsExpectedValue() {
        NexusSinkTask task = new NexusSinkTask();

        assertEquals("0.1.0", task.version());
    }
}
