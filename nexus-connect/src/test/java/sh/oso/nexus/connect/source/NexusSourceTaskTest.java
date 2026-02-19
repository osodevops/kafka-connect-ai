package sh.oso.nexus.connect.source;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class NexusSourceTaskTest {

    @Test
    void versionReturnsExpectedValue() {
        NexusSourceTask task = new NexusSourceTask();

        assertEquals("0.1.0", task.version());
    }
}
