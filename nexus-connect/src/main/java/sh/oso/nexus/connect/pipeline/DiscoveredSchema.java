package sh.oso.nexus.connect.pipeline;

import java.time.Instant;

public record DiscoveredSchema(
        String jsonSchema,
        int sampleSize,
        Instant discoveredAt
) {}
