package sh.oso.connect.ai.connect.cache;

import java.time.Duration;

public record CacheResult(String cachedOutput, double similarity, Duration age) {}
