package sh.oso.connect.ai.adapter.http.pagination;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URLEncoder;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;

/**
 * Offset-based pagination strategy.
 *
 * <p>Tracks a numeric offset that increments by the configured limit after each
 * page. Pagination stops when the response array contains fewer items than the
 * limit, indicating the final page has been reached.</p>
 *
 * <p>Configuration properties:</p>
 * <ul>
 *   <li>{@code http.pagination.offset.param} &ndash; query parameter name for offset (default: "offset")</li>
 *   <li>{@code http.pagination.limit.param} &ndash; query parameter name for limit (default: "limit")</li>
 *   <li>{@code http.pagination.limit.value} &ndash; number of items per page (default: "100")</li>
 * </ul>
 */
public class OffsetPagination implements PaginationStrategy {

    private static final Logger log = LoggerFactory.getLogger(OffsetPagination.class);

    private static final String PROP_OFFSET_PARAM = "http.pagination.offset.param";
    private static final String PROP_LIMIT_PARAM = "http.pagination.limit.param";
    private static final String PROP_LIMIT_VALUE = "http.pagination.limit.value";

    private static final String DEFAULT_OFFSET_PARAM = "offset";
    private static final String DEFAULT_LIMIT_PARAM = "limit";
    private static final int DEFAULT_LIMIT_VALUE = 100;

    private final ObjectMapper objectMapper = new ObjectMapper();

    private String offsetParam;
    private String limitParam;
    private int limitValue;
    private int currentOffset;
    private boolean hasMore;

    @Override
    public void configure(Map<String, String> props) {
        this.offsetParam = props.getOrDefault(PROP_OFFSET_PARAM, DEFAULT_OFFSET_PARAM);
        this.limitParam = props.getOrDefault(PROP_LIMIT_PARAM, DEFAULT_LIMIT_PARAM);
        this.limitValue = Integer.parseInt(props.getOrDefault(PROP_LIMIT_VALUE, String.valueOf(DEFAULT_LIMIT_VALUE)));
        this.currentOffset = 0;
        this.hasMore = true;
    }

    @Override
    public String type() {
        return "offset";
    }

    @Override
    public String firstPageUrl(String baseUrl) {
        this.currentOffset = 0;
        this.hasMore = true;
        return appendParams(baseUrl, 0);
    }

    @Override
    public Optional<String> nextPageUrl(String currentUrl, HttpResponse<String> response, String responseBody) {
        try {
            JsonNode root = objectMapper.readTree(responseBody);

            int resultCount;
            if (root.isArray()) {
                resultCount = root.size();
            } else {
                // Try common wrapper fields
                JsonNode data = findArrayNode(root);
                resultCount = data != null ? data.size() : 0;
            }

            if (resultCount < limitValue) {
                log.debug("Received {} items (limit {}); pagination complete", resultCount, limitValue);
                hasMore = false;
                return Optional.empty();
            }

            currentOffset += limitValue;
            String baseUrl = stripParams(currentUrl, offsetParam, limitParam);
            String nextUrl = appendParams(baseUrl, currentOffset);
            log.debug("Next offset: {} -> {}", currentOffset, nextUrl);
            hasMore = true;
            return Optional.of(nextUrl);
        } catch (Exception e) {
            log.warn("Failed to parse response for offset pagination; stopping", e);
            hasMore = false;
            return Optional.empty();
        }
    }

    @Override
    public boolean hasMore() {
        return hasMore;
    }

    private String appendParams(String url, int offset) {
        String separator = url.contains("?") ? "&" : "?";
        return url + separator
                + offsetParam + "=" + offset
                + "&" + limitParam + "=" + limitValue;
    }

    /**
     * Finds the first array node in the JSON object, checking common wrapper field names.
     */
    private static JsonNode findArrayNode(JsonNode root) {
        for (String field : new String[]{"data", "results", "items", "records"}) {
            JsonNode node = root.get(field);
            if (node != null && node.isArray()) {
                return node;
            }
        }
        // Fall back to the first array field found
        var fields = root.fields();
        while (fields.hasNext()) {
            var entry = fields.next();
            if (entry.getValue().isArray()) {
                return entry.getValue();
            }
        }
        return null;
    }

    private static String stripParams(String url, String... params) {
        int queryStart = url.indexOf('?');
        if (queryStart < 0) {
            return url;
        }

        String base = url.substring(0, queryStart);
        String query = url.substring(queryStart + 1);
        String[] pairs = query.split("&");
        StringBuilder sb = new StringBuilder();

        for (String pair : pairs) {
            boolean skip = false;
            for (String param : params) {
                if (pair.startsWith(param + "=")) {
                    skip = true;
                    break;
                }
            }
            if (!skip) {
                sb.append(sb.isEmpty() ? "" : "&").append(pair);
            }
        }

        return sb.isEmpty() ? base : base + "?" + sb;
    }
}
