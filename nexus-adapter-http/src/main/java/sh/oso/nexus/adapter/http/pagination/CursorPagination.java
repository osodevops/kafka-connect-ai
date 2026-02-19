package sh.oso.nexus.adapter.http.pagination;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;

/**
 * Cursor-based pagination strategy.
 *
 * <p>Reads a cursor value from a JSON field in the response body and appends it
 * as a query parameter to subsequent requests. Pagination stops when the cursor
 * field is absent, null, or empty.</p>
 *
 * <p>Configuration properties:</p>
 * <ul>
 *   <li>{@code http.pagination.cursor.field} &ndash; JSON field containing the next cursor (default: "next_cursor")</li>
 *   <li>{@code http.pagination.cursor.param} &ndash; query parameter name for the cursor (default: "cursor")</li>
 * </ul>
 */
public class CursorPagination implements PaginationStrategy {

    private static final Logger log = LoggerFactory.getLogger(CursorPagination.class);

    private static final String PROP_CURSOR_FIELD = "http.pagination.cursor.field";
    private static final String PROP_CURSOR_PARAM = "http.pagination.cursor.param";

    private static final String DEFAULT_CURSOR_FIELD = "next_cursor";
    private static final String DEFAULT_CURSOR_PARAM = "cursor";

    private final ObjectMapper objectMapper = new ObjectMapper();

    private String cursorField;
    private String cursorParam;
    private boolean hasMore;

    @Override
    public void configure(Map<String, String> props) {
        this.cursorField = props.getOrDefault(PROP_CURSOR_FIELD, DEFAULT_CURSOR_FIELD);
        this.cursorParam = props.getOrDefault(PROP_CURSOR_PARAM, DEFAULT_CURSOR_PARAM);
        this.hasMore = true;
    }

    @Override
    public String type() {
        return "cursor";
    }

    @Override
    public String firstPageUrl(String baseUrl) {
        this.hasMore = true;
        return baseUrl;
    }

    @Override
    public Optional<String> nextPageUrl(String currentUrl, HttpResponse<String> response, String responseBody) {
        try {
            JsonNode root = objectMapper.readTree(responseBody);
            JsonNode cursorNode = root.path(cursorField);

            if (cursorNode.isMissingNode() || cursorNode.isNull()
                    || (cursorNode.isTextual() && cursorNode.asText().isEmpty())) {
                log.debug("No cursor found in field '{}'; pagination complete", cursorField);
                hasMore = false;
                return Optional.empty();
            }

            String cursorValue = cursorNode.asText();
            String nextUrl = appendOrReplaceParam(currentUrl, cursorParam, cursorValue);
            log.debug("Next cursor: {} -> {}", cursorValue, nextUrl);
            hasMore = true;
            return Optional.of(nextUrl);
        } catch (Exception e) {
            log.warn("Failed to extract cursor from response; stopping pagination", e);
            hasMore = false;
            return Optional.empty();
        }
    }

    @Override
    public boolean hasMore() {
        return hasMore;
    }

    private static String appendOrReplaceParam(String url, String param, String value) {
        String encoded = URLEncoder.encode(value, StandardCharsets.UTF_8);
        URI uri = URI.create(url);
        String query = uri.getRawQuery();

        String newParam = param + "=" + encoded;

        if (query == null || query.isEmpty()) {
            return url + "?" + newParam;
        }

        // Replace existing param if present
        String[] pairs = query.split("&");
        StringBuilder sb = new StringBuilder();
        boolean replaced = false;
        for (String pair : pairs) {
            if (pair.startsWith(param + "=")) {
                sb.append(sb.isEmpty() ? "" : "&").append(newParam);
                replaced = true;
            } else {
                sb.append(sb.isEmpty() ? "" : "&").append(pair);
            }
        }
        if (!replaced) {
            sb.append("&").append(newParam);
        }

        String base = url.substring(0, url.indexOf('?'));
        return base + "?" + sb;
    }
}
