package sh.oso.connect.ai.adapter.http.pagination;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.http.HttpResponse;
import java.util.Map;
import java.util.Optional;

/**
 * Page-number-based pagination strategy.
 *
 * <p>Increments a page number query parameter after each request. Pagination
 * stops when the response array is empty.</p>
 *
 * <p>Configuration properties:</p>
 * <ul>
 *   <li>{@code http.pagination.page.param} &ndash; query parameter name for the page number (default: "page")</li>
 *   <li>{@code http.pagination.page.start} &ndash; starting page number (default: "1")</li>
 * </ul>
 */
public class PageNumberPagination implements PaginationStrategy {

    private static final Logger log = LoggerFactory.getLogger(PageNumberPagination.class);

    private static final String PROP_PAGE_PARAM = "http.pagination.page.param";
    private static final String PROP_PAGE_START = "http.pagination.page.start";

    private static final String DEFAULT_PAGE_PARAM = "page";
    private static final int DEFAULT_PAGE_START = 1;

    private final ObjectMapper objectMapper = new ObjectMapper();

    private String pageParam;
    private int startPage;
    private int currentPage;
    private boolean hasMore;

    @Override
    public void configure(Map<String, String> props) {
        this.pageParam = props.getOrDefault(PROP_PAGE_PARAM, DEFAULT_PAGE_PARAM);
        this.startPage = Integer.parseInt(props.getOrDefault(PROP_PAGE_START, String.valueOf(DEFAULT_PAGE_START)));
        this.currentPage = startPage;
        this.hasMore = true;
    }

    @Override
    public String type() {
        return "page_number";
    }

    @Override
    public String firstPageUrl(String baseUrl) {
        this.currentPage = startPage;
        this.hasMore = true;
        return appendPageParam(baseUrl, currentPage);
    }

    @Override
    public Optional<String> nextPageUrl(String currentUrl, HttpResponse<String> response, String responseBody) {
        try {
            JsonNode root = objectMapper.readTree(responseBody);

            int resultCount;
            if (root.isArray()) {
                resultCount = root.size();
            } else {
                JsonNode data = findArrayNode(root);
                resultCount = data != null ? data.size() : 0;
            }

            if (resultCount == 0) {
                log.debug("Empty result set on page {}; pagination complete", currentPage);
                hasMore = false;
                return Optional.empty();
            }

            currentPage++;
            String baseUrl = stripParam(currentUrl, pageParam);
            String nextUrl = appendPageParam(baseUrl, currentPage);
            log.debug("Next page: {} -> {}", currentPage, nextUrl);
            hasMore = true;
            return Optional.of(nextUrl);
        } catch (Exception e) {
            log.warn("Failed to parse response for page number pagination; stopping", e);
            hasMore = false;
            return Optional.empty();
        }
    }

    @Override
    public boolean hasMore() {
        return hasMore;
    }

    private String appendPageParam(String url, int page) {
        String separator = url.contains("?") ? "&" : "?";
        return url + separator + pageParam + "=" + page;
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
        var fields = root.fields();
        while (fields.hasNext()) {
            var entry = fields.next();
            if (entry.getValue().isArray()) {
                return entry.getValue();
            }
        }
        return null;
    }

    private static String stripParam(String url, String param) {
        int queryStart = url.indexOf('?');
        if (queryStart < 0) {
            return url;
        }

        String base = url.substring(0, queryStart);
        String query = url.substring(queryStart + 1);
        String[] pairs = query.split("&");
        StringBuilder sb = new StringBuilder();

        for (String pair : pairs) {
            if (!pair.startsWith(param + "=")) {
                sb.append(sb.isEmpty() ? "" : "&").append(pair);
            }
        }

        return sb.isEmpty() ? base : base + "?" + sb;
    }
}
