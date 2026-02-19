package sh.oso.connect.ai.adapter.http.pagination;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.http.HttpResponse;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Link-header-based pagination strategy (RFC 5988).
 *
 * <p>Parses the HTTP {@code Link} header from each response and follows the
 * {@code rel="next"} URL. Pagination stops when no {@code next} link is present.</p>
 */
public class LinkHeaderPagination implements PaginationStrategy {

    private static final Logger log = LoggerFactory.getLogger(LinkHeaderPagination.class);

    /**
     * Pattern to match a single link value: {@code <URL>; rel="next"}.
     * Handles optional whitespace and both quoted and unquoted rel values.
     */
    private static final Pattern LINK_PATTERN = Pattern.compile(
            "<([^>]+)>\\s*;\\s*rel\\s*=\\s*\"?next\"?",
            Pattern.CASE_INSENSITIVE
    );

    private boolean hasMore;

    @Override
    public void configure(Map<String, String> props) {
        this.hasMore = true;
    }

    @Override
    public String type() {
        return "link_header";
    }

    @Override
    public String firstPageUrl(String baseUrl) {
        this.hasMore = true;
        return baseUrl;
    }

    @Override
    public Optional<String> nextPageUrl(String currentUrl, HttpResponse<String> response, String responseBody) {
        Optional<String> linkHeader = response.headers().firstValue("Link")
                .or(() -> response.headers().firstValue("link"));

        if (linkHeader.isEmpty()) {
            log.debug("No Link header present; pagination complete");
            hasMore = false;
            return Optional.empty();
        }

        String header = linkHeader.get();
        Optional<String> nextUrl = parseNextLink(header);

        if (nextUrl.isEmpty()) {
            log.debug("No rel=\"next\" link found in header; pagination complete");
            hasMore = false;
            return Optional.empty();
        }

        log.debug("Next link: {}", nextUrl.get());
        hasMore = true;
        return nextUrl;
    }

    @Override
    public boolean hasMore() {
        return hasMore;
    }

    /**
     * Parses the Link header value and extracts the URL with {@code rel="next"}.
     *
     * <p>The header may contain multiple comma-separated link values, e.g.:
     * {@code <https://api.example.com/items?page=2>; rel="next", <https://api.example.com/items?page=5>; rel="last"}</p>
     *
     * @param header the Link header value
     * @return the next URL, or empty if not found
     */
    private static Optional<String> parseNextLink(String header) {
        // Split by comma to handle multiple link values, but be careful with
        // commas inside angle brackets
        String[] parts = header.split(",");
        for (String part : parts) {
            Matcher matcher = LINK_PATTERN.matcher(part.trim());
            if (matcher.find()) {
                return Optional.of(matcher.group(1));
            }
        }
        return Optional.empty();
    }
}
