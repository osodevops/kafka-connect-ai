package sh.oso.connect.ai.adapter.http.pagination;

import java.net.http.HttpResponse;
import java.util.Map;
import java.util.Optional;

/**
 * Strategy interface for paginating through HTTP API responses.
 *
 * <p>Implementations handle different pagination mechanisms such as cursor-based,
 * offset-based, page-number-based, and Link-header-based pagination.</p>
 */
public interface PaginationStrategy {

    /**
     * Configures the strategy from connector properties.
     *
     * @param props connector configuration properties
     */
    void configure(Map<String, String> props);

    /**
     * Returns the pagination type identifier (e.g. "cursor", "offset").
     */
    String type();

    /**
     * Builds the URL for the first page request.
     *
     * @param baseUrl the base endpoint URL
     * @return the URL to fetch the first page
     */
    String firstPageUrl(String baseUrl);

    /**
     * Determines the URL for the next page based on the current response.
     *
     * @param currentUrl   the URL that was just requested
     * @param response     the HTTP response from the current page
     * @param responseBody the response body as a string
     * @return the next page URL, or empty if there are no more pages
     */
    Optional<String> nextPageUrl(String currentUrl, HttpResponse<String> response, String responseBody);

    /**
     * Returns whether there are more pages to fetch.
     */
    boolean hasMore();
}
