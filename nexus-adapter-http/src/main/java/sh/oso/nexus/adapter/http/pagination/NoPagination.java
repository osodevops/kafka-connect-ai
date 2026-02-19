package sh.oso.nexus.adapter.http.pagination;

import java.net.http.HttpResponse;
import java.util.Map;
import java.util.Optional;

/**
 * A no-op pagination strategy that fetches a single page with no continuation.
 */
public class NoPagination implements PaginationStrategy {

    @Override
    public void configure(Map<String, String> props) {
        // nothing to configure
    }

    @Override
    public String type() {
        return "none";
    }

    @Override
    public String firstPageUrl(String baseUrl) {
        return baseUrl;
    }

    @Override
    public Optional<String> nextPageUrl(String currentUrl, HttpResponse<String> response, String responseBody) {
        return Optional.empty();
    }

    @Override
    public boolean hasMore() {
        return false;
    }
}
