package sh.oso.connect.ai.adapter.http.pagination;

/**
 * Factory for creating {@link PaginationStrategy} instances by type name.
 *
 * <p>Supported types:</p>
 * <ul>
 *   <li>{@code none} &ndash; single-page, no pagination</li>
 *   <li>{@code cursor} &ndash; cursor-based pagination</li>
 *   <li>{@code offset} &ndash; offset/limit-based pagination</li>
 *   <li>{@code page_number} &ndash; page-number-based pagination</li>
 *   <li>{@code link_header} &ndash; RFC 5988 Link header pagination</li>
 * </ul>
 */
public final class PaginationStrategyFactory {

    private PaginationStrategyFactory() {}

    /**
     * Creates a pagination strategy for the given type.
     *
     * @param type the pagination type identifier
     * @return a new, unconfigured {@link PaginationStrategy} instance
     * @throws IllegalArgumentException if the type is not supported
     */
    public static PaginationStrategy create(String type) {
        return switch (type.toLowerCase()) {
            case "none" -> new NoPagination();
            case "cursor" -> new CursorPagination();
            case "offset" -> new OffsetPagination();
            case "page_number" -> new PageNumberPagination();
            case "link_header" -> new LinkHeaderPagination();
            default -> throw new IllegalArgumentException(
                    "Unsupported pagination type: " + type
                            + ". Supported: none, cursor, offset, page_number, link_header");
        };
    }
}
