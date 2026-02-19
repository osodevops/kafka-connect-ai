package sh.oso.nexus.adapter.http.pagination;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PaginationStrategyFactoryTest {

    @Test
    void createNoneReturnsNoPagination() {
        PaginationStrategy strategy = PaginationStrategyFactory.create("none");
        assertInstanceOf(NoPagination.class, strategy);
    }

    @Test
    void createCursorReturnsCursorPagination() {
        PaginationStrategy strategy = PaginationStrategyFactory.create("cursor");
        assertInstanceOf(CursorPagination.class, strategy);
    }

    @Test
    void createOffsetReturnsOffsetPagination() {
        PaginationStrategy strategy = PaginationStrategyFactory.create("offset");
        assertInstanceOf(OffsetPagination.class, strategy);
    }

    @Test
    void createPageNumberReturnsPageNumberPagination() {
        PaginationStrategy strategy = PaginationStrategyFactory.create("page_number");
        assertInstanceOf(PageNumberPagination.class, strategy);
    }

    @Test
    void createLinkHeaderReturnsLinkHeaderPagination() {
        PaginationStrategy strategy = PaginationStrategyFactory.create("link_header");
        assertInstanceOf(LinkHeaderPagination.class, strategy);
    }

    @Test
    void createUnknownTypeThrowsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class,
                () -> PaginationStrategyFactory.create("unknown"));
    }
}
