package sh.oso.connect.ai.adapter.warehouse;

import org.junit.jupiter.api.Test;
import sh.oso.connect.ai.api.model.TransformedRecord;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class WarehouseSinkAdapterTest {

    @Test
    void typeReturnsWarehouse() {
        WarehouseSinkAdapter adapter = new WarehouseSinkAdapter();
        assertEquals("warehouse", adapter.type());
    }

    @Test
    void configDefContainsWarehouseProvider() {
        WarehouseSinkAdapter adapter = new WarehouseSinkAdapter();
        assertNotNull(adapter.configDef().configKeys().get("warehouse.provider"));
    }

    @Test
    void configDefContainsWarehouseBatchSize() {
        WarehouseSinkAdapter adapter = new WarehouseSinkAdapter();
        assertNotNull(adapter.configDef().configKeys().get("warehouse.batch.size"));
    }

    @Test
    void configDefContainsSnowflakeKeys() {
        WarehouseSinkAdapter adapter = new WarehouseSinkAdapter();
        assertNotNull(adapter.configDef().configKeys().get("snowflake.url"));
        assertNotNull(adapter.configDef().configKeys().get("snowflake.user"));
        assertNotNull(adapter.configDef().configKeys().get("snowflake.private.key"));
        assertNotNull(adapter.configDef().configKeys().get("snowflake.database"));
        assertNotNull(adapter.configDef().configKeys().get("snowflake.schema"));
        assertNotNull(adapter.configDef().configKeys().get("snowflake.table"));
        assertNotNull(adapter.configDef().configKeys().get("snowflake.warehouse"));
        assertNotNull(adapter.configDef().configKeys().get("snowflake.role"));
    }

    @Test
    void configDefContainsRedshiftKeys() {
        WarehouseSinkAdapter adapter = new WarehouseSinkAdapter();
        assertNotNull(adapter.configDef().configKeys().get("redshift.jdbc.url"));
        assertNotNull(adapter.configDef().configKeys().get("redshift.user"));
        assertNotNull(adapter.configDef().configKeys().get("redshift.password"));
        assertNotNull(adapter.configDef().configKeys().get("redshift.table"));
        assertNotNull(adapter.configDef().configKeys().get("redshift.s3.bucket"));
        assertNotNull(adapter.configDef().configKeys().get("redshift.s3.region"));
        assertNotNull(adapter.configDef().configKeys().get("redshift.iam.role"));
    }

    @Test
    void configDefContainsBigQueryKeys() {
        WarehouseSinkAdapter adapter = new WarehouseSinkAdapter();
        assertNotNull(adapter.configDef().configKeys().get("bigquery.project"));
        assertNotNull(adapter.configDef().configKeys().get("bigquery.dataset"));
        assertNotNull(adapter.configDef().configKeys().get("bigquery.table"));
        assertNotNull(adapter.configDef().configKeys().get("bigquery.credentials.path"));
    }

    @Test
    void writeWithEmptyListIsNoOp() {
        WarehouseSinkAdapter adapter = new WarehouseSinkAdapter();
        // Should not throw even without start() being called
        assertDoesNotThrow(() -> adapter.write(Collections.emptyList()));
    }

    @Test
    void isHealthyReturnsFalseBeforeStart() {
        WarehouseSinkAdapter adapter = new WarehouseSinkAdapter();
        assertFalse(adapter.isHealthy());
    }

    @Test
    void stopIsIdempotent() {
        WarehouseSinkAdapter adapter = new WarehouseSinkAdapter();
        // Calling stop multiple times should not throw
        assertDoesNotThrow(() -> {
            adapter.stop();
            adapter.stop();
            adapter.stop();
        });
    }

    @Test
    void stopAfterNoStartDoesNotThrow() {
        WarehouseSinkAdapter adapter = new WarehouseSinkAdapter();
        assertDoesNotThrow(adapter::stop);
    }

    @Test
    void flushBeforeStartDoesNotThrow() {
        WarehouseSinkAdapter adapter = new WarehouseSinkAdapter();
        assertDoesNotThrow(adapter::flush);
    }
}
