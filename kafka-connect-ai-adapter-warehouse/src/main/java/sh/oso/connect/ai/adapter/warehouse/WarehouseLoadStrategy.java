package sh.oso.connect.ai.adapter.warehouse;

import sh.oso.connect.ai.api.model.TransformedRecord;

import java.util.List;
import java.util.Map;

/**
 * Strategy interface for loading data into cloud data warehouses.
 * Each implementation handles the specifics of a particular warehouse provider
 * (Snowflake, Redshift, BigQuery).
 */
public interface WarehouseLoadStrategy {

    /**
     * Initialise the strategy with the connector properties.
     *
     * @param props configuration properties
     */
    void start(Map<String, String> props);

    /**
     * Load a batch of transformed records into the warehouse.
     *
     * @param records the records to load
     */
    void load(List<TransformedRecord> records);

    /**
     * Flush any buffered data to the warehouse.
     */
    void flush();

    /**
     * Check whether the strategy's underlying connection is healthy.
     *
     * @return true if healthy
     */
    boolean isHealthy();

    /**
     * Release resources held by this strategy.
     */
    void stop();
}
