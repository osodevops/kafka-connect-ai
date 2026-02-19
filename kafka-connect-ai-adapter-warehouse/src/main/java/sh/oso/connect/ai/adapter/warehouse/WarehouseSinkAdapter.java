package sh.oso.connect.ai.adapter.warehouse;

import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.connect.ai.adapter.warehouse.config.WarehouseSinkConfig;
import sh.oso.connect.ai.api.adapter.SinkAdapter;
import sh.oso.connect.ai.api.config.AiConnectConfig;
import sh.oso.connect.ai.api.error.NonRetryableException;
import sh.oso.connect.ai.api.model.TransformedRecord;

import java.util.List;
import java.util.Map;

/**
 * Sink adapter for cloud data warehouses.
 * <p>
 * Delegates to a {@link WarehouseLoadStrategy} based on the configured
 * {@code warehouse.provider}:
 * <ul>
 *   <li>{@code snowflake} - Snowflake via JDBC with COPY INTO</li>
 *   <li>{@code redshift} - Redshift via S3 staging and COPY</li>
 *   <li>{@code bigquery} - BigQuery via Storage Write API</li>
 * </ul>
 */
public class WarehouseSinkAdapter implements SinkAdapter {

    private static final Logger log = LoggerFactory.getLogger(WarehouseSinkAdapter.class);

    private WarehouseLoadStrategy strategy;
    private WarehouseSinkConfig config;
    private boolean started;

    @Override
    public String type() {
        return "warehouse";
    }

    @Override
    public ConfigDef configDef() {
        return new ConfigDef()
                // General warehouse config
                .define(AiConnectConfig.WAREHOUSE_PROVIDER, ConfigDef.Type.STRING,
                        ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH,
                        "Warehouse provider: snowflake, redshift, or bigquery")
                .define(AiConnectConfig.WAREHOUSE_BATCH_SIZE, ConfigDef.Type.INT,
                        1000, ConfigDef.Importance.MEDIUM,
                        "Batch size for warehouse loads")
                // Snowflake
                .define(AiConnectConfig.SNOWFLAKE_URL, ConfigDef.Type.STRING,
                        "", ConfigDef.Importance.HIGH,
                        "Snowflake JDBC URL")
                .define(AiConnectConfig.SNOWFLAKE_USER, ConfigDef.Type.STRING,
                        "", ConfigDef.Importance.HIGH,
                        "Snowflake username")
                .define(AiConnectConfig.SNOWFLAKE_PRIVATE_KEY, ConfigDef.Type.PASSWORD,
                        "", ConfigDef.Importance.HIGH,
                        "Snowflake private key for key-pair authentication")
                .define(AiConnectConfig.SNOWFLAKE_DATABASE, ConfigDef.Type.STRING,
                        "", ConfigDef.Importance.HIGH,
                        "Snowflake database name")
                .define(AiConnectConfig.SNOWFLAKE_SCHEMA, ConfigDef.Type.STRING,
                        "PUBLIC", ConfigDef.Importance.MEDIUM,
                        "Snowflake schema name")
                .define(AiConnectConfig.SNOWFLAKE_TABLE, ConfigDef.Type.STRING,
                        "", ConfigDef.Importance.HIGH,
                        "Snowflake target table name")
                .define(AiConnectConfig.SNOWFLAKE_WAREHOUSE, ConfigDef.Type.STRING,
                        "", ConfigDef.Importance.HIGH,
                        "Snowflake virtual warehouse name")
                .define(AiConnectConfig.SNOWFLAKE_ROLE, ConfigDef.Type.STRING,
                        "", ConfigDef.Importance.MEDIUM,
                        "Snowflake role to use")
                // Redshift
                .define(AiConnectConfig.REDSHIFT_JDBC_URL, ConfigDef.Type.STRING,
                        "", ConfigDef.Importance.HIGH,
                        "Redshift JDBC URL")
                .define(AiConnectConfig.REDSHIFT_USER, ConfigDef.Type.STRING,
                        "", ConfigDef.Importance.HIGH,
                        "Redshift username")
                .define(AiConnectConfig.REDSHIFT_PASSWORD, ConfigDef.Type.PASSWORD,
                        "", ConfigDef.Importance.HIGH,
                        "Redshift password")
                .define(AiConnectConfig.REDSHIFT_TABLE, ConfigDef.Type.STRING,
                        "", ConfigDef.Importance.HIGH,
                        "Redshift target table name")
                .define(AiConnectConfig.REDSHIFT_S3_BUCKET, ConfigDef.Type.STRING,
                        "", ConfigDef.Importance.HIGH,
                        "S3 bucket for Redshift staging")
                .define(AiConnectConfig.REDSHIFT_S3_REGION, ConfigDef.Type.STRING,
                        "us-east-1", ConfigDef.Importance.MEDIUM,
                        "AWS region for the S3 staging bucket")
                .define(AiConnectConfig.REDSHIFT_IAM_ROLE, ConfigDef.Type.STRING,
                        "", ConfigDef.Importance.HIGH,
                        "IAM role ARN for Redshift COPY command")
                // BigQuery
                .define(AiConnectConfig.BIGQUERY_PROJECT, ConfigDef.Type.STRING,
                        "", ConfigDef.Importance.HIGH,
                        "Google Cloud project ID")
                .define(AiConnectConfig.BIGQUERY_DATASET, ConfigDef.Type.STRING,
                        "", ConfigDef.Importance.HIGH,
                        "BigQuery dataset name")
                .define(AiConnectConfig.BIGQUERY_TABLE, ConfigDef.Type.STRING,
                        "", ConfigDef.Importance.HIGH,
                        "BigQuery target table name")
                .define(AiConnectConfig.BIGQUERY_CREDENTIALS_PATH, ConfigDef.Type.STRING,
                        "", ConfigDef.Importance.HIGH,
                        "Path to Google Cloud service account credentials JSON file");
    }

    @Override
    public void start(Map<String, String> props) {
        this.config = new WarehouseSinkConfig(props);

        String provider = config.provider();
        if (provider.isEmpty()) {
            throw new NonRetryableException("warehouse.provider is required");
        }

        this.strategy = createStrategy(provider);
        this.strategy.start(props);
        this.started = true;

        log.info("WarehouseSinkAdapter started with provider={}", provider);
    }

    @Override
    public void write(List<TransformedRecord> records) {
        if (records.isEmpty()) {
            return;
        }
        strategy.load(records);
    }

    @Override
    public void flush() {
        if (strategy != null) {
            strategy.flush();
        }
    }

    @Override
    public boolean isHealthy() {
        return started && strategy != null && strategy.isHealthy();
    }

    @Override
    public void stop() {
        if (strategy != null) {
            strategy.stop();
            strategy = null;
        }
        started = false;
        log.info("WarehouseSinkAdapter stopped");
    }

    private WarehouseLoadStrategy createStrategy(String provider) {
        return switch (provider.toLowerCase()) {
            case "snowflake" -> new SnowflakeLoadStrategy();
            case "redshift" -> new RedshiftLoadStrategy();
            case "bigquery" -> new BigQueryLoadStrategy();
            default -> throw new NonRetryableException(
                    "Unknown warehouse provider: " + provider +
                    ". Supported values: snowflake, redshift, bigquery");
        };
    }

    // Visible for testing
    WarehouseLoadStrategy getStrategy() {
        return strategy;
    }
}
