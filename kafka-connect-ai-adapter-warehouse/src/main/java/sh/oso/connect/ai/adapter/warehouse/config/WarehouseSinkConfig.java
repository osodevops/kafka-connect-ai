package sh.oso.connect.ai.adapter.warehouse.config;

import sh.oso.connect.ai.api.config.AiConnectConfig;

import java.util.Map;

/**
 * Configuration wrapper for the warehouse sink adapter.
 * Provides typed access to all warehouse-related configuration properties.
 */
public class WarehouseSinkConfig {

    private final Map<String, String> props;

    public WarehouseSinkConfig(Map<String, String> props) {
        this.props = props;
    }

    // -- General --

    public String provider() {
        return props.getOrDefault(AiConnectConfig.WAREHOUSE_PROVIDER, "");
    }

    public int batchSize() {
        return Integer.parseInt(props.getOrDefault(AiConnectConfig.WAREHOUSE_BATCH_SIZE, "1000"));
    }

    // -- Snowflake --

    public String snowflakeUrl() {
        return props.getOrDefault(AiConnectConfig.SNOWFLAKE_URL, "");
    }

    public String snowflakeUser() {
        return props.getOrDefault(AiConnectConfig.SNOWFLAKE_USER, "");
    }

    public String snowflakePrivateKey() {
        return props.getOrDefault(AiConnectConfig.SNOWFLAKE_PRIVATE_KEY, "");
    }

    public String snowflakeDatabase() {
        return props.getOrDefault(AiConnectConfig.SNOWFLAKE_DATABASE, "");
    }

    public String snowflakeSchema() {
        return props.getOrDefault(AiConnectConfig.SNOWFLAKE_SCHEMA, "PUBLIC");
    }

    public String snowflakeTable() {
        return props.getOrDefault(AiConnectConfig.SNOWFLAKE_TABLE, "");
    }

    public String snowflakeWarehouse() {
        return props.getOrDefault(AiConnectConfig.SNOWFLAKE_WAREHOUSE, "");
    }

    public String snowflakeRole() {
        return props.getOrDefault(AiConnectConfig.SNOWFLAKE_ROLE, "");
    }

    // -- Redshift --

    public String redshiftJdbcUrl() {
        return props.getOrDefault(AiConnectConfig.REDSHIFT_JDBC_URL, "");
    }

    public String redshiftUser() {
        return props.getOrDefault(AiConnectConfig.REDSHIFT_USER, "");
    }

    public String redshiftPassword() {
        return props.getOrDefault(AiConnectConfig.REDSHIFT_PASSWORD, "");
    }

    public String redshiftTable() {
        return props.getOrDefault(AiConnectConfig.REDSHIFT_TABLE, "");
    }

    public String redshiftS3Bucket() {
        return props.getOrDefault(AiConnectConfig.REDSHIFT_S3_BUCKET, "");
    }

    public String redshiftS3Region() {
        return props.getOrDefault(AiConnectConfig.REDSHIFT_S3_REGION, "us-east-1");
    }

    public String redshiftIamRole() {
        return props.getOrDefault(AiConnectConfig.REDSHIFT_IAM_ROLE, "");
    }

    // -- BigQuery --

    public String bigqueryProject() {
        return props.getOrDefault(AiConnectConfig.BIGQUERY_PROJECT, "");
    }

    public String bigqueryDataset() {
        return props.getOrDefault(AiConnectConfig.BIGQUERY_DATASET, "");
    }

    public String bigqueryTable() {
        return props.getOrDefault(AiConnectConfig.BIGQUERY_TABLE, "");
    }

    public String bigqueryCredentialsPath() {
        return props.getOrDefault(AiConnectConfig.BIGQUERY_CREDENTIALS_PATH, "");
    }
}
