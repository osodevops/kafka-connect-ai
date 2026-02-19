package sh.oso.connect.ai.adapter.warehouse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.connect.ai.api.config.AiConnectConfig;
import sh.oso.connect.ai.api.error.NonRetryableException;
import sh.oso.connect.ai.api.error.RetryableException;
import sh.oso.connect.ai.api.model.TransformedRecord;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Redshift load strategy using S3 staging and JDBC COPY.
 * <p>
 * Records are serialised as NDJSON, uploaded to an S3 staging bucket,
 * then loaded into Redshift via a COPY command executed over JDBC.
 */
public class RedshiftLoadStrategy implements WarehouseLoadStrategy {

    private static final Logger log = LoggerFactory.getLogger(RedshiftLoadStrategy.class);

    private Connection connection;
    private S3Client s3Client;
    private String table;
    private String s3Bucket;
    private String iamRole;
    private String s3Region;
    private boolean started;

    @Override
    public void start(Map<String, String> props) {
        String jdbcUrl = props.getOrDefault(AiConnectConfig.REDSHIFT_JDBC_URL, "");
        String user = props.getOrDefault(AiConnectConfig.REDSHIFT_USER, "");
        String password = props.getOrDefault(AiConnectConfig.REDSHIFT_PASSWORD, "");
        this.table = props.getOrDefault(AiConnectConfig.REDSHIFT_TABLE, "");
        this.s3Bucket = props.getOrDefault(AiConnectConfig.REDSHIFT_S3_BUCKET, "");
        this.s3Region = props.getOrDefault(AiConnectConfig.REDSHIFT_S3_REGION, "us-east-1");
        this.iamRole = props.getOrDefault(AiConnectConfig.REDSHIFT_IAM_ROLE, "");

        if (jdbcUrl.isEmpty() || table.isEmpty() || s3Bucket.isEmpty() || iamRole.isEmpty()) {
            throw new NonRetryableException(
                    "Redshift configuration incomplete: redshift.jdbc.url, redshift.table, " +
                    "redshift.s3.bucket, and redshift.iam.role are required");
        }

        try {
            this.connection = DriverManager.getConnection(jdbcUrl, user, password);
        } catch (SQLException e) {
            throw new RetryableException("Failed to connect to Redshift: " + e.getMessage(), e);
        }

        this.s3Client = S3Client.builder()
                .region(Region.of(s3Region))
                .build();

        this.started = true;

        log.info("RedshiftLoadStrategy started: table={}, s3Bucket={}, region={}",
                table, s3Bucket, s3Region);
    }

    @Override
    public void load(List<TransformedRecord> records) {
        if (records.isEmpty()) {
            return;
        }

        String s3Key = "kafka-connect-ai/staging/" + UUID.randomUUID() + ".json";

        try {
            // Build NDJSON payload
            StringBuilder ndjson = new StringBuilder();
            for (TransformedRecord record : records) {
                if (record.value() != null) {
                    ndjson.append(new String(record.value(), StandardCharsets.UTF_8)).append("\n");
                }
            }

            // Upload to S3
            s3Client.putObject(
                    PutObjectRequest.builder()
                            .bucket(s3Bucket)
                            .key(s3Key)
                            .contentType("application/json")
                            .build(),
                    RequestBody.fromString(ndjson.toString())
            );

            log.debug("Staged {} records to s3://{}/{}", records.size(), s3Bucket, s3Key);

            // Execute COPY command via JDBC
            String copySql = String.format(
                    "COPY %s FROM 's3://%s/%s' IAM_ROLE '%s' FORMAT AS JSON 'auto' REGION '%s'",
                    table, s3Bucket, s3Key, iamRole, s3Region
            );

            try (Statement stmt = connection.createStatement()) {
                stmt.execute(copySql);
            }

            log.debug("Loaded {} records into Redshift table {}", records.size(), table);

            // Clean up staging file
            cleanupS3(s3Key);

        } catch (SQLException e) {
            cleanupS3(s3Key);
            throw new RetryableException("Failed to COPY into Redshift: " + e.getMessage(), e);
        } catch (Exception e) {
            cleanupS3(s3Key);
            throw new RetryableException("Failed to stage records to S3: " + e.getMessage(), e);
        }
    }

    @Override
    public void flush() {
        // Writes are committed within load()
    }

    @Override
    public boolean isHealthy() {
        if (connection == null) {
            return false;
        }
        try {
            return connection.isValid(5);
        } catch (SQLException e) {
            return false;
        }
    }

    @Override
    public void stop() {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                log.warn("Error closing Redshift connection: {}", e.getMessage());
            }
            connection = null;
        }
        if (s3Client != null) {
            s3Client.close();
            s3Client = null;
        }
        started = false;
        log.info("RedshiftLoadStrategy stopped");
    }

    private void cleanupS3(String s3Key) {
        try {
            s3Client.deleteObject(DeleteObjectRequest.builder()
                    .bucket(s3Bucket)
                    .key(s3Key)
                    .build());
        } catch (Exception e) {
            log.warn("Failed to clean up S3 staging file s3://{}/{}: {}", s3Bucket, s3Key, e.getMessage());
        }
    }
}
