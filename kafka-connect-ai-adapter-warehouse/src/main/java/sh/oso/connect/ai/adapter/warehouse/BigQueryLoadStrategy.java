package sh.oso.connect.ai.adapter.warehouse;

import com.google.api.core.ApiFuture;
import com.google.cloud.bigquery.storage.v1.*;
import com.google.protobuf.Descriptors;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.connect.ai.api.config.AiConnectConfig;
import sh.oso.connect.ai.api.error.NonRetryableException;
import sh.oso.connect.ai.api.error.RetryableException;
import sh.oso.connect.ai.api.model.TransformedRecord;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * BigQuery load strategy using the BigQuery Storage Write API.
 * <p>
 * Records are streamed as JSON rows via the default stream for low-latency writes.
 */
public class BigQueryLoadStrategy implements WarehouseLoadStrategy {

    private static final Logger log = LoggerFactory.getLogger(BigQueryLoadStrategy.class);

    private BigQueryWriteClient writeClient;
    private JsonStreamWriter streamWriter;
    private String project;
    private String dataset;
    private String table;
    private boolean started;

    @Override
    public void start(Map<String, String> props) {
        this.project = props.getOrDefault(AiConnectConfig.BIGQUERY_PROJECT, "");
        this.dataset = props.getOrDefault(AiConnectConfig.BIGQUERY_DATASET, "");
        this.table = props.getOrDefault(AiConnectConfig.BIGQUERY_TABLE, "");
        String credentialsPath = props.getOrDefault(AiConnectConfig.BIGQUERY_CREDENTIALS_PATH, "");

        if (project.isEmpty() || dataset.isEmpty() || table.isEmpty()) {
            throw new NonRetryableException(
                    "BigQuery configuration incomplete: bigquery.project, bigquery.dataset, " +
                    "and bigquery.table are required");
        }

        // Set credentials path if provided (GOOGLE_APPLICATION_CREDENTIALS env var is the alternative)
        if (!credentialsPath.isEmpty()) {
            System.setProperty("GOOGLE_APPLICATION_CREDENTIALS", credentialsPath);
        }

        try {
            this.writeClient = BigQueryWriteClient.create();

            TableName tableName = TableName.of(project, dataset, table);
            String defaultStream = String.format(
                    "projects/%s/datasets/%s/tables/%s/streams/_default",
                    project, dataset, table
            );

            // Get the table schema from BigQuery
            GetWriteStreamRequest getStreamRequest = GetWriteStreamRequest.newBuilder()
                    .setName(defaultStream)
                    .setView(WriteStreamView.FULL)
                    .build();
            WriteStream writeStream = writeClient.getWriteStream(getStreamRequest);
            TableSchema tableSchema = writeStream.getTableSchema();

            this.streamWriter = JsonStreamWriter.newBuilder(defaultStream, tableSchema)
                    .build();

            this.started = true;

            log.info("BigQueryLoadStrategy started: project={}, dataset={}, table={}",
                    project, dataset, table);

        } catch (Exception e) {
            throw new RetryableException("Failed to initialise BigQuery Storage Write API: " + e.getMessage(), e);
        }
    }

    @Override
    public void load(List<TransformedRecord> records) {
        if (records.isEmpty()) {
            return;
        }

        try {
            JSONArray jsonArray = new JSONArray();
            for (TransformedRecord record : records) {
                if (record.value() != null) {
                    String json = new String(record.value(), StandardCharsets.UTF_8);
                    jsonArray.put(new JSONObject(json));
                }
            }

            ApiFuture<AppendRowsResponse> future = streamWriter.append(jsonArray);
            AppendRowsResponse response = future.get();

            if (response.hasError()) {
                throw new RetryableException(
                        "BigQuery append failed: " + response.getError().getMessage());
            }

            log.debug("Loaded {} records into BigQuery {}.{}.{}", records.size(), project, dataset, table);

        } catch (RetryableException e) {
            throw e;
        } catch (Exception e) {
            throw new RetryableException("Failed to load records into BigQuery: " + e.getMessage(), e);
        }
    }

    @Override
    public void flush() {
        // The default stream auto-commits, no explicit flush needed
    }

    @Override
    public boolean isHealthy() {
        return started && writeClient != null && !writeClient.isShutdown();
    }

    @Override
    public void stop() {
        if (streamWriter != null) {
            streamWriter.close();
            streamWriter = null;
        }
        if (writeClient != null) {
            writeClient.close();
            writeClient = null;
        }
        started = false;
        log.info("BigQueryLoadStrategy stopped");
    }
}
