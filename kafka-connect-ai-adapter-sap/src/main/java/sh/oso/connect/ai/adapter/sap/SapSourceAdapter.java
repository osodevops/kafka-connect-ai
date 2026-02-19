package sh.oso.connect.ai.adapter.sap;

import com.sap.conn.jco.JCoDestination;
import com.sap.conn.jco.JCoDestinationManager;
import com.sap.conn.jco.JCoException;
import com.sap.conn.jco.JCoFunction;
import com.sap.conn.jco.JCoParameterList;
import com.sap.conn.jco.JCoTable;
import com.sap.conn.jco.ext.DestinationDataProvider;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.connect.ai.adapter.sap.config.SapSourceConfig;
import sh.oso.connect.ai.api.adapter.SourceAdapter;
import sh.oso.connect.ai.api.config.AiConnectConfig;
import sh.oso.connect.ai.api.error.RetryableException;
import sh.oso.connect.ai.api.model.RawRecord;
import sh.oso.connect.ai.api.model.SourceOffset;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class SapSourceAdapter implements SourceAdapter {

    private static final Logger log = LoggerFactory.getLogger(SapSourceAdapter.class);
    private static final String DESTINATION_NAME = "KAFKA_CONNECT_AI";

    private SapSourceConfig config;
    private JCoDestination destination;
    private long lastPollTimestamp;
    private volatile boolean running;

    @Override
    public String type() {
        return "sap";
    }

    @Override
    public ConfigDef configDef() {
        return new ConfigDef()
                .define(AiConnectConfig.SAP_ASHOST, ConfigDef.Type.STRING,
                        ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH,
                        "SAP application server host")
                .define(AiConnectConfig.SAP_SYSNR, ConfigDef.Type.STRING,
                        "00", ConfigDef.Importance.HIGH,
                        "SAP system number")
                .define(AiConnectConfig.SAP_CLIENT, ConfigDef.Type.STRING,
                        ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH,
                        "SAP client number")
                .define(AiConnectConfig.SAP_USER, ConfigDef.Type.STRING,
                        ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH,
                        "SAP user name")
                .define(AiConnectConfig.SAP_PASSWORD, ConfigDef.Type.PASSWORD,
                        ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH,
                        "SAP password")
                .define(AiConnectConfig.SAP_LANG, ConfigDef.Type.STRING,
                        "EN", ConfigDef.Importance.LOW,
                        "SAP language")
                .define(AiConnectConfig.SAP_MODE, ConfigDef.Type.STRING,
                        "rfc", ConfigDef.Importance.MEDIUM,
                        "Mode: rfc (polling) or idoc (push)")
                .define(AiConnectConfig.SAP_FUNCTION, ConfigDef.Type.STRING,
                        ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH,
                        "RFC/BAPI function module name")
                .define(AiConnectConfig.SAP_POLL_INTERVAL_MS, ConfigDef.Type.LONG,
                        10000L, ConfigDef.Importance.MEDIUM,
                        "Poll interval in milliseconds")
                .define(AiConnectConfig.SAP_GATEWAY_HOST, ConfigDef.Type.STRING,
                        "", ConfigDef.Importance.MEDIUM,
                        "SAP gateway host (for IDoc mode)")
                .define(AiConnectConfig.SAP_GATEWAY_SERVICE, ConfigDef.Type.STRING,
                        "", ConfigDef.Importance.MEDIUM,
                        "SAP gateway service (for IDoc mode)")
                .define(AiConnectConfig.SAP_PROGRAM_ID, ConfigDef.Type.STRING,
                        "", ConfigDef.Importance.MEDIUM,
                        "Registered program ID (for IDoc mode)");
    }

    @Override
    public void start(Map<String, String> props) {
        this.config = new SapSourceConfig(props);
        this.lastPollTimestamp = 0;
        this.running = true;

        try {
            Properties destProps = new Properties();
            destProps.setProperty(DestinationDataProvider.JCO_ASHOST, config.ashost());
            destProps.setProperty(DestinationDataProvider.JCO_SYSNR, config.sysnr());
            destProps.setProperty(DestinationDataProvider.JCO_CLIENT, config.client());
            destProps.setProperty(DestinationDataProvider.JCO_USER, config.user());
            destProps.setProperty(DestinationDataProvider.JCO_PASSWD, config.password());
            destProps.setProperty(DestinationDataProvider.JCO_LANG, config.lang());

            // Register custom destination provider
            SapDestinationProvider provider = new SapDestinationProvider(DESTINATION_NAME, destProps);
            try {
                com.sap.conn.jco.ext.Environment.registerDestinationDataProvider(provider);
            } catch (IllegalStateException e) {
                // Provider already registered
                log.debug("Destination provider already registered");
            }

            this.destination = JCoDestinationManager.getDestination(DESTINATION_NAME);
            destination.ping();
        } catch (JCoException e) {
            throw new RetryableException("Failed to connect to SAP: " + e.getMessage(), e);
        }

        log.info("SapSourceAdapter started: host={}, client={}, mode={}, function={}",
                config.ashost(), config.client(), config.mode(), config.function());
    }

    @Override
    public List<RawRecord> fetch(SourceOffset currentOffset, int maxRecords) throws InterruptedException {
        long now = System.currentTimeMillis();
        long elapsed = now - lastPollTimestamp;
        if (elapsed < config.pollIntervalMs()) {
            Thread.sleep(config.pollIntervalMs() - elapsed);
        }
        lastPollTimestamp = System.currentTimeMillis();

        if ("idoc".equals(config.mode())) {
            return fetchIdoc(currentOffset, maxRecords);
        }
        return fetchRfc(currentOffset, maxRecords);
    }

    private List<RawRecord> fetchRfc(SourceOffset currentOffset, int maxRecords) {
        try {
            JCoFunction function = destination.getRepository()
                    .getFunction(config.function());

            if (function == null) {
                throw new RetryableException("Function not found: " + config.function());
            }

            function.execute(destination);

            List<RawRecord> records = new ArrayList<>();
            JCoParameterList exportParams = function.getExportParameterList();
            JCoParameterList tableParams = function.getTableParameterList();

            if (tableParams != null) {
                for (int i = 0; i < tableParams.getFieldCount() && records.size() < maxRecords; i++) {
                    if (tableParams.getField(i).isTable()) {
                        JCoTable table = tableParams.getTable(i);
                        for (int row = 0; row < table.getNumRows() && records.size() < maxRecords; row++) {
                            table.setRow(row);
                            Map<String, Object> rowData = new HashMap<>();
                            for (int col = 0; col < table.getFieldCount(); col++) {
                                rowData.put(table.getMetaData().getName(col),
                                        table.getString(col));
                            }
                            String json = new com.fasterxml.jackson.databind.ObjectMapper()
                                    .writeValueAsString(rowData);
                            byte[] value = json.getBytes(StandardCharsets.UTF_8);

                            Map<String, String> partition = Map.of(
                                    "adapter", "sap", "function", config.function());
                            Map<String, Object> offset = Map.of("row", row);
                            Map<String, String> metadata = Map.of(
                                    "source.function", config.function(),
                                    "source.table", tableParams.getMetaData().getName(i));

                            records.add(new RawRecord(null, value, metadata,
                                    new SourceOffset(partition, offset)));
                        }
                    }
                }
            }

            if (records.isEmpty() && exportParams != null) {
                Map<String, Object> exportData = new HashMap<>();
                for (int i = 0; i < exportParams.getFieldCount(); i++) {
                    exportData.put(exportParams.getMetaData().getName(i),
                            exportParams.getString(i));
                }
                String json = new com.fasterxml.jackson.databind.ObjectMapper()
                        .writeValueAsString(exportData);
                byte[] value = json.getBytes(StandardCharsets.UTF_8);

                Map<String, String> partition = Map.of(
                        "adapter", "sap", "function", config.function());
                Map<String, String> metadata = Map.of(
                        "source.function", config.function());

                records.add(new RawRecord(null, value, metadata,
                        new SourceOffset(partition, Map.of())));
            }

            log.debug("Fetched {} records from SAP function {}", records.size(), config.function());
            return records;
        } catch (Exception e) {
            throw new RetryableException("Failed to fetch from SAP: " + e.getMessage(), e);
        }
    }

    private List<RawRecord> fetchIdoc(SourceOffset currentOffset, int maxRecords) {
        // IDoc receiver mode requires JCoIDocServer which is push-based.
        // For the initial implementation, this returns empty and logs a warning.
        // Full IDoc support requires a background thread with JCoIDocServer.
        log.warn("IDoc mode is not yet fully implemented. Use RFC mode for polling.");
        return List.of();
    }

    @Override
    public void commitOffset(SourceOffset offset) {
        // RFC polling is stateless per call
    }

    @Override
    public boolean isHealthy() {
        if (destination == null) {
            return false;
        }
        try {
            destination.ping();
            return true;
        } catch (JCoException e) {
            return false;
        }
    }

    @Override
    public void stop() {
        running = false;
        log.info("SapSourceAdapter stopped");
    }
}
