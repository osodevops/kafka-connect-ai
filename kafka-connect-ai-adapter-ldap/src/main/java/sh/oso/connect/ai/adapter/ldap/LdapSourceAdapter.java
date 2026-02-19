package sh.oso.connect.ai.adapter.ldap;

import com.unboundid.asn1.ASN1OctetString;
import com.unboundid.ldap.sdk.LDAPConnection;
import com.unboundid.ldap.sdk.LDAPException;
import com.unboundid.ldap.sdk.SearchRequest;
import com.unboundid.ldap.sdk.SearchResult;
import com.unboundid.ldap.sdk.SearchResultEntry;
import com.unboundid.ldap.sdk.SearchScope;
import com.unboundid.ldap.sdk.controls.ContentSyncRequestControl;
import com.unboundid.ldap.sdk.controls.ContentSyncRequestMode;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.connect.ai.adapter.ldap.config.LdapSourceConfig;
import sh.oso.connect.ai.api.adapter.SourceAdapter;
import sh.oso.connect.ai.api.config.AiConnectConfig;
import sh.oso.connect.ai.api.error.RetryableException;
import sh.oso.connect.ai.api.model.RawRecord;
import sh.oso.connect.ai.api.model.SourceOffset;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LdapSourceAdapter implements SourceAdapter {

    private static final Logger log = LoggerFactory.getLogger(LdapSourceAdapter.class);

    private LDAPConnection connection;
    private LdapSourceConfig config;
    private long lastPollTimestamp;

    @Override
    public String type() {
        return "ldap";
    }

    @Override
    public ConfigDef configDef() {
        return new ConfigDef()
                .define(AiConnectConfig.LDAP_URL, ConfigDef.Type.STRING,
                        "ldap://localhost:389", ConfigDef.Importance.HIGH,
                        "LDAP server URL")
                .define(AiConnectConfig.LDAP_BIND_DN, ConfigDef.Type.STRING,
                        "", ConfigDef.Importance.HIGH,
                        "Bind DN for authentication")
                .define(AiConnectConfig.LDAP_BIND_PASSWORD, ConfigDef.Type.PASSWORD,
                        "", ConfigDef.Importance.HIGH,
                        "Bind password")
                .define(AiConnectConfig.LDAP_BASE_DN, ConfigDef.Type.STRING,
                        ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH,
                        "Base DN for searches")
                .define(AiConnectConfig.LDAP_FILTER, ConfigDef.Type.STRING,
                        "(objectClass=*)", ConfigDef.Importance.MEDIUM,
                        "LDAP search filter")
                .define(AiConnectConfig.LDAP_SOURCE_MODE, ConfigDef.Type.STRING,
                        "polling", ConfigDef.Importance.MEDIUM,
                        "Source mode: polling, persistent_search, syncrepl, usn")
                .define(AiConnectConfig.LDAP_POLL_INTERVAL_MS, ConfigDef.Type.LONG,
                        5000L, ConfigDef.Importance.MEDIUM,
                        "Poll interval in milliseconds")
                .define(AiConnectConfig.LDAP_ATTRIBUTES, ConfigDef.Type.STRING,
                        "*", ConfigDef.Importance.LOW,
                        "Comma-separated list of attributes to return (* for all)");
    }

    @Override
    public void start(Map<String, String> props) {
        this.config = new LdapSourceConfig(props);
        this.lastPollTimestamp = 0;

        try {
            URI uri = URI.create(config.url());
            String host = uri.getHost();
            int port = uri.getPort() > 0 ? uri.getPort() : 389;
            boolean useSsl = "ldaps".equalsIgnoreCase(uri.getScheme());

            if (useSsl) {
                this.connection = new LDAPConnection(
                        new com.unboundid.util.ssl.SSLUtil().createSSLSocketFactory(),
                        host, port);
            } else {
                this.connection = new LDAPConnection(host, port);
            }

            if (!config.bindDn().isEmpty()) {
                this.connection.bind(config.bindDn(), config.bindPassword());
            }
        } catch (Exception e) {
            throw new RetryableException("Failed to connect to LDAP: " + e.getMessage(), e);
        }

        log.info("LdapSourceAdapter started: url={}, baseDn={}, mode={}",
                config.url(), config.baseDn(), config.sourceMode());
    }

    @Override
    public List<RawRecord> fetch(SourceOffset currentOffset, int maxRecords) throws InterruptedException {
        long now = System.currentTimeMillis();
        long elapsed = now - lastPollTimestamp;
        if (elapsed < config.pollIntervalMs()) {
            Thread.sleep(config.pollIntervalMs() - elapsed);
        }
        lastPollTimestamp = System.currentTimeMillis();

        try {
            return switch (config.sourceMode()) {
                case "syncrepl" -> fetchSyncRepl(currentOffset, maxRecords);
                case "usn" -> fetchUsnPolling(currentOffset, maxRecords);
                default -> fetchPolling(currentOffset, maxRecords);
            };
        } catch (Exception e) {
            throw new RetryableException("Failed to fetch from LDAP: " + e.getMessage(), e);
        }
    }

    private List<RawRecord> fetchPolling(SourceOffset currentOffset, int maxRecords) throws LDAPException {
        String[] attrs = config.attributes().isEmpty()
                ? new String[]{"*"}
                : config.attributes().toArray(new String[0]);

        String filter = config.filter();
        String lastModifyTimestamp = (String) currentOffset.offset().get("modifyTimestamp");
        if (lastModifyTimestamp != null && !lastModifyTimestamp.isEmpty()) {
            filter = "(&" + filter + "(modifyTimestamp>=" + lastModifyTimestamp + "))";
        }

        SearchRequest searchRequest = new SearchRequest(
                config.baseDn(), SearchScope.SUB, filter, attrs);
        searchRequest.setSizeLimit(maxRecords);

        SearchResult result = connection.search(searchRequest);
        return convertEntries(result.getSearchEntries());
    }

    private List<RawRecord> fetchSyncRepl(SourceOffset currentOffset, int maxRecords) throws LDAPException {
        String[] attrs = config.attributes().isEmpty()
                ? new String[]{"*"}
                : config.attributes().toArray(new String[0]);

        SearchRequest searchRequest = new SearchRequest(
                config.baseDn(), SearchScope.SUB, config.filter(), attrs);
        searchRequest.setSizeLimit(maxRecords);

        ASN1OctetString cookie = null;
        String cookieStr = (String) currentOffset.offset().get("cookie");
        if (cookieStr != null && !cookieStr.isEmpty()) {
            cookie = new ASN1OctetString(java.util.Base64.getDecoder().decode(cookieStr));
        }

        searchRequest.addControl(new ContentSyncRequestControl(
                ContentSyncRequestMode.REFRESH_ONLY, cookie, false));

        SearchResult result = connection.search(searchRequest);
        return convertEntries(result.getSearchEntries());
    }

    private List<RawRecord> fetchUsnPolling(SourceOffset currentOffset, int maxRecords) throws LDAPException {
        String[] attrs = config.attributes().isEmpty()
                ? new String[]{"*", "uSNChanged"}
                : config.attributes().toArray(new String[0]);

        String filter = config.filter();
        Object usnChanged = currentOffset.offset().get("usnChanged");
        if (usnChanged != null) {
            filter = "(&" + filter + "(uSNChanged>=" + usnChanged + "))";
        }

        SearchRequest searchRequest = new SearchRequest(
                config.baseDn(), SearchScope.SUB, filter, attrs);
        searchRequest.setSizeLimit(maxRecords);

        SearchResult result = connection.search(searchRequest);
        return convertEntries(result.getSearchEntries());
    }

    private List<RawRecord> convertEntries(List<SearchResultEntry> entries) {
        List<RawRecord> records = new ArrayList<>();
        for (SearchResultEntry entry : entries) {
            String dn = entry.getDN();
            byte[] key = dn.getBytes(StandardCharsets.UTF_8);

            Map<String, Object> entryMap = new HashMap<>();
            entryMap.put("dn", dn);
            for (com.unboundid.ldap.sdk.Attribute attr : entry.getAttributes()) {
                String[] values = attr.getValues();
                if (values.length == 1) {
                    entryMap.put(attr.getName(), values[0]);
                } else {
                    entryMap.put(attr.getName(), List.of(values));
                }
            }

            String json;
            try {
                json = new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(entryMap);
            } catch (Exception e) {
                json = entryMap.toString();
            }

            byte[] value = json.getBytes(StandardCharsets.UTF_8);

            Map<String, String> partition = Map.of("adapter", "ldap", "baseDn", config.baseDn());
            Map<String, Object> offset = new HashMap<>();
            String modifyTs = entry.getAttributeValue("modifyTimestamp");
            if (modifyTs != null) {
                offset.put("modifyTimestamp", modifyTs);
            }
            String usn = entry.getAttributeValue("uSNChanged");
            if (usn != null) {
                offset.put("usnChanged", usn);
            }

            Map<String, String> metadata = Map.of(
                    "source.dn", dn,
                    "source.baseDn", config.baseDn());

            records.add(new RawRecord(key, value, metadata, new SourceOffset(partition, offset)));
        }
        return records;
    }

    @Override
    public void commitOffset(SourceOffset offset) {
        // Offset tracked via query parameters
    }

    @Override
    public boolean isHealthy() {
        return connection != null && connection.isConnected();
    }

    @Override
    public void stop() {
        if (connection != null) {
            connection.close();
        }
        log.info("LdapSourceAdapter stopped");
    }
}
