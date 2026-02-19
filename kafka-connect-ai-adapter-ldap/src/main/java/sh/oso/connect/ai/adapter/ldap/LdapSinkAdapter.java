package sh.oso.connect.ai.adapter.ldap;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.unboundid.ldap.sdk.Attribute;
import com.unboundid.ldap.sdk.LDAPConnection;
import com.unboundid.ldap.sdk.LDAPException;
import com.unboundid.ldap.sdk.Modification;
import com.unboundid.ldap.sdk.ModificationType;
import com.unboundid.ldap.sdk.ResultCode;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.connect.ai.adapter.ldap.config.LdapSinkConfig;
import sh.oso.connect.ai.api.adapter.SinkAdapter;
import sh.oso.connect.ai.api.config.AiConnectConfig;
import sh.oso.connect.ai.api.error.RetryableException;
import sh.oso.connect.ai.api.model.TransformedRecord;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class LdapSinkAdapter implements SinkAdapter {

    private static final Logger log = LoggerFactory.getLogger(LdapSinkAdapter.class);

    private final ObjectMapper objectMapper = new ObjectMapper();

    private LDAPConnection connection;
    private LdapSinkConfig config;

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
                        "Base DN for entries");
    }

    @Override
    public void start(Map<String, String> props) {
        this.config = new LdapSinkConfig(props);

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

        log.info("LdapSinkAdapter started: url={}, baseDn={}", config.url(), config.baseDn());
    }

    @Override
    public void write(List<TransformedRecord> records) {
        if (records.isEmpty()) {
            return;
        }

        for (TransformedRecord record : records) {
            try {
                String json = new String(record.value(), StandardCharsets.UTF_8);
                JsonNode node = objectMapper.readTree(json);

                String dn = node.has("dn") ? node.get("dn").asText() : null;
                if (dn == null || dn.isEmpty()) {
                    log.warn("Skipping record without 'dn' field");
                    continue;
                }

                List<Attribute> attributes = new ArrayList<>();
                Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
                while (fields.hasNext()) {
                    Map.Entry<String, JsonNode> field = fields.next();
                    if ("dn".equals(field.getKey())) {
                        continue;
                    }
                    attributes.add(new Attribute(field.getKey(), field.getValue().asText()));
                }

                // Upsert pattern: try modify, catch NO_SUCH_OBJECT -> add
                try {
                    List<Modification> mods = new ArrayList<>();
                    for (Attribute attr : attributes) {
                        mods.add(new Modification(ModificationType.REPLACE,
                                attr.getName(), attr.getValues()));
                    }
                    connection.modify(dn, mods);
                } catch (LDAPException e) {
                    if (e.getResultCode() == ResultCode.NO_SUCH_OBJECT) {
                        connection.add(dn, attributes);
                    } else {
                        throw e;
                    }
                }
            } catch (Exception e) {
                throw new RetryableException("Failed to write to LDAP: " + e.getMessage(), e);
            }
        }

        log.debug("Wrote {} records to LDAP", records.size());
    }

    @Override
    public void flush() {
        // LDAP operations are synchronous
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
        log.info("LdapSinkAdapter stopped");
    }
}
