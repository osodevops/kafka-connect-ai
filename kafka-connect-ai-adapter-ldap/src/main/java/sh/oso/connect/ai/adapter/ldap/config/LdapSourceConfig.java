package sh.oso.connect.ai.adapter.ldap.config;

import sh.oso.connect.ai.api.config.AiConnectConfig;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class LdapSourceConfig {

    private final Map<String, String> props;

    public LdapSourceConfig(Map<String, String> props) {
        this.props = props;
    }

    public String url() {
        return props.getOrDefault(AiConnectConfig.LDAP_URL, "ldap://localhost:389");
    }

    public String bindDn() {
        return props.getOrDefault(AiConnectConfig.LDAP_BIND_DN, "");
    }

    public String bindPassword() {
        return props.getOrDefault(AiConnectConfig.LDAP_BIND_PASSWORD, "");
    }

    public String baseDn() {
        return props.get(AiConnectConfig.LDAP_BASE_DN);
    }

    public String filter() {
        return props.getOrDefault(AiConnectConfig.LDAP_FILTER, "(objectClass=*)");
    }

    public String sourceMode() {
        return props.getOrDefault(AiConnectConfig.LDAP_SOURCE_MODE, "polling");
    }

    public long pollIntervalMs() {
        return Long.parseLong(props.getOrDefault(AiConnectConfig.LDAP_POLL_INTERVAL_MS, "5000"));
    }

    public List<String> attributes() {
        String attrs = props.getOrDefault(AiConnectConfig.LDAP_ATTRIBUTES, "*");
        if ("*".equals(attrs)) {
            return Collections.emptyList();
        }
        return Arrays.asList(attrs.split(","));
    }
}
