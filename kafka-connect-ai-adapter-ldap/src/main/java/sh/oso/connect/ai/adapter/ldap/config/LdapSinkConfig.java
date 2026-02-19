package sh.oso.connect.ai.adapter.ldap.config;

import sh.oso.connect.ai.api.config.AiConnectConfig;

import java.util.Map;

public class LdapSinkConfig {

    private final Map<String, String> props;

    public LdapSinkConfig(Map<String, String> props) {
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
}
