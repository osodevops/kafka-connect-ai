package sh.oso.connect.ai.adapter.sap.config;

import sh.oso.connect.ai.api.config.AiConnectConfig;

import java.util.Map;

public class SapSourceConfig {

    private final Map<String, String> props;

    public SapSourceConfig(Map<String, String> props) {
        this.props = props;
    }

    public String ashost() {
        return props.get(AiConnectConfig.SAP_ASHOST);
    }

    public String sysnr() {
        return props.getOrDefault(AiConnectConfig.SAP_SYSNR, "00");
    }

    public String client() {
        return props.get(AiConnectConfig.SAP_CLIENT);
    }

    public String user() {
        return props.get(AiConnectConfig.SAP_USER);
    }

    public String password() {
        return props.get(AiConnectConfig.SAP_PASSWORD);
    }

    public String lang() {
        return props.getOrDefault(AiConnectConfig.SAP_LANG, "EN");
    }

    public String mode() {
        return props.getOrDefault(AiConnectConfig.SAP_MODE, "rfc");
    }

    public String function() {
        return props.get(AiConnectConfig.SAP_FUNCTION);
    }

    public long pollIntervalMs() {
        return Long.parseLong(props.getOrDefault(AiConnectConfig.SAP_POLL_INTERVAL_MS, "10000"));
    }

    public String gatewayHost() {
        return props.getOrDefault(AiConnectConfig.SAP_GATEWAY_HOST, "");
    }

    public String gatewayService() {
        return props.getOrDefault(AiConnectConfig.SAP_GATEWAY_SERVICE, "");
    }

    public String programId() {
        return props.getOrDefault(AiConnectConfig.SAP_PROGRAM_ID, "");
    }
}
