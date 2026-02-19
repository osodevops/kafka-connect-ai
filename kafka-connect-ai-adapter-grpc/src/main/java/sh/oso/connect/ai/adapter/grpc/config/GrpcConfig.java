package sh.oso.connect.ai.adapter.grpc.config;

import sh.oso.connect.ai.api.config.AiConnectConfig;

import java.util.Map;

public class GrpcConfig {

    private final Map<String, String> props;

    public GrpcConfig(Map<String, String> props) {
        this.props = props;
    }

    public String target() {
        return props.getOrDefault(AiConnectConfig.GRPC_TARGET, "");
    }

    public String service() {
        return props.getOrDefault(AiConnectConfig.GRPC_SERVICE, "");
    }

    public String method() {
        return props.getOrDefault(AiConnectConfig.GRPC_METHOD, "");
    }

    public String mode() {
        return props.getOrDefault(AiConnectConfig.GRPC_MODE, "unary");
    }

    public boolean tlsEnabled() {
        return Boolean.parseBoolean(props.getOrDefault(AiConnectConfig.GRPC_TLS_ENABLED, "false"));
    }

    public String protoDescriptorPath() {
        return props.getOrDefault(AiConnectConfig.GRPC_PROTO_DESCRIPTOR_PATH, "");
    }

    public long pollIntervalMs() {
        return Long.parseLong(props.getOrDefault(AiConnectConfig.GRPC_POLL_INTERVAL_MS, "5000"));
    }

    public Map<String, String> props() {
        return props;
    }
}
