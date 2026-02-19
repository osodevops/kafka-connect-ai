package sh.oso.connect.ai.adapter.grpc;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sh.oso.connect.ai.api.config.AiConnectConfig;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

class GrpcSinkAdapterTest {

    private GrpcSinkAdapter adapter;

    @BeforeEach
    void setUp() {
        adapter = new GrpcSinkAdapter();
    }

    @Test
    void typeReturnsGrpc() {
        assertEquals("grpc", adapter.type());
    }

    @Test
    void configDefContainsRequiredFields() {
        var configDef = adapter.configDef();
        assertNotNull(configDef.configKeys().get(AiConnectConfig.GRPC_TARGET));
        assertNotNull(configDef.configKeys().get(AiConnectConfig.GRPC_SERVICE));
        assertNotNull(configDef.configKeys().get(AiConnectConfig.GRPC_METHOD));
        assertNotNull(configDef.configKeys().get(AiConnectConfig.GRPC_TLS_ENABLED));
        assertNotNull(configDef.configKeys().get(AiConnectConfig.GRPC_PROTO_DESCRIPTOR_PATH));
    }

    @Test
    void writeEmptyListIsNoOp() {
        assertDoesNotThrow(() -> adapter.write(Collections.emptyList()));
    }

    @Test
    void isHealthyReturnsFalseBeforeStart() {
        assertFalse(adapter.isHealthy());
    }

    @Test
    void stopIsIdempotent() {
        assertDoesNotThrow(() -> adapter.stop());
        assertDoesNotThrow(() -> adapter.stop());
    }
}
