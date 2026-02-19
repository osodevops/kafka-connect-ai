package sh.oso.connect.ai.adapter.ldap;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

class LdapSinkAdapterTest {

    private LdapSinkAdapter adapter;

    @BeforeEach
    void setUp() {
        adapter = new LdapSinkAdapter();
    }

    @Test
    void typeReturnsLdap() {
        assertEquals("ldap", adapter.type());
    }

    @Test
    void configDefContainsRequiredFields() {
        var configDef = adapter.configDef();
        assertNotNull(configDef.configKeys().get("ldap.url"));
        assertNotNull(configDef.configKeys().get("ldap.bind.dn"));
        assertNotNull(configDef.configKeys().get("ldap.bind.password"));
        assertNotNull(configDef.configKeys().get("ldap.base.dn"));
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
