package sh.oso.connect.ai.adapter.ldap;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class LdapSourceAdapterTest {

    private LdapSourceAdapter adapter;

    @BeforeEach
    void setUp() {
        adapter = new LdapSourceAdapter();
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
        assertNotNull(configDef.configKeys().get("ldap.filter"));
        assertNotNull(configDef.configKeys().get("ldap.source.mode"));
        assertNotNull(configDef.configKeys().get("ldap.poll.interval.ms"));
        assertNotNull(configDef.configKeys().get("ldap.attributes"));
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
