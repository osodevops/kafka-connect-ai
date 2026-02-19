package sh.oso.connect.ai.adapter.sap;

import com.sap.conn.jco.ext.DestinationDataEventListener;
import com.sap.conn.jco.ext.DestinationDataProvider;

import java.util.Properties;

public class SapDestinationProvider implements DestinationDataProvider {

    private final String destinationName;
    private final Properties properties;

    public SapDestinationProvider(String destinationName, Properties properties) {
        this.destinationName = destinationName;
        this.properties = properties;
    }

    @Override
    public Properties getDestinationProperties(String name) {
        if (destinationName.equals(name)) {
            return properties;
        }
        return null;
    }

    @Override
    public void setDestinationDataEventListener(DestinationDataEventListener listener) {
        // Static configuration; no dynamic updates
    }

    @Override
    public boolean supportsEvents() {
        return false;
    }
}
