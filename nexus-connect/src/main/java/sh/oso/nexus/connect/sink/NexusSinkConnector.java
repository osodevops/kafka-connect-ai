package sh.oso.nexus.connect.sink;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.nexus.connect.config.NexusSinkConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NexusSinkConnector extends SinkConnector {

    private static final Logger log = LoggerFactory.getLogger(NexusSinkConnector.class);

    private Map<String, String> props;

    @Override
    public String version() {
        return "0.1.0";
    }

    @Override
    public void start(Map<String, String> props) {
        this.props = new HashMap<>(props);
        new NexusSinkConfig(props); // validate config
        log.info("NexusSinkConnector started");
    }

    @Override
    public Class<? extends Task> taskClass() {
        return NexusSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> configs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            configs.add(new HashMap<>(props));
        }
        return configs;
    }

    @Override
    public void stop() {
        log.info("NexusSinkConnector stopped");
    }

    @Override
    public ConfigDef config() {
        return NexusSinkConfig.BASE_CONFIG;
    }
}
