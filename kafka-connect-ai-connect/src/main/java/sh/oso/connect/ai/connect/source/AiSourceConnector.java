package sh.oso.connect.ai.connect.source;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.connect.ai.connect.config.AiSourceConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AiSourceConnector extends SourceConnector {

    private static final Logger log = LoggerFactory.getLogger(AiSourceConnector.class);

    private Map<String, String> props;

    @Override
    public String version() {
        return "0.1.0";
    }

    @Override
    public void start(Map<String, String> props) {
        this.props = new HashMap<>(props);
        new AiSourceConfig(props); // validate config
        log.info("AiSourceConnector started");
    }

    @Override
    public Class<? extends Task> taskClass() {
        return AiSourceTask.class;
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
        log.info("AiSourceConnector stopped");
    }

    @Override
    public ConfigDef config() {
        return AiSourceConfig.BASE_CONFIG;
    }
}
