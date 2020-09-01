package bolt;

import com.google.common.io.Resources;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;


public class ExitAirMediocreBolt extends KafkaBolt {
    public ExitAirMediocreBolt() {
        super();

        try {
            InputStream props = Resources.getResource("producer.props").openStream();
            Properties properties = new Properties();
            properties.load(props);
            this.withTopicSelector(new DefaultTopicSelector("grp-16-atmo-alert"))
                    .withProducerProperties(properties)
                    .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper("key", "alert"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
