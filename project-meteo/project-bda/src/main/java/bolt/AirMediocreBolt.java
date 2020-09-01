package bolt;

import Operator.Computation;
import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class AirMediocreBolt  implements IRichBolt {
    private static final Logger LOG = Logger.getLogger(AirMediocreBolt.class);
    // c'est outputCollector pour emetre de message en sortie du bolt
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;

    }

    @Override
    public void execute(Tuple tuple) {
        // Check if there is no available bicycle. If so, send an alert.
        LOG.info(tuple);
        String alert = Computation.airMediocreAlert(tuple);
        if(alert!=null) {
            collector.emit(tuple, new Values(alert));
        }
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("alert"));

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
