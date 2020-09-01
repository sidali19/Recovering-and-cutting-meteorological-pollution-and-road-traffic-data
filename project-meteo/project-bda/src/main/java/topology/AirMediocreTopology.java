package topology;

import bolt.AirMediocreBolt;
import bolt.ExitAirMediocreBolt;
import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
;import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;

;

public class AirMediocreTopology {
    private static final Logger LOG = Logger.getLogger(AirMediocreTopology.class);
    // arg 1: <zk-hosts> ip:port
    // arg 2: <kafka-topic> topic
    // arg 3: <zk-path> /brokers
    // arg 4: <clientid> storm-consumer

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {

      /*  if(args.length < 4) {
            LOG.fatal("Incorrect number of arguments. Required arguments: <zk-hosts> <kafka-topic> <zk-path> <clientid>");
            System.exit(1);
        }
        */
        int nbExecutors = 1;
        //final BrokerHosts zkrHosts = new ZkHosts("cl-ter-manager-jt3m3gbgctz6-qdrjqxrsgate-nlx635i3m5wq:2181");
         final String kafkaTopic = "grp-16-atmo";
         final String broker = "node-12:9092";
        //final String zkRoot = "/cluster-node-cztqntk4f5x6-clogovtafhcq-ikmht4fosq6e.novalocal:9092";
        //final String clientId = "storm-consumer";;
        //Builder<String, String> builder = KafkaSpoutConfig.builder(broker,kafkaTopic);
        KafkaSpoutConfig kafkaSpoutConf = KafkaSpoutConfig.builder(broker,kafkaTopic).setGroupId("group-16").build();

        KafkaSpout kafkaConf = new KafkaSpout<>(kafkaSpoutConf);
        //kafkaSpoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());

        // Create topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-spout", kafkaConf);
        builder.setBolt("AirMediocreBolt", new AirMediocreBolt(), nbExecutors).shuffleGrouping("kafka-spout");
        builder.setBolt("ExitAirMediocreBolt", new ExitAirMediocreBolt(), nbExecutors).shuffleGrouping("AirMediocreBolt");

        // Submit the topolgy to local cluster
        // En local
        //final LocalCluster localCluster = new LocalCluster();
        //localCluster.submitTopology("AirMediocreTopology",new HashMap() , builder.createTopology());


        // In the VM
        Config config = new Config();
        config.setDebug(true);
        //config.put(config.TOPOLOGY_MAX_SPOUT_PENDING,1);
        //config.put(Config.NIMBUS_HOST,"192.168.76.144");
        //config.put(Config.NIMBUS_THRIFT_PORT,6627);
        StormSubmitter.submitTopology("AirMediocreTopology", config, builder.createTopology());
    }

}

