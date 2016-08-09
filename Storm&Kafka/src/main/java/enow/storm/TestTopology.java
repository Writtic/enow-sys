package enow.storm;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;

import java.io.IOException;


/**
 * This example pulls tweets from twitter and runs them from a filter written in Esper query language (EQL). Our
 * ExternalFeedToKafkaAdapterSpout pushes messages into a topic. These messages are then routed into an EsperBolt which
 * uses EQL to do some simple filtering, We then route the filtered messages to a KafkaOutputBolt which
 * dumps the filtered messages on a second topic.
 */
public class TestTopology {

	private final String outputTopic = "test";
    private final String firstTopic = "onlytest";
    private final String brokerConnectString;                   // kakfa broker server/port info
    public TestTopology(
            final String brokerConnectString) {
        this.brokerConnectString = brokerConnectString;
    }
    public static void main(String[] args) throws InvalidTopologyException, AlreadyAliveException, IOException {
//    	if (args.length != 1) {
//            throw new RuntimeException("USAGE: "
//                    + "<kafkaBrokerConnectString>"
//            );
//        }
    	final String brokerConnectString = "localhost:9092";
        TestTopology topology = new TestTopology(brokerConnectString);
        topology.submitTopology();

    }
    public String getTopicName() {          // input topic
        return firstTopic;
    }

    public String getSecondTopicName() {   // output topic
        return outputTopic;
    }
    
    protected String getZkConnect() {   // Uses zookeeper created by LocalCluster
        return "localhost:2181";
    }


    public void submitTopology() throws IOException, AlreadyAliveException, InvalidTopologyException {
        System.out.println("input topic: " + getTopicName() + "output topic:" + getSecondTopicName());
        final Config conf = getDebugConfigForStormTopology();
        conf.setNumWorkers(2);
        try {
			StormSubmitter.submitTopology(getTopicName(), conf, createTopology());
		} catch (AuthorizationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

    protected StormTopology createTopology() {
        return InitTopology.
                createTopology(
                        getZkConnect(),
                        brokerConnectString,
                        getTopicName(),
                        getSecondTopicName()
                        );
    }
    public static Config getDebugConfigForStormTopology() {
        Config config = new Config();
        config.setDebug(true);
        config.put(Config.STORM_ZOOKEEPER_CONNECTION_TIMEOUT, 900 * 1000);
        config.put(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT, 900 * 1000);
        return config;
    }
}