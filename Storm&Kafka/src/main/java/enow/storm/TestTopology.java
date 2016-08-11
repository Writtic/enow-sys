package enow.storm;

import org.apache.storm.Config;

//import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff;
import org.apache.storm.kafka.spout.KafkaSpoutRetryService;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * This example pulls tweets from twitter and runs them from a filter written in
 * Esper query language (EQL). Our ExternalFeedToKafkaAdapterSpout pushes
 * messages into a topic. These messages are then routed into an EsperBolt which
 * uses EQL to do some simple filtering, We then route the filtered messages to
 * a KafkaOutputBolt which dumps the filtered messages on a second topic.
 */
public class TestTopology {

	final static int MAX_ALLOWED_TO_RUN_MILLISECS = 1000 * 90 /* seconds */;

    private int expectedNumMessages = 8;

//    private LocalCluster cluster = new LocalCluster();

    private static final String TOPIC_NAME = "test";//"big-topix-" + new Random().nextInt();
    volatile static boolean finishedCollecting = false;

	private final String jarUrl = "/usr/local/Cellar/storm/1.0.1/libexec/lib/storm-core-1.0.1.jar";
	private final String outputTopic = "messages"; //+ "_output";
    private final String firstTopic = TOPIC_NAME; //+ "_input";
	private final String brokerConnectString; // kakfa broker server/port info

	public TestTopology(final String brokerConnectString) {
		this.brokerConnectString = brokerConnectString;
	}

	public static void main(String[] args) throws InvalidTopologyException, AlreadyAliveException, IOException {
		final String brokerConnectString = "localhost:9092";
		TestTopology topology = new TestTopology(brokerConnectString);
		topology.runTest();
	}

	public String getTopicName() { // input topic
		return firstTopic;
	}

	public String getSecondTopicName() { // output topic
		return outputTopic;
	}

	protected String getZkConnect() { // Uses zookeeper created by LocalCluster
		return "localhost:2181";
	}

	public void submitTopology() throws IOException, AlreadyAliveException, InvalidTopologyException {
		System.out.println("input topic: " + getTopicName() + "output topic:" + getSecondTopicName());
		final Config conf = getDebugConfigForStormTopology();
		conf.setNumWorkers(2);
		TopologyBuilder builder = new TopologyBuilder();
		configureTopology(builder);
		System.setProperty("storm.jar", jarUrl);
		try {
			StormSubmitter.submitTopology(getTopicName(), conf, builder.createTopology());
		} catch (AuthorizationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	protected void configureTopology(TopologyBuilder topologyBuilder) {
	    configureKafkaCDRSpout(topologyBuilder);
	    configureKafkaSpoutBandwidthTesterBolt(topologyBuilder);
	}

	private void configureKafkaCDRSpout(TopologyBuilder builder) {
	    KafkaSpout kafkaSpout = new KafkaSpout(createKafkaCDRSpoutConfig());
	    int spoutCount = 3;
	    builder.setSpout("kafkaspout", kafkaSpout, spoutCount)
	            .setNumTasks(5);
	}
	private SpoutConfig createKafkaCDRSpoutConfig() {
	    BrokerHosts hosts = new ZkHosts("localhost:2181");
	    String topic = "test";
	    String zkRoot = "/";
	    String consumerGroupId = "stormConsumer";
	    SpoutConfig kafkaSpoutConfig = new SpoutConfig(hosts, topic, zkRoot, consumerGroupId);
	    KafkaSpoutRetryService retryService = new KafkaSpoutRetryExponentialBackoff(new KafkaSpoutRetryExponentialBackoff.TimeInterval(500, TimeUnit.MICROSECONDS),
	            KafkaSpoutRetryExponentialBackoff.TimeInterval.milliSeconds(2), Integer.MAX_VALUE, KafkaSpoutRetryExponentialBackoff.TimeInterval.seconds(10));
	    
	    Map<String, Object> kafkaConsumerProps= new HashMap<>();
	    kafkaConsumerProps.put(KafkaSpoutConfig.Consumer.BOOTSTRAP_SERVERS,"127.0.0.1:9092");
	    kafkaConsumerProps.put(KafkaSpoutConfig.Consumer.GROUP_ID,"kafkaSpoutTestGroup");
	    kafkaConsumerProps.put(KafkaSpoutConfig.Consumer.KEY_DESERIALIZER,"org.apache.kafka.common.serialization.StringDeserializer");
	    kafkaConsumerProps.put(KafkaSpoutConfig.Consumer.VALUE_DESERIALIZER,"org.apache.kafka.common.serialization.StringDeserializer");

	    KafkaSpoutConfig kafkaSpoutConfig = new KafkaSpoutConfig.Builder<String, String>(kafkaConsumerProps, kafkaSpoutStreams, tuplesBuilder, retryService)
	            .setOffsetCommitPeriodMs(10_000)
	            .setFirstPollOffsetStrategy("EARLIEST")
	            .setMaxUncommittedOffsets(250)
	            .build();
	   
	    return kafkaSpoutConfig;
	}

	public void configureKafkaSpoutBandwidthTesterBolt(TopologyBuilder topologyBuilder) {
	    topologyBuilder.setBolt("testBolt", new ToKafkaBolt(brokerConnectString, "messages"), 8)
	            .setNumTasks(5)
	            .localOrShuffleGrouping("kafkaspout");
	}

	public static Config getDebugConfigForStormTopology() {
		Config config = new Config();
		config.setDebug(true);
		config.put(Config.STORM_LOCAL_DIR, "/tmp/storm-data");
		config.put(Config.NIMBUS_THRIFT_PORT, 6627);
		config.put(Config.STORM_ZOOKEEPER_PORT, 2181);
		config.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList(new String[] { "localhost" })); // zookeeper
		config.put(Config.STORM_ZOOKEEPER_CONNECTION_TIMEOUT, 900 * 1000);
		config.put(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT, 900 * 1000);
		config.setNumWorkers(2);
		return config;
	}
	
	private void runTest() {
			try {
				submitTopology();
			} catch (AlreadyAliveException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvalidTopologyException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        System.out.println("SUCCESSFUL COMPLETION");
    }
}