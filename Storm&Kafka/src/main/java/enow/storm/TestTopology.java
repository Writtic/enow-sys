package enow.storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

/**
 * This example pulls tweets from twitter and runs them from a filter written in
 * Esper query language (EQL). Our ExternalFeedToKafkaAdapterSpout pushes
 * messages into a topic. These messages are then routed into an EsperBolt which
 * uses EQL to do some simple filtering, We then route the filtered messages to
 * a KafkaOutputBolt which dumps the filtered messages on a second topic.
 */
public class TestTopology {

	final static int MAX_ALLOWED_TO_RUN_MILLISECS = 1000 * 90 /* seconds */;

//    CountDownLatch topologyStartedLatch = new CountDownLatch(1);

    private static int STORM_KAFKA_FROM_READ_FROM_START = -2;
    private static int STORM_KAFKA_FROM_READ_FROM_CURRENT_OFFSET = -1;
    private static int readFromMode = STORM_KAFKA_FROM_READ_FROM_START;
    private int expectedNumMessages = 8;
    
    private static final int SECOND = 1000;
    private static List<String> messagesReceived = new ArrayList<String>();

    private LocalCluster cluster = new LocalCluster();

    private static final String TOPIC_NAME = "test";//"big-topix-" + new Random().nextInt();
    volatile static boolean finishedCollecting = false;
	
    private static String[] sentences = new String[]{
            "one dog9 - saw the fox over the moon",
            "two cats9 - saw the fox over the moon",
            "four bears9 - saw the fox over the moon",
            "five goats9 - saw the fox over the moon",
    };

//    private TestProducer kafkaProducer = new  TestProducer(sentences, TOPIC_NAME, topologyStartedLatch);
    private TestProducer2 kafkaProducer = new TestProducer2(sentences, TOPIC_NAME);
    public static void recordRecievedMessage(String msg) {
        synchronized (TestTopology.class) {                 // ensure visibility of list updates between threads
            messagesReceived.add(msg);
        }
    }

	private final String jarUrl = "/usr/local/Cellar/storm/1.0.1/libexec/lib/storm-core-1.0.1.jar";
	private final String outputTopic = TOPIC_NAME; //+ "_output";
    private final String firstTopic = TOPIC_NAME; //+ "_input";
	private final String brokerConnectString; // kakfa broker server/port info

	public TestTopology(final String brokerConnectString) {
		this.brokerConnectString = brokerConnectString;
	}

	public static void main(String[] args) throws InvalidTopologyException, AlreadyAliveException, IOException {
		// if (args.length != 1) {
		// throw new RuntimeException("USAGE: "
		// 			+ "<kafkaBrokerConnectString>"
		// 		);
		// }
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
		try {
			System.setProperty("storm.jar", jarUrl);
			StormSubmitter.submitTopology(getTopicName(), conf, createTopology());
		} catch (AuthorizationException e) {
			System.out.println(e);
			e.printStackTrace();
		} catch (AlreadyAliveException ae) {
			System.out.println(ae);
			ae.printStackTrace();
		} catch (InvalidTopologyException ie) {
			System.out.println(ie);
			ie.printStackTrace();
		}
	}

	protected StormTopology createTopology() {
		return InitTopology.
				createTopology(
				getZkConnect(), 
				brokerConnectString, 
				getTopicName(), 
				getSecondTopicName(), 
				expectedNumMessages);
	}

	public static Config getDebugConfigForStormTopology() {
		Config config = new Config();
		config.setDebug(true);
		config.put(Config.STORM_ZOOKEEPER_CONNECTION_TIMEOUT, 900 * 1000);
		config.put(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT, 900 * 1000);
		return config;
	}
	
	private void runTest() {
//		CoordinationUtils.setMaxTimeToRunTimer(MAX_ALLOWED_TO_RUN_MILLISECS);
//		CoordinationUtils.waitForServerUp("localhost", 2181, 5 * SECOND);   // Wait for zookeeper to come up

//        kafkaProducer.startKafkaServer();
//        kafkaProducer.createTopic(TOPIC_NAME);

        kafkaProducer.startProducer();
//		CoordinationUtils.await(kafkaProducer.producerFinishedInitialBatchLatch);

		try {
			setupKafkaSpoutAndSubmitTopology();
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
//		CoordinationUtils.countDown(topologyStartedLatch);

        verifyResults();
        shutdown();
        
        System.out.println("SUCCESSFUL COMPLETION");
        
        //System.exit(0);
    }
	
	private void verifyResults() {
        synchronized (TestTopology.class) {                 // ensure visibility of list updates between threads
            int count = 0;
            for (String msg : messagesReceived) {
                if (msg.contains("cat") || msg.contains("dog") || msg.contains("bear") || msg.contains("goat")) {
                    count++;
                }
            }
            if (count != expectedNumMessages) {
                System.out.println(">>>>>>>>>>>>>>>>>>>>FAILURE -   Did not receive expected messages");
                System.exit(-1);
            }
        }
    }
	
	private void setupKafkaSpoutAndSubmitTopology() throws InterruptedException {
		System.out.println("input topic: " + getTopicName() + "output topic:" + getSecondTopicName());
        BrokerHosts brokerHosts = new ZkHosts("localhost:2181");

        SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, TOPIC_NAME, "/" + TOPIC_NAME, "storm");
        //kafkaConfig.forceStartOffsetTime(readFromMode  /* either earliest or current offset */);
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

        Config conf = getDebugConfigForStormTopology();
		conf.setNumWorkers(2);
		conf.put(Config.STORM_LOCAL_DIR, "/usr/local/Cellar/storm/1.0.1");
		conf.put(Config.NIMBUS_THRIFT_PORT, 6627);
		conf.put(Config.STORM_ZOOKEEPER_PORT, 2181);
		conf.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList(new String[] { "localhost" }));
        cluster.submitTopology("kafka-test", conf, createTopology());
    }
	
	private void shutdown() {
        cluster.shutdown();
//        kafkaProducer.shutdown();
    }
}