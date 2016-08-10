package enow.storm;

import java.util.concurrent.CountDownLatch;

import com.google.common.io.Files;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import java.io.File;
import java.util.Properties;

import org.apache.kafka.clients.producer.*;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;

import kafka.admin.AdminUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;

public class TestProducer {

	private KafkaServer kafkaServer = null;
	private String topicName;

	CountDownLatch topologyStartedLatch;
	public CountDownLatch producerFinishedInitialBatchLatch = new CountDownLatch(1);

	Producer<String, String> producer;

	private String[] sentences;

	TestProducer(String[] sentences, String topicName, CountDownLatch topologyStartedLatch) {
		this.sentences = sentences;
		this.topicName = topicName;
		this.topologyStartedLatch = topologyStartedLatch;
	}

	public Thread startProducer() {
		Thread sender = new Thread(new Runnable() {
			public void run() {
				emitBatch();
				CoordinationUtils.countDown(producerFinishedInitialBatchLatch);
				CoordinationUtils.await(topologyStartedLatch);
				emitBatch(); // emit second batch after we know topology is up
			}
		}, "producerThread");
		sender.start();
		return sender;
	}

	private void emitBatch() {
		Properties props = new Properties();
		props.put("batch.size", "1");
		props.put("bootstrap.servers", "localhost:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		Producer<String, String> producer = new KafkaProducer<String, String>(props);

		for (String sentence : sentences) {
			System.out.println(sentence);
			ProducerRecord<String, String> data = new ProducerRecord<String, String>(topicName, sentence);
			producer.send(data);
		}
		producer.close();

	}

	public void createTopic(String topicName) {
		//String zookeeperConnect = "zkserver1:2181,zkserver2:2181";
		String zookeeperConnect = "localhost:2181";
		int sessionTimeoutMs = 10 * 1000;
		int connectionTimeoutMs = 8 * 1000;
		// Note: You must initialize the ZkClient with ZKStringSerializer. If you don't, then
		// createTopic() will only seem to work (it will return without error). The topic will exist in
		// only ZooKeeper and will be returned when listing topics, but Kafka itself does not create the topic.
		ZkClient zkClient = new ZkClient(zookeeperConnect, sessionTimeoutMs, connectionTimeoutMs,
				ZKStringSerializer$.MODULE$);

		// Security for Kafka was added in Kafka 0.9.0.0
		boolean isSecureKafkaCluster = false;
		ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeperConnect), isSecureKafkaCluster);
		
		String topic = topicName;
		int partitions = 2;
		int replication = 3;
		Properties props = new Properties();
		props.put("flush.messages", "1");
		Properties topicConfig = new Properties(props); // add per-topic configurations settings here
		AdminUtils.createTopic(zkUtils, topic, partitions, replication, topicConfig);
		zkClient.close();
	}

	public void startKafkaServer() {
		File tmpDir = Files.createTempDir();
		Properties props = createProperties(tmpDir.getAbsolutePath(), 9092, 1);
		KafkaConfig kafkaConfig = new KafkaConfig(props);

		kafkaServer = new KafkaServer(kafkaConfig, null, null);
		kafkaServer.startup();
	}

	public void shutdown() {
		kafkaServer.shutdown();
	}

	private Properties createProperties(String logDir, int port, int brokerId) {
		Properties properties = new Properties();
		properties.put("port", port + "");
		properties.put("broker.id", brokerId + "");
		properties.put("log.dir", logDir);
		properties.put("zookeeper.connect", "localhost:2181"); // Uses zookeeper
																										// created by LocalCluster
		return properties;
	}

}