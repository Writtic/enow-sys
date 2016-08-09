package enow.storm;

import java.util.UUID;

import org.apache.storm.generated.StormTopology;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.kafka.*;

public class InitTopology {
	public static int STORM_KAFKA_FROM_READ_FROM_CURRENT_OFFSET = -1;
	public static int STORM_KAFKA_FROM_READ_FROM_START = -2;

	public static StormTopology createTopology(String zookeeperConnectString, String kafkaBrokerConnectString,
			String inputTopic, String outputTopic) {
		TopologyBuilder builder = new TopologyBuilder();

		ToKafkaBolt kafkaOutputBolt = new ToKafkaBolt();

		builder.setSpout("kafkaSpout", createKafkaSpout(zookeeperConnectString, inputTopic));

		builder.setBolt("kafkaOutputBolt", kafkaOutputBolt, 1).shuffleGrouping("kafkaSpout");
		
		return builder.createTopology();
	}

	public static KafkaSpout createKafkaSpout(String zkConnect, String topicName) {
		BrokerHosts brokerHosts = new ZkHosts(zkConnect);
		SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, topicName, "", "storm");
		//kafkaConfig.forceStartOffsetTime(STORM_KAFKA_FROM_READ_FROM_START);
		kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		return new KafkaSpout(kafkaConfig);
	}
}
