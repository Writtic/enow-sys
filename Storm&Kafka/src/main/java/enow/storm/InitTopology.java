package enow.storm;

import java.util.Properties;

import org.apache.storm.generated.StormTopology;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.kafka.*;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;

public class InitTopology {
//	public static int STORM_KAFKA_FROM_READ_FROM_CURRENT_OFFSET = -1;
//	public static int STORM_KAFKA_FROM_READ_FROM_START = -2;

	public static StormTopology createTopology(String zookeeperConnectString, String kafkaBrokerConnectString,
			String inputTopic, String outputTopic, int expectedNumMessages) {
		TopologyBuilder builder = new TopologyBuilder();

		//ToKafkaBolt kafkaOutputBolt = new ToKafkaBolt(zookeeperConnectString, outputTopic, expectedNumMessages);
		
		builder.setSpout("kafkaSpout", createKafkaSpout(zookeeperConnectString, inputTopic), 1);

		builder.setBolt("kafkaOutputBolt", createKafkaBolt(kafkaBrokerConnectString, outputTopic), 1).shuffleGrouping("kafkaSpout");
		
		return builder.createTopology();
	}
	
	public static KafkaBolt<?, ?> createKafkaBolt(String kafkaBrokerConnectString, String topicName) {
		//set producer properties.
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaBrokerConnectString);
        props.put("acks", "1");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        
		KafkaBolt<?, ?> bolt = new KafkaBolt<Object, Object>().withProducerProperties(props)
                .withTopicSelector(new DefaultTopicSelector(topicName))
                .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper<Object, Object>());
		
		return bolt;
	}
	
	public static KafkaSpout createKafkaSpout(String zkConnect, String topicName) {
		BrokerHosts brokerHosts = new ZkHosts(zkConnect);
		SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, topicName, "/tmp/zookeeper", "storm");
		//kafkaConfig.forceStartOffsetTime(STORM_KAFKA_FROM_READ_FROM_START);
		kafkaConfig.zkPort=2181;
		kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		return new KafkaSpout(kafkaConfig);
	}
}
