package enow.storm;

import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;

import java.util.Map;
import java.util.Properties;
//Send a message to Apache Kafka via Bolt of Apache Storm
public class ToKafkaBolt extends KafkaBolt<String, String> {
	private static final long serialVersionUID = 1L;

	private String brokerConnectString;
	private String topicName;
	
	private transient OutputCollector collector;
	private transient TopologyContext context;

	ToKafkaBolt(String brokerConnectString, String topicName) {
		this.brokerConnectString = brokerConnectString;
		this.topicName = topicName;
	}

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		Properties props = new Properties();
		props.put("producer.type", "sync");
		props.put("batch.size", "1");
		props.put("bootstrap.servers", brokerConnectString);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		
		this.withProducerProperties(props)
        .withTopicSelector(new DefaultTopicSelector(topicName))
		.withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper<String, String>());
        
		this.context = context;
		this.collector = collector;
	}

	public void execute(Tuple input) {
		final String msg = input.toString();
		String[] words = msg.split(" ");
		for(String word:words){
			System.out.println("Word: "+word);
			collector.emit(input, new Values(word));
		}
		try {
			collector.ack(input);
		} catch (Exception e) {
			collector.fail(input);
		}
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare( new Fields("word"));
	}
}
