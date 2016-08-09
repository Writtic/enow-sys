package enow.storm;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.kafka.clients.producer.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
//Send a message to Apache Kafka via Bolt of Apache Storm
public class ToKafkaBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;

	private String brokerConnectString;
	private String topicName;
	private String serializerClass;

	private transient Producer<String, String> producer;
	private transient OutputCollector collector;
	private transient TopologyContext context;

	public void ToKafkaBolt(String brokerConnectString, String topicName, String serializerClass) {
		if (serializerClass == null) {
			serializerClass = "kafka.serializer.StringEncoder";
		}
		this.brokerConnectString = brokerConnectString;
		this.serializerClass = serializerClass;
		this.topicName = topicName;
	}

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		Properties props = new Properties();
		props.put("metadata.broker.list", brokerConnectString);
		props.put("serializer.class", serializerClass);
		props.put("producer.type", "sync");
		props.put("batch.size", "1");

		producer = new KafkaProducer<String, String>(props);

		this.context = context;
		this.collector = collector;
	}

	public void execute(Tuple input) {
		String tuple = null;
		try {
			tuple = input.getString(0);
			ProducerRecord<String, String> data = new ProducerRecord<String, String>(topicName, tuple);
			producer.send(data);
			collector.ack(input);
		} catch (Exception e) {
			collector.fail(input);
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

	public static KafkaProducer<String, String> initProducer() throws IOException {
		Properties props = new Properties();
		props.put("metadata.broker.list", "localhost:9092");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("producer.type", "async");
		props.put("batch.size", "1");

		return new KafkaProducer<String, String>(props);
	}
}
