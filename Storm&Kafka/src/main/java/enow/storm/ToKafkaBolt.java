package enow.storm;

import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.kafka.clients.producer.*;

import org.apache.storm.kafka.bolt.mapper.TupleToKafkaMapper;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
//Send a message to Apache Kafka via Bolt of Apache Storm
public class ToKafkaBolt extends KafkaBolt<Object, Object> {
	private static final long serialVersionUID = 1L;

	private String brokerConnectString;
	private String topicName;
	private String serializerClass;
	
	private int expectedNumMessages;
    private int countReceivedMessages = 0;

	private transient Producer<String, String> producer;
	private transient OutputCollector collector;
	private transient TopologyContext context;

	ToKafkaBolt(String brokerConnectString, String topicName, int expectedNumMessages) {
		this.expectedNumMessages = expectedNumMessages;
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
        .withTopicSelector(new DefaultTopicSelector(topicName));
        
		this.context = context;
		this.collector = collector;
	}

	public void execute(Tuple input) {
		this.withTupleToKafkaMapper(TupleToKafkaMapper input);
		final String msg = input.toString();

        countReceivedMessages++;
        String info = " recvd: " + countReceivedMessages + " expected: " + expectedNumMessages;
        System.out.println(info +    " >>>>>>>>>>>>>" + msg);

        TestTopology.recordRecievedMessage(msg);
        ProducerRecord<String, String> data = new ProducerRecord<String, String>(topicName, msg+" from Apache Storm");
        if (countReceivedMessages == expectedNumMessages) {
            System.out.println(" +++++++++++++++++++++ MARKING");
            TestTopology.finishedCollecting = true;
        }
        if (countReceivedMessages > expectedNumMessages) {
            System.out.print("Fatal error: too many messages received");
            System.exit(-1);
        }
		try {
			producer.send(data);
			collector.ack(input);
		} catch (Exception e) {
			collector.fail(input);
		}
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}
}
