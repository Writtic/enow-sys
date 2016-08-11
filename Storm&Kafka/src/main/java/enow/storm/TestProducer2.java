package enow.storm;

import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.clients.producer.*;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.InvalidTopologyException;

public class TestProducer2 {

	private String topicName;

	Producer<String, String> producer;

	private static String[] sentences = new String[]{
            "one dog9 - saw the fox over the moon",
            "two cats9 - saw the fox over the moon",
            "four bears9 - saw the fox over the moon",
            "five goats9 - saw the fox over the moon",
    };

	TestProducer2(String topicName) {
		this.topicName = topicName;
	}

	public static void main(String[] args) throws InvalidTopologyException, AlreadyAliveException, IOException {
		TestProducer2 producer = new TestProducer2("test");
		producer.emitBatch();
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
}