package enow.storm;

import java.util.Properties;

import org.apache.kafka.clients.producer.*;

public class TestProducer2 {

	private String topicName;

	Producer<String, String> producer;

	private String[] sentences;

	TestProducer2(String[] sentences, String topicName) {
		this.sentences = sentences;
		this.topicName = topicName;
	}

	public void startProducer() {
		emitBatch();
		emitBatch();
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