package enow.storm;

import java.util.Properties;

import org.apache.kafka.clients.producer.*;
public class TestProducer {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
			
		Properties props = new Properties();
		 props.put("bootstrap.servers", "localhost:9092");
		 props.put("acks", "all");
		 props.put("retries", 0);
		 props.put("batch.size", 16384);
		 props.put("linger.ms", 1);
		 props.put("buffer.memory", 33554432);
		 props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		 props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		 KafkaProducer<String, String> producer = new KafkaProducer<String,String>(props);
		 
		 producer.send(new ProducerRecord<String, String>("onlytest", "test"));

		 producer.close();
		 
	}
}