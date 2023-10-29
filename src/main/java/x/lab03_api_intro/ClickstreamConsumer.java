package x.lab03_api_intro;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

// Oct 2023 class

public class ClickstreamConsumer {

	public static void main(String[] args) throws Exception {
		
		Properties props = new Properties();
		
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

		props.put(ConsumerConfig.CLIENT_ID_CONFIG, "clickstream-consumer");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "clickstream-consumer");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		
		
		consumer.subscribe(Arrays.asList("clickstream"));

		System.out.println("Listening on clickstream topic");
		
		int numMessages = 0;
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

			int count = records.count();
			if (count == 0)
				continue;
			System.out.println("Got " + count + " messages");

			for (ConsumerRecord<String, String> record : records) {
				numMessages++;
				System.out.println("Received message [" + numMessages + "] : " + record);

			}
		}

		

	}

}
