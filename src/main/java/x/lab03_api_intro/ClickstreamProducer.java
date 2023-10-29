package x.lab03_api_intro;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import x.utils.ClickStreamGenerator;
import x.utils.ClickstreamData;
import x.utils.MyUtils;

// Oct 2023 class

public class ClickstreamProducer {
	public static void main(String[] args) throws Exception {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ProducerConfig.CLIENT_ID_CONFIG, "ClickstreamProducer");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		KafkaProducer<String, String> producer = new KafkaProducer<>(props);

		for (int i = 1; i <= 10; i++) {
			ClickstreamData clickstream = ClickStreamGenerator.getClickStreamRecord();
			String clickstreamJSON = ClickStreamGenerator.getClickstreamAsJSON(clickstream);

			String key = clickstream.domain;
			String value = clickstreamJSON;

			
			ProducerRecord<String, String> record = new ProducerRecord<>("clickstream", key, value);

			// Experiment: measure the time taken for both send options

			long t1 = System.nanoTime();
			// sending without waiting for response
			producer.send(record);

			// sending and waiting for response
			// RecordMetadata meta = producer.send(record).get();
			long t2 = System.nanoTime();

			System.out.println(String.format("Sent record [%d] (key:%s, value:%s), " + "time took = %.2f ms", i, key, value,
					(t2 - t1) / 1e6));

			MyUtils.randomDelay(100, 500);

		}

		producer.close();

	}

}
