package x.lab05_offsets;

import java.time.Duration;

import java.util.Arrays;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Oct 2023 class

public class ManualOffsetConsumer {

	private static final Logger logger = LoggerFactory.getLogger(ManualOffsetConsumer.class);

	public static void main(String[] args) throws Exception {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", "group_manual_offset");
		props.put("auto.offset.reset", "earliest");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		props.put("enable.auto.commit", "false");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList("offsets"));

        System.out.println("Listening on topic: offsets...");

		int numMessages = 0;
		while (true) {

			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

			int count = records.count();
			if (count == 0)
				continue;

			 System.out.println("Got " + count + " messages");

			for (ConsumerRecord<String, String> record : records) {
				numMessages++;
				 System.out.println("Received message [" + numMessages + "] : " + record);
			}

			// print offsets
			Set<TopicPartition> partitions = consumer.assignment();
			for (TopicPartition p : partitions) {
				long pos = consumer.position(p);
				 System.out.println("OFFSET : partition:" + p.partition() + ", offset:" + pos);
			}

			//********************
			//consumer.commitSync();

		}

	}
}
