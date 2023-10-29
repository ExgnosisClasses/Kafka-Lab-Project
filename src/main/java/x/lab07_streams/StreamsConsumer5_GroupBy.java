/// Filter out the streams
package x.lab07_streams;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;


import com.google.gson.Gson;

import x.utils.ClickstreamData;
import x.utils.MyConfig;

// Oct 2023 Class

public class StreamsConsumer5_GroupBy {
	

	public static void main(String[] args) {

		Properties config = new Properties();
		config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, MyConfig.DEFAULT_BOOTSTRAP_SERVERS);
		config.put("group.id", "streaming5");
		config.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-groupby");
		config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		// Records should be flushed every 10 seconds. This is less than the
		// default
		// in order to keep this example interactive.
		config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
		// For illustrative purposes we disable record caches
		config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

		final StreamsBuilder builder = new StreamsBuilder();

		final KStream<String, String> clickstream = builder.stream("clickstream");

		// clickstream.print(Printed.toSysOut());

		/*-
		 ==== First transformation is MAP ====
		 input::
			key=domain (String),
			value = JSON (String)
		            {"timestamp":1451635200005,"session":"session_251","domain":"facebook.com","cost":91,"user":"user_16","campaign":"campaign_5","ip":"ip_67","action":"clicked"}
		
		 mapped output:
		    key = action (String)
		    value = 1 (Integer) (used for counting / aggregating later)
		 */
		final Gson gson = new Gson();
		final KStream<String, Integer> actionStream = clickstream
				.map(new KeyValueMapper<String, String, KeyValue<String, Integer>>() {
					public KeyValue<String, Integer> apply(String key, String value) {
						try {
							ClickstreamData clickstream = gson.fromJson(value, ClickstreamData.class);
							//System.out.println("map() : got : " + value);
							String action = (clickstream.action != null) && (!clickstream.action.isEmpty())
									? clickstream.action
									: "unknown";
							KeyValue<String, Integer> actionKV = new KeyValue<>(action, 1);
							//System.out.println("map() : returning : " + actionKV);
							return actionKV;
						} catch (Exception ex) {
							// logger.error("",ex);
							System.out.println("Invalid JSON : \n" + value);
							return new KeyValue<String, Integer>("unknown", 1);
						}
					}
				});
		// actionStream.print(Printed.toSysOut());

		/*-
		 ==== Now aggregate and count actions ===
		we have to explicity state the K,V serdes in groupby, as the types are changing
		*/
		 KGroupedStream<String, Integer> grouped =
		  					actionStream.groupByKey(Grouped.with(Serdes.String(), Serdes.Integer()));

		
		  final KTable<String, Long> actionCount = grouped.count();
		  actionCount.toStream().print(Printed.toSysOut());



		// start the stream
		final KafkaStreams streams = new KafkaStreams(builder.build(), config);
		streams.cleanUp();
		streams.start();

		System.out.println("kstreams starting on " + MyConfig.TOPIC_CLICKSTREAM);

		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

	}

}
