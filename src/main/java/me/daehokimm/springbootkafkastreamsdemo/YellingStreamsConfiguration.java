package me.daehokimm.springbootkafkastreamsdemo;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class YellingStreamsConfiguration {

	public static final String KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";
	public static final String STREAMS_APP_NAME = "yelling-streams";
	public static final String SOURCE_TOPIC_NAME = "whisper-message";
	public static final String STATE_STORE_NAME = "count-store";
	public static final int SUPPRESSED_SECONDS = 30;

	@Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_BUILDER_BEAN_NAME)
	public StreamsBuilderFactoryBean streamsBuilderFactoryBean(KafkaStreamsConfiguration configuration) {
		return new StreamsBuilderFactoryBean(configuration);
	}

	@Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
	public KafkaStreamsConfiguration kafkaStreamsConfiguration() {
		Map<String, Object> properties = new HashMap<>();
		properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
		properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
		properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
		properties.put(StreamsConfig.APPLICATION_ID_CONFIG, STREAMS_APP_NAME);

		return new KafkaStreamsConfiguration(properties);
	}

	@Bean
	public KStream<String, String> kStream(StreamsBuilder streamsBuilder) {
		KStream<String, String> stream = streamsBuilder.stream(SOURCE_TOPIC_NAME, Consumed.with(Serdes.String(), Serdes.String()));

		// Mapping values lowercase to uppercase 
		stream
				.map((key, value) -> new KeyValue<>(key, value.toUpperCase()))
				.print(Printed.toSysOut());        // or sink other topic by `.to("sink-topic")`


		// Count each alphabet
		stream
				.flatMap(((key, value) -> {
					String[] characters = value.split("");
					ArrayList<KeyValue<String, Integer>> keyValueList = new ArrayList<>();
					for (String s : characters) {
						keyValueList.add(new KeyValue<>(s.toUpperCase(), 1));
					}
					return keyValueList;
				}))
				.groupByKey(Grouped.with(Serdes.String(), Serdes.Integer()))
				.count(Materialized.as(STATE_STORE_NAME))
				.suppress(Suppressed.untilTimeLimit(Duration.ofSeconds(SUPPRESSED_SECONDS), Suppressed.BufferConfig.unbounded()))
				.toStream()
				.print(Printed.toSysOut());        // or sink other topic by `.to("sink-topic")`

		return stream;
	}
}
