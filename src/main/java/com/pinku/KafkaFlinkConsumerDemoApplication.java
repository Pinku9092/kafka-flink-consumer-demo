package com.pinku;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class KafkaFlinkConsumerDemoApplication {

	public static void main(String[] args) throws Exception {
		//System.setProperty("execution.target", "local");

		System.out.println("Kafka Flink Consumer Demo Application");
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("group.id", "flink-group");
		properties.setProperty("auto.offset.reset", "earliest");
		FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
				"testtopic1",
				new SimpleStringSchema(),
				properties
		);
		env.addSource(consumer)
				.name("Kafka Source")
				.addSink(new MongoSink());

		env.getConfig().setGlobalJobParameters(new org.apache.flink.api.common.ExecutionConfig.GlobalJobParameters() {});
		// Execute the Flink job
		env.execute("Flink Kafka Consumer Job");
		System.out.println("main method executed");

	}

}
