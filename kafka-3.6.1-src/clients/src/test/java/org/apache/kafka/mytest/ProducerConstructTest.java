package org.apache.kafka.mytest;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.PartitionInfo;

import java.util.Properties;

public class ProducerConstructTest {
	public static final String brokerList = "kafka9001.eniot.io:9092";

	public static void main(String[] args) {
		Properties properties = new Properties();
		properties.put("key.serializer",
				"org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer",
				"org.apache.kafka.common.serialization.StringSerializer");
		properties.put("bootstrap.servers", brokerList);

		KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

		producer.close();
	}
}
