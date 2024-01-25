package org.apache.kafka.mytest;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerSendTest {
	public static final String brokerList = "kafka9001.eniot.io:9092,kafka9002.eniot.io:9092";
	public static final String topic = "zk_errorlog";

	public static void main(String[] args) throws ExecutionException, InterruptedException {
		Properties properties = new Properties();
		properties.put("key.serializer",
				"org.apache.kafka.common.serialization.IntegerSerializer");
		properties.put("value.serializer",
				"org.apache.kafka.common.serialization.StringSerializer");
		properties.put("bootstrap.servers", brokerList);

		KafkaProducer<Integer, String> producer = new KafkaProducer<>(properties);

		ProducerRecord<Integer, String> r1 = new ProducerRecord<>(topic, 1,"hello, producer1");
		ProducerRecord<Integer, String> r2 = new ProducerRecord<>(topic, 2,"hello, producer2");
		ProducerRecord<Integer, String> r3 = new ProducerRecord<>(topic, 3,"hello, producer3");

		// 1
		producer.send(r1);
		// 2
		RecordMetadata recordMetadata = producer.send(r2).get();
		System.out.println(recordMetadata.toString());
		// 3
		producer.send(r3, new Callback() {
					@Override
					public void onCompletion(RecordMetadata metadata, Exception exception) {
						System.out.println(metadata.toString());
					}
				});
		producer.close();
	}
}
