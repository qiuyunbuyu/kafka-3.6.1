package org.apache.kafka.mytest;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class TxnProducerSendTest {
	public static final String brokerList = "localhost:9092";
	public static final String topic = "studytopic";

	public static void main(String[] args) throws ExecutionException, InterruptedException {
		Properties properties = new Properties();
		properties.put("key.serializer",
				"org.apache.kafka.common.serialization.IntegerSerializer");
		properties.put("value.serializer",
				"org.apache.kafka.common.serialization.StringSerializer");
		properties.put("bootstrap.servers", brokerList);
		properties.put("transactional.id", "fishyu");

		KafkaProducer<Integer, String> producer = new KafkaProducer<>(properties);

		producer.initTransactions();
		producer.beginTransaction();

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

		for(int i = 0; i < 100; i++){

			ProducerRecord<Integer, String> rx = new ProducerRecord<>(topic, i,"hello, producer1");
			producer.send(rx);
		}

		producer.commitTransaction();
		producer.close();
	}
}
