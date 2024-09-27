package org.apache.kafka.mytest.partitioner;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerPartitionerSendTest {
	public static final String brokerList = "kafka8007.eniot.io:9092";
	public static final String topic = "tt1";

	public static void main(String[] args) throws ExecutionException, InterruptedException {
		Properties properties = new Properties();
		properties.put("key.serializer",
				"org.apache.kafka.common.serialization.IntegerSerializer");
		properties.put("value.serializer",
				"org.apache.kafka.common.serialization.StringSerializer");
		properties.put("bootstrap.servers", brokerList);
//		properties.put("partitioner.class","org.apache.kafka.mytest.partitioner.MyPartitioner");

		KafkaProducer<Integer, String> producer = new KafkaProducer<>(properties);

//		ProducerRecord<Integer, String> r1 = new ProducerRecord<>(topic, 1,"hello, producer1");
//		ProducerRecord<Integer, String> r2 = new ProducerRecord<>(topic, 2,"hello, producer2");
//		ProducerRecord<Integer, String> r3 = new ProducerRecord<>(topic, 3,"hello, producer3");

		for(int i = 0; i < 100; i++){
			ProducerRecord<Integer, String> record =  new ProducerRecord<>(topic, i,"hello, producer");
			producer.send(record, new Callback() {
				@Override
				public void onCompletion(RecordMetadata metadata, Exception exception) {
					System.out.println(metadata.toString());
				}
			});
		}

		producer.close();
	}
}
