package org.apache.kafka.mytest;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerSendTest {
	public static final String brokerList = "kafka9001.eniot.io:9092";
	public static final String topic = "zk_errorlog";

	public static void main(String[] args) throws ExecutionException, InterruptedException {
		Properties properties = new Properties();
		properties.put("key.serializer",
				"org.apache.kafka.common.serialization.IntegerSerializer");
		properties.put("value.serializer",
				"org.apache.kafka.common.serialization.StringSerializer");
		properties.put("bootstrap.servers", brokerList);

//		properties.put("linger.ms", 10000000);

		KafkaProducer<Integer, String> producer = new KafkaProducer<>(properties);

		ProducerRecord<Integer, String> r1 = new ProducerRecord<>(topic, 1,"hello, producer1");
		ProducerRecord<Integer, String> r2 = new ProducerRecord<>(topic, 1,"hello, producer1");
		ProducerRecord<Integer, String> r3 = new ProducerRecord<>(topic, 1,"hello, producer1");
		ProducerRecord<Integer, String> r4 = new ProducerRecord<>(topic, 1,"hello, producer1");
		ProducerRecord<Integer, String> r5 = new ProducerRecord<>(topic, 1,"hello, producer1");

		// ===============ProducerBatch1==================
//		ProducerBatch(topicPartition=zk_errorlog-0, recordCount=0):1729133409121
//				--------------------0---------------null
//		ProducerBatch(topicPartition=zk_errorlog-0, recordCount=1):1729133409121
//				--------------------0---------------0
//		ProducerBatch(topicPartition=zk_errorlog-0, recordCount=2):1729133409121
//				--------------------0---------------1
//		ProducerBatch(topicPartition=zk_errorlog-0, recordCount=3):1729133409121
//				--------------------0---------------2
//		ProducerBatch(topicPartition=zk_errorlog-0, recordCount=4):1729133409121
//				--------------------0---------------3
//		ProducerBatch(topicPartition=zk_errorlog-0, recordCount=0):1729060926368
		// ===============ProducerBatch1==================
		producer.send(r1);
		// 2
		producer.send(r2);
		// 3
		producer.send(r3, new Callback() {
					@Override
					public void onCompletion(RecordMetadata metadata, Exception exception) {
						System.out.println(metadata.toString());
					}
				});

		producer.send(r4);
		producer.send(r5);

		Thread.sleep(10000);

		// ===============ProducerBatch2==================
//		ProducerBatch(topicPartition=zk_errorlog-0, recordCount=0):1729133419148
//				--------------------0---------------null
//		ProducerBatch(topicPartition=zk_errorlog-0, recordCount=1):1729133419148
//				--------------------0---------------0
//		ProducerBatch(topicPartition=zk_errorlog-0, recordCount=2):1729133419148
//				--------------------0---------------1
//		ProducerBatch(topicPartition=zk_errorlog-0, recordCount=3):1729133419148
//				--------------------0---------------2
		// ===============ProducerBatch2==================
		producer.send(r4);
		producer.send(r5);
		producer.send(r4);
		producer.send(r5);

//		for(int i = 0; i < 1000; i++){
//
//			ProducerRecord<Integer, String> rx = new ProducerRecord<>(topic, i,"hello, producer1");
//			producer.send(rx);
//		}
		producer.close();
	}
}
