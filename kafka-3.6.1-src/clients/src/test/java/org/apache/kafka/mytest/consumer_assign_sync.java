package org.apache.kafka.mytest;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * assign(1 TP) + commitSync
 */
public class consumer_assign_sync {
	public static final String brokerList = "kafka9001.eniot.io:9092";
	public static final String topic = "EOC_ALERT_MESSAGE_TOPIC";
	public static final String groupId = "consumer.fishyu";
	public static final AtomicBoolean isRunning = new AtomicBoolean(true);

	public static Properties initConfig() {
		Properties props = new Properties();
		props.put("key.deserializer",
				"org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer",
				"org.apache.kafka.common.serialization.StringDeserializer");
		props.put("bootstrap.servers", brokerList);
		// group.id和client.id不同的作用和意义
		props.put("group.id", groupId);
		props.put("client.id", "consumer.client.id.fishyu080103");

		props.put("enable.auto.commit", "false");
		return props;
	}
	public static void main(String[] args) {
		Properties properties = initConfig();
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
		// 订阅topic
//		consumer.subscribe(Arrays.asList(topic));

		consumer.assign(Collections.singleton(new TopicPartition("EOC_ALERT_MESSAGE_TOPIC", 0)));

		try {
			while(isRunning.get()){
				// 拉取消息和ConsumerRecords对象
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

				// ConsumerRecord对象
				for(ConsumerRecord<String, String> record : records){
					System.out.println(record.toString());
				}
				consumer.commitSync();
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} finally {
			consumer.close();
		}
	}
}
