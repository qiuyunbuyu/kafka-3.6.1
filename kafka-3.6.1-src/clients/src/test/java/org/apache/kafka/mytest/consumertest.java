package org.apache.kafka.mytest;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class consumertest {
	public static final String brokerList = "localhost:9092";
	public static final String topic = "studytopic";
	public static final String groupId = "group.fishyu080103";
	public static final AtomicBoolean isRunning = new AtomicBoolean(true);

	public static Properties initConfig() {
		// 必须指定的参数
		Properties props = new Properties();
		props.put("key.deserializer",
				"org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer",
				"org.apache.kafka.common.serialization.StringDeserializer");
		props.put("bootstrap.servers", brokerList);
		// group.id和client.id不同的作用和意义
		props.put("group.id", groupId);
		props.put("client.id", "consumer.client.id.fishyu080103");

		props.put("isolation.level", "read_committed");
		return props;
	}
	public static void main(String[] args) {
		Properties properties = initConfig();
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
		// 订阅topic
		consumer.subscribe(Arrays.asList(topic));


		try {
			while(isRunning.get()){
				// 拉取消息和ConsumerRecords对象
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

				// ConsumerRecord对象
				for(ConsumerRecord<String, String> record : records){
					System.out.println(record.toString());
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} finally {
			consumer.close();
		}
	}
}
