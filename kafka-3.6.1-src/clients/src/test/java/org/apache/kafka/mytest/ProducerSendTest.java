package org.apache.kafka.mytest;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import javax.management.*;
import java.lang.management.ManagementFactory;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class ProducerSendTest {
	public static final String brokerList = "kafka9001.eniot.io:9092";
	public static final String topic = "topic-fishyu";

	public static void main(String[] args) throws ExecutionException, InterruptedException, MalformedObjectNameException, ReflectionException, AttributeNotFoundException, InstanceNotFoundException, MBeanException {
		Properties properties = new Properties();
		properties.put("key.serializer",
				"org.apache.kafka.common.serialization.IntegerSerializer");
		properties.put("value.serializer",
				"org.apache.kafka.common.serialization.StringSerializer");
		properties.put("bootstrap.servers", brokerList);

//		properties.put("client-id", "kafka-producer-http-reporter");
//		properties.put("metric.reporters", TestHttpMetricsReporter.class.getName());
//		properties.put("metrics.url", "http://127.0.0.1:18975/metrics");
//		properties.put("metrics.period", "3");
//		properties.put("metrics.host", "1897");

		String mbeanName = "kafka.producer:type=producer-metrics,client-id=producer-1";
		MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
		ObjectName objectName = new ObjectName(mbeanName);
		String totalSentMessagesAttr = "record-send-total";



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

		for(int i = 0; i < 1000; i++){

			ProducerRecord<Integer, String> rx = new ProducerRecord<>(topic, i,"hello, producer1");
			producer.send(rx);
			Thread.sleep(1000);
			// 单例的MBeanServer对应，由于register的时候是KafkaMbean，所以get的时候也是调用的KafkaMbean的getAttribute(String name)方法来返回值
			double totalSentMessages = (double) mbs.getAttribute(objectName, totalSentMessagesAttr);
			System.out.println(totalSentMessages);
		}
		producer.close();
	}
}
