package hadoop_kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

/**
 * @author movw
 * @date 2018年8月17日 下午8:25:47
 * @version 1.0
 */
public class CustomConsumer {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) {
		Properties properties = new Properties();

		properties.put("zookeeper.connect", "bigdata111:2181");
		properties.put("group.id", "g1");
		properties.put("zookeeper.session.timeout.ms", "500");
		properties.put("zookeeper.sync.time.ms", "250");
		properties.put("auto.commit.interval.ms", "1000");

		// 创建消费者连接器
		ConsumerConnector consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));

		HashMap<String, Integer> topicCount = new HashMap<>();
		topicCount.put("first", 1);

		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCount);

		KafkaStream<byte[], byte[]> stream = consumerMap.get("first").get(0);

		ConsumerIterator<byte[], byte[]> it = stream.iterator();

		while (it.hasNext()) {
			System.out.println(new String(it.next().message()));
		}
	}

}
