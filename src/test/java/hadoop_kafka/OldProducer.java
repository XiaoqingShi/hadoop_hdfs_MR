package hadoop_kafka;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * @author movw
 * @date 2018年8月17日 下午8:12:59
 * @version 1.0
 */
public class OldProducer {
	@SuppressWarnings("deprecation")
	public static void main(String[] args) {

		Properties properties = new Properties();
		properties.put("metadata.broker.list", "bigdata111:9092");
		properties.put("request.required.acks", "1");
		properties.put("serializer.class", "kafka.serializer.StringEncoder");

		Producer<Integer, String> producer = new Producer<Integer, String>(new ProducerConfig(properties));

		KeyedMessage<Integer, String> message = new KeyedMessage<Integer, String>("first", "hello world");
		producer.send(message);
	}

}
