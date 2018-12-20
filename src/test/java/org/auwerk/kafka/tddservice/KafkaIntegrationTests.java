package org.auwerk.kafka.tddservice;

import java.util.Collections;
import java.util.HashMap;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.KafkaTestUtils;

/**
 * Simple Kafka integration tests
 * 
 * @author auwerk
 *
 */
public class KafkaIntegrationTests {
	private static final String MY_TOPIC_ID = "my-topic";
	private static final String MY_KEY = "my-key";
	private static final String MY_VALUE = "my-value";

	@ClassRule
	public static EmbeddedKafkaRule embeddedKafkaRule = new EmbeddedKafkaRule(1, true, MY_TOPIC_ID);

	private Producer<String, String> kafkaProducer;
	private Consumer<String, String> kafkaConsumer;

	@Before
	public void before() {
		// Create producer
		var producerConfigs = new HashMap<>(KafkaTestUtils.producerProps(embeddedKafkaRule.getEmbeddedKafka()));
		kafkaProducer = new DefaultKafkaProducerFactory<>(producerConfigs, new StringSerializer(),
				new StringSerializer()).createProducer();

		// Create consumer
		var consumerConfigs = new HashMap<>(
				KafkaTestUtils.consumerProps("consumer", "false", embeddedKafkaRule.getEmbeddedKafka()));
		consumerConfigs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		kafkaConsumer = new DefaultKafkaConsumerFactory<>(consumerConfigs, new StringDeserializer(),
				new StringDeserializer()).createConsumer();
		kafkaConsumer.subscribe(Collections.singleton(MY_TOPIC_ID));
	}

	@After
	public void after() {
		if (kafkaConsumer != null) {
			kafkaConsumer.close();
			kafkaConsumer = null;
		}
		if (kafkaProducer != null) {
			kafkaProducer.close();
			kafkaProducer = null;
		}
	}

	@Test
	public void kafkaIntegrationWorks() {
		kafkaProducer.send(new ProducerRecord<String, String>(MY_TOPIC_ID, MY_KEY, MY_VALUE));
		kafkaProducer.flush();

		var consumerRecord = KafkaTestUtils.getSingleRecord(kafkaConsumer, MY_TOPIC_ID);

		Assert.assertNotNull(consumerRecord);
		Assert.assertEquals(MY_KEY, consumerRecord.key());
		Assert.assertEquals(MY_VALUE, consumerRecord.value());
	}

}
