package org.auwerk.kafka.tddservice;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.auwerk.kafka.tddservice.service.HelloKafkaService;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * Kafka integration tests in Spring context
 * 
 * @author auwerk
 *
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class KafkaSpringIntegrationTests {
	private static final String MY_KEY = "my-key";
	private static final String MY_VALUE = "my-value";
	private static final String MY_VALUE_2 = "my-value-2";

	@ClassRule
	public static EmbeddedKafkaRule embeddedKafkaRule = new EmbeddedKafkaRule(1, true);

	@Value("${kafka-tdd-service.topic-id:undefined-topic}")
	private String topicId;

	@Autowired
	private ConsumerFactory<String, String> kafkaConsumerFactory;
	@Autowired
	private ProducerFactory<String, String> kafkaProducerFactory;
	@Autowired
	private HelloKafkaService helloKafkaService;

	private Producer<String, String> kafkaProducer;
	private Consumer<String, String> kafkaConsumer;

	@Before
	public void before() {
		kafkaProducer = kafkaProducerFactory.createProducer();

		kafkaConsumer = kafkaConsumerFactory.createConsumer();
		kafkaConsumer.subscribe(Collections.singleton(topicId));
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
	public void kafkaSpringIntegrationWorks() {
		kafkaProducer.send(new ProducerRecord<String, String>(topicId, MY_KEY, MY_VALUE));
		kafkaProducer.flush();

		var consumerRecord = KafkaTestUtils.getSingleRecord(kafkaConsumer, topicId);

		Assert.assertNotNull(consumerRecord);
		Assert.assertEquals(MY_KEY, consumerRecord.key());
		Assert.assertEquals(MY_VALUE, consumerRecord.value());
	}

	@Test
	public void kafkaHelloServiceWorks() {
		kafkaProducer.send(new ProducerRecord<String, String>(topicId, MY_KEY, MY_VALUE_2));
		kafkaProducer.flush();

		List<String> values = helloKafkaService.pollValues();
		Assert.assertTrue(values.size() == 2);
		Assert.assertEquals(String.format("%s:%s", MY_KEY, MY_VALUE_2), values.get(1));
	}

	@TestConfiguration
	public static class KafkaSpringIntegrationTestsConfiguration {

		@Bean
		public ProducerFactory<?, ?> kafkaProducerFactory() {
			var producerConfigs = new HashMap<>(KafkaTestUtils.producerProps(embeddedKafkaRule.getEmbeddedKafka()));
			return new DefaultKafkaProducerFactory<>(producerConfigs, new StringSerializer(), new StringSerializer());
		}

		@Bean
		public ConsumerFactory<?, ?> kafkaConsumerFactory() {
			var consumerConfigs = new HashMap<>(
					KafkaTestUtils.consumerProps("consumer", "false", embeddedKafkaRule.getEmbeddedKafka()));
			consumerConfigs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			return new DefaultKafkaConsumerFactory<>(consumerConfigs, new StringDeserializer(),
					new StringDeserializer());
		}

	}

}
