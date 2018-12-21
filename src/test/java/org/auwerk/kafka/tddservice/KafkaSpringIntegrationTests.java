package org.auwerk.kafka.tddservice;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.auwerk.kafka.tddservice.service.KafkaListenerService;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
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

	@ClassRule
	public static EmbeddedKafkaRule embeddedKafkaRule = new EmbeddedKafkaRule(1, true);

	@Value("${kafka-tdd-service.topic-id:undefined-topic}")
	private String topicId;

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;
	@Autowired
	private KafkaListenerService kafkaListenerService;

	@Test
	public void kafkaListenerServiceWorks() throws InterruptedException {
		kafkaTemplate.send(topicId, MY_KEY, MY_VALUE);
		kafkaListenerService.getLatch().await(200, TimeUnit.MILLISECONDS);

		Assert.assertEquals(0, kafkaListenerService.getLatch().getCount());
		Assert.assertEquals(MY_VALUE, kafkaListenerService.getRecentMessage());
	}

	@TestConfiguration
	public static class KafkaSpringIntegrationTestsConfiguration {

		@Bean
		public ProducerFactory<String, String> kafkaProducerFactory() {
			var producerConfigs = new HashMap<>(KafkaTestUtils.producerProps(embeddedKafkaRule.getEmbeddedKafka()));
			producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
			producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
			return new DefaultKafkaProducerFactory<>(producerConfigs);
		}

		@Bean
		public KafkaTemplate<String, String> kafkaTemplate() {
			return new KafkaTemplate<>(kafkaProducerFactory());
		}

		@Bean
		public ConsumerFactory<String, String> kafkaConsumerFactory() {
			var consumerConfigs = new HashMap<>(
					KafkaTestUtils.consumerProps("consumer", "false", embeddedKafkaRule.getEmbeddedKafka()));
			consumerConfigs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
			consumerConfigs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
			return new DefaultKafkaConsumerFactory<>(consumerConfigs);
		}

		@Bean
		public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
			var containerFactory = new ConcurrentKafkaListenerContainerFactory<String, String>();
			containerFactory.setConsumerFactory(kafkaConsumerFactory());
			return containerFactory;
		}

	}

}
