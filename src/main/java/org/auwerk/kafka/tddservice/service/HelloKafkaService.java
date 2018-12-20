package org.auwerk.kafka.tddservice.service;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class HelloKafkaService {

	@Value("${kafka-tdd-service.topic-id:undefined-topic}")
	private String topicId;

	@Autowired
	private ConsumerFactory<String, String> kafkaConsumerFactory;

	public List<String> pollValues() {
		try (Consumer<String, String> kafkaConsumer = kafkaConsumerFactory.createConsumer()) {
			kafkaConsumer.subscribe(Collections.singletonList(topicId));

			final List<String> values = new ArrayList<>();
			ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));
			log.info("polled {} consumer records", consumerRecords.count());
			consumerRecords.forEach(consumerRecord -> {
				log.info("record: {}", consumerRecord);
				values.add(String.format("%s:%s", consumerRecord.key(), consumerRecord.value()));
			});
			return values;
		}
	}

}
