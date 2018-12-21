package org.auwerk.kafka.tddservice.service;

import java.util.concurrent.CountDownLatch;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class KafkaListenerService {

	@Getter
	private final CountDownLatch latch = new CountDownLatch(1);
	@Getter
	private String recentMessage;

	@KafkaListener(topics = "#{'${kafka-tdd-service.topic-id:undefined-topic}'}", groupId = "listener-svc")
	public void listen(final String message) {
		log.info("got a message! here it is: {}", message);
		recentMessage = message;
		latch.countDown();
	}

}
