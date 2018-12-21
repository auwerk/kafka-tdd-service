package org.auwerk.kafka.tddservice.service;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class KafkaListenerService {
	
	private List<CompletableFuture<String>> waiters = new LinkedList<>();
	
	@KafkaListener(topics = "#{'${kafka-tdd-service.topic-id:undefined-topic}'}", groupId = "listener-svc")
	public void listen(final String message) {
		log.info("got a message! here it is: {}", message);
		waiters.forEach(future -> future.complete(message));
		waiters.clear();
	}
	
	public CompletableFuture<String> getRecentMessage() {
		var future = new CompletableFuture<String>();
		waiters.add(future);
		return future;
	}

}
