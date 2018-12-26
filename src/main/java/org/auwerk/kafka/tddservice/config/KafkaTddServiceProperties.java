package org.auwerk.kafka.tddservice.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import lombok.Data;

@Data
@ConfigurationProperties(prefix = "kafka-tdd-service")
public class KafkaTddServiceProperties {
	
	private String topicId;

}
