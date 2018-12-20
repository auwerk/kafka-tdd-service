package org.auwerk.kafka.tddservice.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@ConfigurationProperties(prefix = "kafka-tdd-service")
public class KafkaTddServiceProperties {
	
	private String topicId;

}
