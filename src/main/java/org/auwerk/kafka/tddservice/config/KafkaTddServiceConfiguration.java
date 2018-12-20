package org.auwerk.kafka.tddservice.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;

@Configuration
@EnableKafka
@EnableConfigurationProperties(KafkaTddServiceProperties.class)
public class KafkaTddServiceConfiguration {

}
