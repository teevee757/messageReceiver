package com.flyingj.jmssqs.messageReceiver.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@ConfigurationProperties("sqs")
@EnableConfigurationProperties(SqsProperties.class)
@Data
public class SqsProperties {
    private String endpointURI;
    private String queueName;
    private String region;
}
