package com.flyingj.jmssqs.messageReceiver;

import com.amazon.sqs.javamessaging.ProviderConfiguration;
import com.amazon.sqs.javamessaging.SQSConnection;
import com.amazon.sqs.javamessaging.SQSConnectionFactory;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.flyingj.jmssqs.messageReceiver.config.ApplicationContextProvider;
import com.flyingj.jmssqs.messageReceiver.config.SqsProperties;
import com.flyingj.jmssqs.messageReceiver.service.MessageListener;
import com.flyingj.jmssqs.messageReceiver.service.MessageService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;

@SpringBootApplication
@EnableConfigurationProperties(SqsProperties.class)
public class MessageReceiverApplication implements CommandLineRunner {

    @Autowired
    ApplicationContextProvider applicationContextProvider;

    @Autowired
    MessageService messageService;

    @Autowired
    MessageListener messageListener;

    public static void main(String[] args) {
        SpringApplication.run(MessageReceiverApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        if (args[0].equalsIgnoreCase("Async")) {
        	messageListener.processAsync();
        } else {
            messageService.process();
			System.exit(0);
        }
	}

    @Bean
    SQSConnectionFactory sqsClient(SqsProperties sqsProperties) {
        SQSConnectionFactory connectionFactory = new SQSConnectionFactory(
                new ProviderConfiguration(), AmazonSQSClient.builder().withEndpointConfiguration(
                new AwsClientBuilder.EndpointConfiguration(sqsProperties.getEndpointURI(), sqsProperties.getRegion()))
                .build());
        return connectionFactory;
    }

    @Bean
    SQSConnection sqsConnection(SQSConnectionFactory sqsConnectionFactory) throws JMSException {
        return sqsConnectionFactory.createConnection();
    }

    @Bean
    Session sqsSession(SQSConnection sqsConnection) throws JMSException {
        return sqsConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
    }

    @Bean
    MessageConsumer sqsMessageConsumer(Session sqsSession, SqsProperties sqsProperties) throws JMSException {
        return sqsSession.createConsumer(sqsSession.createQueue(sqsProperties.getQueueName()));
    }

}
