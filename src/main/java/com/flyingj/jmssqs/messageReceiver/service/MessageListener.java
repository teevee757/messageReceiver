package com.flyingj.jmssqs.messageReceiver.service;

import com.amazon.sqs.javamessaging.SQSConnection;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.flyingj.jmssqs.messageReceiver.config.SqsProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.TextMessage;

@Service
public class MessageListener implements javax.jms.MessageListener {
    @Autowired
    MessageConsumer messageConsumer;

    @Autowired
    SQSConnection sqsConnection;

    @Autowired
    MessageListener messageListener;

    @Autowired
    SqsProperties sqsProperties;


    public void processAsync() throws JMSException {
        messageConsumer.setMessageListener(messageListener);
        sqsConnection.start();
    }
    @Override
    public void onMessage(Message message) {
        try {
            System.out.println("Received: " + ((TextMessage) message).getText());
            processMessage(message);
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    private void processMessage(Message message) throws JMSException {
        message.acknowledge();
        System.out.println("Processing Message...");
        sqsConnection.getAmazonSQSClient().deleteMessage(
                new DeleteMessageRequest(sqsProperties.getQueueName(), message.getJMSMessageID()));
        System.out.println("Message deleted!");
    }
}
