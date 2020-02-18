package com.flyingj.jmssqs.messageReceiver.service;

import com.amazon.sqs.javamessaging.SQSConnection;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.util.Base64;
import com.flyingj.jmssqs.messageReceiver.config.SqsProperties;
import lombok.Data;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.jms.*;
import java.util.concurrent.TimeUnit;

@Service
public class MessageService {
    private static final int listenerDuration = 1;

    @Autowired
    SqsProperties sqsProperties;

    @Autowired
    SQSConnection sqsConnection;

    @Autowired
    MessageConsumer sqsMessageConsumer;

    @Autowired
    Session sqsSession;

    public void process() throws JMSException {
        sqsConnection.start();
        receiveMessages(sqsMessageConsumer);
    }

    private void deleteMessage(String messageId) throws JMSException {
        sqsConnection.getWrappedAmazonSQSClient().deleteMessage(
                new DeleteMessageRequest(sqsProperties.getQueueName(), messageId));
        System.out.println("Message deleted!");
    }

    private void receiveMessages(MessageConsumer consumer ) {
        try {
            while( true ) {
                System.out.println( "Waiting for messages");
                // Wait 1 minute for a message
                Message message = consumer.receive(TimeUnit.MINUTES.toMillis(listenerDuration));
                if( message == null ) {
                    System.out.println( "Shutting down after 1 minute of silence" );
                    break;
                }
                message.acknowledge();
                handleMessage(message);
                System.out.println( "Acknowledged message " + message.getJMSMessageID() );
                if (message.getJMSMessageID() != null) {
//                    deleteMessage(message.getJMSMessageID());
                }
            }
        } catch (JMSException e) {
            System.err.println( "Error receiving from SQS: " + e.getMessage() );
            e.printStackTrace();
        }
    }

    public void handleMessage(Message message) throws JMSException {
        System.out.println( "Got message " + message.getJMSMessageID() );
        System.out.println( "Content: ");
        if( message instanceof TextMessage) {
            TextMessage txtMessage = ( TextMessage ) message;
            System.out.println( "\t" + txtMessage.getText() );
        } else if( message instanceof BytesMessage){
            BytesMessage byteMessage = ( BytesMessage ) message;
            // Assume the length fits in an int - SQS only supports sizes up to 256k so that
            // should be true
            byte[] bytes = new byte[(int)byteMessage.getBodyLength()];
            byteMessage.readBytes(bytes);
            System.out.println( "\t" +  Base64.encodeAsString( bytes ) );
        } else if( message instanceof ObjectMessage) {
            ObjectMessage objMessage = (ObjectMessage) message;
            System.out.println( "\t" + objMessage.getObject() );
        }
    }

}
