package com.alibaba.datax.common.messages;

import com.rabbitmq.client.*;
import java.io.IOException;


public class RabbitmqUtils {
    private static final String RABBIT_HOST = "localhost";
    private static final String RABBIT_USERNAME = "guest";
    private static final String RABBIT_PASSWORD = "guest";

    private static final String EXCHANGE_NAME = "collection.message.exchange";
    private static final String QUEUE_NAME = "collection.message.queue";
    private static final String ROUTING_KEY = "collection.message.queue.key";

    public static Connection getConnection(){
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost(RABBIT_HOST);
        connectionFactory.setPort(AMQP.PROTOCOL.PORT);
        connectionFactory.setUsername(RABBIT_USERNAME);
        connectionFactory.setPassword(RABBIT_PASSWORD);
        Connection connection = null;
        try{
            connection = connectionFactory.newConnection();
        }catch(Exception e){
            e.printStackTrace();
        }
        return connection;
    }

    public static void produceMessage(String msg){
        Connection connection = getConnection();
        try{
            Channel channel = connection.createChannel(1);
            channel.queueDeclare(QUEUE_NAME, true, false, false, null);
            channel.exchangeDeclare(EXCHANGE_NAME,"direct",true,false,null);
            channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, ROUTING_KEY);
            String message = msg;
            channel.basicPublish(EXCHANGE_NAME, ROUTING_KEY, null, message.getBytes());
            System.out.println("producing...");
            channel.close();
            connection.close();
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    public static void consumeMessage(){
        Connection connection = getConnection();
        try{
            Channel channel = connection.createChannel(1);
            channel.queueDeclare(QUEUE_NAME, true, false, false, null);
            channel.exchangeDeclare(EXCHANGE_NAME,"direct",true,false,null);
            channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, ROUTING_KEY);
            Consumer consumer = new DefaultConsumer(channel){
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)throws IOException{
                    try{
                        Thread.sleep(2000);
                    }catch(InterruptedException e){
                        e.printStackTrace();
                    }
                    StringBuffer message = new StringBuffer();
                    super.handleDelivery(consumerTag, envelope, properties, body);
                    message.append(new String(body, "UTF-8"));
                    System.out.println(message.toString());
                }
            };
            channel.basicConsume(QUEUE_NAME, true, consumer);

        }catch(Exception e){
            e.printStackTrace();
        }
    }

    public static void main(String[] args){
        try{
            //produceMessage();
            Thread.sleep(2000);
            consumeMessage();
        }catch(Exception e){
            e.printStackTrace();
        }
    }
}
