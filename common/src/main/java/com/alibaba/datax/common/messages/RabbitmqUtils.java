package com.alibaba.datax.common.messages;

import com.rabbitmq.client.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;


public class RabbitmqUtils {
    private String rabbitHost;
    private String rabbitUsername;
    private String rabbitPassword;

    private String exchangeName;
    private String queueName;
    private String routingKey;

    private Connection connection;
    private Channel channel;

    public RabbitmqUtils(){
        String envPath = System.getProperty("datax.home");
        String properPath = envPath + "\\conf\\rabbitserver.properties";
        try{
            Properties properties = new Properties();
            FileInputStream fis = new FileInputStream(properPath);
            properties.load(fis);
            fis.close();
            rabbitHost = properties.getProperty("rabbit.host");
            rabbitUsername = properties.getProperty("rabbit.name");
            rabbitPassword = properties.getProperty("rabbit.password");
            exchangeName = properties.getProperty("rabbit.exchangename");
            queueName = properties.getProperty("rabbit.queuename");
            routingKey = properties.getProperty("rabbit.routingkey");
            getConnection();
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    public void finalize(){
        try{
            channel.close();
            connection.close();
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    private void getConnection(){
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost(rabbitHost);
        connectionFactory.setPort(AMQP.PROTOCOL.PORT);
        connectionFactory.setUsername(rabbitUsername);
        connectionFactory.setPassword(rabbitPassword);
        try{
            this.connection = connectionFactory.newConnection();
            channel = connection.createChannel(1);
            channel.queueDeclare(queueName, false, false, true, null);
            channel.exchangeDeclare(exchangeName,"direct",false,true,null);
            channel.queueBind(queueName, exchangeName, routingKey);
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    public void produceMessage(String msg){
        try{
//            Channel channel = connection.createChannel(1);
//            channel.queueDeclare(queueName, true, false, false, null);
//            channel.exchangeDeclare(exchangeName,"direct",true,false,null);
//            channel.queueBind(queueName, exchangeName, routingKey);
            String message = msg;
            channel.basicPublish(exchangeName, routingKey, null, message.getBytes());
//            channel.close();
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    public void consumeMessage(){
        try{
//            Channel channel = connection.createChannel(1);
//            channel.queueDeclare(queueName, false, false, false, null);
//            channel.exchangeDeclare(exchangeName,"direct",false,false,null);
//            channel.queueBind(queueName, exchangeName, routingKey);
            Consumer consumer = new DefaultConsumer(channel){
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)throws IOException{
                    /*try{
                        Thread.sleep(2000);
                    }catch(InterruptedException e){
                        e.printStackTrace();
                    }*/
                    StringBuffer message = new StringBuffer();
                    super.handleDelivery(consumerTag, envelope, properties, body);
                    message.append(new String(body, "UTF-8"));
                    System.out.println(message.toString());
                }
            };
            channel.basicConsume(queueName, true, consumer);
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    public static void main(String[] args){
        try{
            RabbitmqUtils rabbitmqUtils = new RabbitmqUtils();
            //Thread.sleep(20);
            rabbitmqUtils.consumeMessage();
        }catch(Exception e){
            e.printStackTrace();
        }
    }
}
