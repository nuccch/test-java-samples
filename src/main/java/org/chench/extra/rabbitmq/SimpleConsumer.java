package org.chench.extra.rabbitmq;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 简单的消息消费者
 *
 * https://www.rabbitmq.com/tutorials/tutorial-one-java.html
 *
 * @author chench
 * @desc org.chench.extra.rabbitmq.SimpleConsumer
 * @date 2023.07.14
 */
public class SimpleConsumer {
    private final static String QUEUE_NAME = "hello";

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("192.168.100.20");
        factory.setUsername("admin");
        factory.setPassword("admin");

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        // 先定义队列
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        System.out.println(String.format("[*] Waiting for message..."));

        DeliverCallback callback = new DeliverCallback() {
            @Override
            public void handle(String consumerTag, Delivery delivery) throws IOException {
                String message = new String(delivery.getBody(), "UTF-8");
                System.out.println(String.format("[x] received message: %s", message));
            }
        };
        // 从指定队列消费
        channel.basicConsume(QUEUE_NAME, true, callback, consumerTag -> {});
    }
}
