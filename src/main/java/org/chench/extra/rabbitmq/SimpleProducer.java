package org.chench.extra.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 简单的消息发送者
 *
 * https://www.rabbitmq.com/tutorials/tutorial-one-java.html
 *
 * @author chench
 * @desc org.chench.extra.rabbitmq.SimpleProducer
 * @date 2023.07.14
 */
public class SimpleProducer {
    private final static String QUEUE_NAME = "hello";

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("192.168.100.20");
        factory.setUsername("admin");
        factory.setPassword("admin");
        try (Connection connection = factory.newConnection();Channel channel = connection.createChannel()) {
            // 定义队列
            channel.queueDeclare(QUEUE_NAME, false, false, false, null);
            String message = "Hello,World!";
            // 将消息发布到默认交换器，默认交换器隐含绑定到每一个队列，routing-key等于队列名称
            channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
            System.out.println(String.format("[x] Sent '%s'", message));
        }
    }

}
