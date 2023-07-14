package org.chench.extra.rabbitmq;

import com.rabbitmq.client.*;
import lombok.SneakyThrows;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * 死信机制
 *
 * 当给RabbitMQ中的消息设置了TTL时，满足如下三个条件将被重新转发到死信交换器：
 * 1.消息超时
 * 2.消息未被确认
 * 3.超过队列长度而被丢弃
 *
 * @author chench
 * @desc org.chench.extra.rabbitmq.DeadLetterSimple
 * @date 2023.07.14
 */
public class DeadLetterSimple {
    // MQ信息
    private final static String MQ_HOST = "192.168.100.20";
    private final static String MQ_USER = "admin";
    private final static String MQ_PASS = "admin";

    // 死信交换器，用于转发死信消息
    private final static String DEAD_LETTER_EXCHANGE = "dead_letter_exchange";

    // 死信进入到死信交换器之后新的routing-key
    private final static String DEAD_LETTER_ROUTING_KEY ="dead-letter-routing-key";

    // 保存带超时时间的消息的源队列
    private final static String DEAD_LETTER_QUEUE_ORIGIN = "dead_letter_queue_origin";

    // 最终从死信交换器读取数据的目标队列
    private final static String DEAD_LETTER_QUEUE_TARGET = "dead_letter_queue_target";

    // 生产死信消息
    private void produce(String message, long expiration) throws IOException, TimeoutException {
        try (Connection connection = buildConnectionFactory().newConnection(); Channel channel = connection.createChannel()) {
            // 定义死信交换器，也是一个普通的交换器
            channel.exchangeDeclare(DEAD_LETTER_EXCHANGE, "direct");

            // 定义死信队列，用于保存死信消息，并指定死信交换器
            // 这样进入该死信队列的消息在超时之后就会被重新转发到指定的交换器
            Map<String, Object> args = new HashMap<>();
            args.put("x-dead-letter-exchange", DEAD_LETTER_EXCHANGE);
            args.put("x-dead-letter-routing-key", DEAD_LETTER_ROUTING_KEY);
            channel.queueDeclare(DEAD_LETTER_QUEUE_ORIGIN, false, false, false, args);

            // 将消息发布到默认交换器，默认交换器隐含绑定到每一个队列，routing-key等于队列名称
            // 给发布到交换器的消息都设置一个超时时间
            AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                    //.expiration("60000")
                    .expiration(String.valueOf(expiration))
                    .build();
            channel.basicPublish("", DEAD_LETTER_QUEUE_ORIGIN, properties, message.getBytes());
            System.out.println(String.format("[x] Sent '%s' %s", message, new Date()));
        }
    }

    // 消费死信消息
    private void consume() throws IOException, TimeoutException {
        Connection connection = buildConnectionFactory().newConnection();
        Channel channel = connection.createChannel();

        // 定义消费数据的队列，该队列与死信交换器进行绑定
        channel.queueDeclare(DEAD_LETTER_QUEUE_TARGET, false, false, false, null);
        channel.queueBind(DEAD_LETTER_QUEUE_TARGET, DEAD_LETTER_EXCHANGE, DEAD_LETTER_ROUTING_KEY);
        System.out.println(String.format("[*] Waiting for message..."));

        DeliverCallback callback = new DeliverCallback() {
            @Override
            public void handle(String consumerTag, Delivery delivery) throws IOException {
                String message = new String(delivery.getBody(), "UTF-8");
                System.out.println(String.format("[x] received message: %s %s", message, new Date()));
            }
        };
        // 从指定队列消费
        channel.basicConsume(DEAD_LETTER_QUEUE_TARGET, true, callback, consumerTag -> {});
    }

    private ConnectionFactory buildConnectionFactory() {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(MQ_HOST);
        factory.setUsername(MQ_USER);
        factory.setPassword(MQ_PASS);
        return factory;
    }

    private void startConsumer() throws InterruptedException {
        new Thread(new Runnable() {
            @SneakyThrows
            @Override
            public void run() {
                consume();
            }
        }).join();
    }

    private void doSleep(int seconds) {
        try {
            Thread.sleep(seconds * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException, TimeoutException {
        DeadLetterSimple simple = new DeadLetterSimple();
        simple.consume();
        //simple.produce("hello_60000", 60000);

        // 验证一个问题：
        // 消息只有到达队列头部的时候才可能会成为死信；如果并未到达队列头部即使超时时间到了，也不会成为死信被转发的
        simple.produce("hello1_60000", 60000);
        simple.doSleep(3);

        simple.produce("hello2_60000", 60000);
        simple.doSleep(3);

        // 这个消息会最先超时但是因为不在队列头部，不能最先得到处理
        simple.produce("hello3_30000", 30000);
    }

}
