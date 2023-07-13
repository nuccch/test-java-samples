package org.chench.extra.kafka;

import org.junit.Before;
import org.junit.Test;

/**
 * Send to or consumer from kafka broker
 *
 * @author chench
 * @desc org.chench.extra.kafka.KafkaMessageTest
 * @date 2022.11.02
 */
public class KafkaMessageTest {
    private MessageProducer producer;
    private MessageConsumer consumer;
    private String topic = "test_topic";

    @Before
    public void setup() {
        String bootstrapServers = "192.168.56.104:9092";
        producer = new MessageProducer(bootstrapServers);
        consumer = new MessageConsumer(bootstrapServers);
    }

    @Test
    public void testSendMessage() {
        producer.sendMessage(topic, "HelloWorld!");
    }

    @Test
    public void testConsumer() {
        consumer.consume(topic, "test_topic_group");
    }
}
