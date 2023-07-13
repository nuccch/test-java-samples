package org.chench.extra.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * Kafka message consumer
 *
 * https://kafka.apache.org/documentation/
 * https://kafka.apache.org/documentation/#consumerapi
 *
 * @author chench
 * @desc org.chench.extra.kafka.MessageConsumer
 * @date 2022.11.02
 */
@Slf4j
public class MessageConsumer {
    private String bootstrapServers = null;

    public MessageConsumer(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public void consume(String topic, String group) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", bootstrapServers);
        props.setProperty("group.id", group);
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String,String> consumer = new KafkaConsumer(props);
        consumer.subscribe(Arrays.asList(topic));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
            for (ConsumerRecord<String, String> record : records) {
                log.info("partition = {}, offset = {}, key = {}, value = {}", record.partition(),record.offset(), record.key(), record.value());
            }
        }
    }
}
