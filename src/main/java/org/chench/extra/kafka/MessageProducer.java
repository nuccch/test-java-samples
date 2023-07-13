package org.chench.extra.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.Properties;

/**
 * Kafka message producer
 *
 * https://kafka.apache.org/documentation/
 * https://kafka.apache.org/documentation/#producerapi
 *
 * @author chench
 * @desc org.chench.extra.kafka.MessageProducer
 * @date 2022.11.02
 */
@Slf4j
public class MessageProducer {
    private String bootstrapServers = null;

    public MessageProducer(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public void sendMessage(String topic, String message) {
        // create instance for properties to access producer configs
        Properties props = new Properties();
        //Assign localhost id
        props.put("bootstrap.servers", this.bootstrapServers);
        //Set acknowledgements for producer requests.
        props.put("acks", "all");
        //If the request fails, the producer can automatically retry,
        props.put("retries", 3);
        //Specify buffer size in config
        props.put("batch.size", 16384);
        //Reduce the no of requests less than 0
        props.put("linger.ms", 1);
        //The buffer.memory controls the total amount of memory available to the producer for buffering.
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        log.info("Before send: {}", message);
        producer.send(new ProducerRecord<String, String>(topic,  message));
        log.info("After send.");
        producer.close(Duration.ofSeconds(1));
    }
}
