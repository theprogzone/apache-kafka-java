package org.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

@Slf4j
public class KafkaProducer {

    public static void main(String[] args) {

        log.info("Kafka producer");

        Properties properties = new Properties();

        // Connect to localhost
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // Set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // Create producer
        org.apache.kafka.clients.producer.KafkaProducer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<>(properties);

        // Create producer record
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("kafka_topic_1", "This is first message!");

        // Send data
        producer.send(producerRecord);

        //Flush and close the producer
        producer.flush();
        producer.close();
    }
}