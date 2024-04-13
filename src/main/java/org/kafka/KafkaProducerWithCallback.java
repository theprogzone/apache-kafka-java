package org.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

@Slf4j
public class KafkaProducerWithCallback {

    public static void main(String[] args) {

        log.info("Kafka producer with call back");

        Properties properties = new Properties();

        // Connect to localhost
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // Set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.setProperty("batch.size", "5");
        properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());

        // Create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i=0; i<10; i++) {

            // Create producer record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("kafka_topic_1", "Message : "+(i+1));

            // Send data
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        log.info("Received new metadata, Topic: {}, Partition: {}, Offset: {}, Timestamp: {}",
                                recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp());
                    } else {
                        log.error("Error while producing: {}", e.getMessage());
                    }
                }
            });
        }

        //Flush and close the producer
        producer.flush();
        producer.close();
    }
}