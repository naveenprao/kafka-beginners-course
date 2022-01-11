package com.github.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

@Slf4j
public class ProducerDemo {
    public static void main(String[] args) throws InterruptedException {
        // Create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // Create the producers
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        // Producer Record
        ProducerRecord<String, String> record = new ProducerRecord<>("second_topic", "hello world");
        // Send data
        producer.send(record);
        producer.flush();
        // send data with callback();
        producer.send(record, (metadata, exception) -> {
            if (exception == null) {
                log.info("*************** metadata.toString() ****************\n" + metadata.toString());
            } else {
                log.error(exception.getStackTrace().toString());
            }
        });

        // send data with key and callback
        for (int i = 0; i < 20; i++) {
            String key = i % 2 == 0 ? "even" : "odd";
            record = new ProducerRecord<>("second_topic", key, "hello world #" + i);
            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    log.info("*************** metadata.toString() ****************\n" + metadata.toString());
                } else {
                    log.error(exception.getStackTrace().toString());
                }
            });
            Thread.sleep(500);
        }
        producer.close();
    }
}
