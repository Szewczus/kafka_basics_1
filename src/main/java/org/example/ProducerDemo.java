package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {
    private static Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());
    public static void main(String[] args) {

        //create producer properties
        Properties properties = new Properties();

        //connect to localhost
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());


        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        //create producer record:
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo-java", "Hello_world5");
        //send data
        producer.send(producerRecord);
        //flush and close the producer
        //tell the producer to send all data and block util done -- synchronouse
        producer.flush();
        //close producer:
        producer.close();
    }
}