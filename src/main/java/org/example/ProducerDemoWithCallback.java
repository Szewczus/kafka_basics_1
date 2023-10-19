package org.example;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    private static Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());

    public static void main(String[] args) {

        //create producer properties
        Properties properties = new Properties();

        //connect to localhost
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        //properties.setProperty("batch.size", "40");

        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int j=0; j<10; j++){
            for (int i=0; i<30; i++){
                //create producer record:
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo-java1", "Hello_world5");
                //send data
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e==null){
                            // the record was successfully send
                            log.info("Received metadata: "+
                                    " Topic: "+ recordMetadata.topic() +
                                    " Partition: " + recordMetadata.partition() +
                                    " Offset: " + recordMetadata.offset() +
                                    " Timestamp" + recordMetadata.timestamp()
                            );
                        }
                    }
                });
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }


        //flush and close the producer
        //tell the producer to send all data and block util done -- synchronouse
        producer.flush();
        //close producer:
        producer.close();
    }
}
