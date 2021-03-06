package com.example.kafka.product;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class Producer {
    public static void main(String args[]) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("test-topic", "create.key", "ren");
        ProducerRecord<String, String> record1 = new ProducerRecord<String, String>("test-topic", "1", "juan");
        Future<RecordMetadata> send = kafkaProducer.send(record);
        Future<RecordMetadata> send1 = kafkaProducer.send(record1);
        try {
            RecordMetadata recordMetadata = send.get();
            System.out.println("偏移量："+recordMetadata.offset()+" --topic:"+recordMetadata.topic());

        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        kafkaProducer.flush();
        kafkaProducer.close();

    }
}
