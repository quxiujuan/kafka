package com.example.kafka.consumer;



import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

public class Cousmer {

    public static void main(String args[]) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", "1111");


        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
        kafkaConsumer.subscribe(Collections.singletonList("test-topic"));

        while (true) {
            ConsumerRecords<String, String> poll = kafkaConsumer.poll(500);
            for (ConsumerRecord<String, String> context : poll) {
                System.out.println("消息所在分区:" + context.partition() + "-消息的偏移量" + context.offset() + "key:" + context.key() + "value:" + context.value());


            }
        }


    }
}
