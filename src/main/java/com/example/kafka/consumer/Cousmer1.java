package com.example.kafka.consumer;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class Cousmer1 {

    public static void main(String args[]) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("enable.auto.commit", "false");
        properties.setProperty("group.id", "1111");


        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
        kafkaConsumer.subscribe(Collections.singletonList("test-topic"));
        Map<TopicPartition, OffsetAndMetadata> offsets=new HashMap<>();
        offsets.put(new TopicPartition("test-topic",5),new OffsetAndMetadata(0,"userID"));
        try {
            while (true) {
                ConsumerRecords<String, String> poll = kafkaConsumer.poll(500);
                for (ConsumerRecord<String, String> context : poll) {
                    System.out.println("消息所在分区:" + context.partition() + "-消息的偏移量:" + context.offset() + " key:" + context.key() + " value:" + context.value());

                }
                //异步提交偏移量
                kafkaConsumer.commitAsync();
                //手动指定同步的偏移量（不建议，容易丢失数据）
//                kafkaConsumer.commitSync(offsets);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            //同步提交偏移量
            kafkaConsumer.commitSync();
        }


    }
}
