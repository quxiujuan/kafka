package com.example.kafka.consumer;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;

/**
 * 独立消费者
 */
public class OneCousmer {

    public static void main(String args[]) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("enable.auto.commit", "false");


        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
        List<TopicPartition> list=new ArrayList<>();
        list.add(new TopicPartition("test-topic",7));
//        kafkaConsumer.subscribe(Collections.singletonList("test-topic"));
        kafkaConsumer.assign(list);
        try {
            while (true) {
                ConsumerRecords<String, String> poll = kafkaConsumer.poll(500);
                for (ConsumerRecord<String, String> context : poll) {
                    System.out.println("消息所在分区:" + context.partition() + "-消息的偏移量:" + context.offset() + " key:" + context.key() + " value:" + context.value());

                }
                //异步提交偏移量
                kafkaConsumer.commitAsync();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            //同步提交偏移量
            kafkaConsumer.commitSync();
        }


    }
}
