package com.github.simranp.demo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

public class ConsumerDemo {
  public static void main(String[] args) {
    Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);
    //create properties
    Properties properties = new Properties();
    String bootstrapServers = "127.0.0.1:9092";
    String groupId = "hello_world_application";
    String topicName = "hello_world";

    properties.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.put(GROUP_ID_CONFIG, groupId);
    properties.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
    //create consumer
    KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
    //subscribe to topic(s)
    kafkaConsumer.subscribe(Collections.singleton(topicName));
    //poll
    while (true) {
      ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
      for (ConsumerRecord<String, String> rec : records) {
        logger.info("Key: " + rec.key() + ", Value: " + rec.value());
        logger.info("Partition: " + rec.partition() + ", Offset:" + rec.offset());
      }
    }
  }
}
