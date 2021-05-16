package com.github.simranp.demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

public class ProducerDemo {
  public static void main(String[] args) {
    //create properties
    Properties properties = new Properties();
    String bootstrapServers = "127.0.0.1:9092";
    properties.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    //create kafka producer
    KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

    //create producer record
    ProducerRecord<String, String> message = new ProducerRecord<>("hello_world", "Hi, I am Java Producer");

    //send message
    kafkaProducer.send(message);

    //flush message
    kafkaProducer.close();
  }
}
