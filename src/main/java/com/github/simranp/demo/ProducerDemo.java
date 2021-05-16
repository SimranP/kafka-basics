package com.github.simranp.demo;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

public class ProducerDemo {
  public static void main(String[] args) {
    Logger logger = LoggerFactory.getLogger(ProducerDemo.class);

    //create properties
    Properties properties = new Properties();
    String bootstrapServers = "127.0.0.1:9092";
    properties.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    //create kafka producer
    KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
    for (int i = 0; i < 12; i++) {
      //create producer record

      ProducerRecord<String, String> message = new ProducerRecord<>("hello_world", "Hi, I am Java Producer");

      //send message
      kafkaProducer.send(message, (metadata, exception) -> {
        if (exception == null) {
          logger.info("Successfully produced message on --->" + metadata.toString());
        } else {
          logger.error("Error received", exception);
        }
      });
    }

    //flush message
    kafkaProducer.flush();
    //close producer
    kafkaProducer.close();
  }
}
