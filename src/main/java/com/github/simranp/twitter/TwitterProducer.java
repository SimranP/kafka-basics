package com.github.simranp.twitter;

import com.twitter.hbc.httpclient.BasicClient;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

public class TwitterProducer {
  Logger logger = LoggerFactory.getLogger(TwitterProducer.class);

  public static void main(String[] args) {
    new TwitterProducer().run();
  }

  public void run() {
    BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(100);
    BasicClient twitterClient = TwitterClient.create(msgQueue);
    twitterClient.connect();

    KafkaProducer<String, String> kafkaProducer = createKafkaProducer();

    while (!twitterClient.isDone()) {
      String msg = null;
      try {
        msg = msgQueue.poll(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        e.printStackTrace();
        twitterClient.stop();
      }
      if (msg != null) {
        logger.info(msg);
        kafkaProducer.send(new ProducerRecord<>("twitter_tweets", null, msg), (metadata, exception) -> {
          if (exception != null) {
            logger.error("Something bad happened");
          }
        });
      }

    }

    // add a shutdown hook
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      logger.info("stopping application...");
      logger.info("shutting down client from twitter...");
      twitterClient.stop();
      logger.info("closing producer...");
      kafkaProducer.close();
      logger.info("done!");
    }));

    logger.info("End of application");
  }

  private KafkaProducer<String, String> createKafkaProducer() {
    Properties properties = new Properties();
    String bootstrapServers = "127.0.0.1:9092";
    properties.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    //create kafka producer
    return new KafkaProducer<>(properties);
  }
}