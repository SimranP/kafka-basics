package com.github.simranp.twitter;

import com.google.gson.JsonParser;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Properties;

import static com.github.simranp.twitter.ElasticSearchClient.createClient;
import static java.util.Set.of;

public class TwitterKafkaConsumer {
  public static final String TOPIC_NAME = "twitter_tweets";
  Logger logger = LoggerFactory.getLogger(TwitterKafkaConsumer.class);

  private static KafkaConsumer<String, String> createConsumer() {
    String bootstrapServers = "127.0.0.1:9092";
    String groupId = "tweets-filter-app";

    // create consumer configs
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    // create consumer
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
    consumer.subscribe(of(TOPIC_NAME));
    return consumer;
  }

  private void run() throws IOException {
    RestHighLevelClient client = createClient();
    KafkaConsumer<String, String> consumer = createConsumer();
    IndexRequest indexRequest = new IndexRequest("twitter");

    while (true) {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
      for (ConsumerRecord<String, String> record : records) {
        indexRequest
          .source(record.value(), XContentType.JSON)
          .id(extractIdFromTweet(record.value()));
        IndexResponse index = client.index(indexRequest, RequestOptions.DEFAULT);
        String id = index.getId();
        logger.info(id);
      }
    }

//    client.close();
  }

  private String extractIdFromTweet(String record) {
    JsonParser jsonParser = new JsonParser();
    return jsonParser.parse(record)
      .getAsJsonObject()
      .get("id_str")
      .getAsString();
  }

  public static void main(String[] args) throws IOException {
    new TwitterKafkaConsumer().run();
  }
}
