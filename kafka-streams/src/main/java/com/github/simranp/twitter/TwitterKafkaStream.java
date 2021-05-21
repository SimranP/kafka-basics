package com.github.simranp.twitter;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class TwitterKafkaStream {
  public void run() {
    //create properties
    Properties properties = new Properties();
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-streams");
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
    //create topology
    StreamsBuilder streamsBuilder = new StreamsBuilder();
    KStream<Object, Object> twitter_tweets = streamsBuilder.stream("twitter_tweets");
    KStream<Object, Object> popular_twitter_tweets = twitter_tweets.filter((k, tweetJson) -> extractFollowersCount(tweetJson.toString()) > 10000);
    popular_twitter_tweets.to("popular_twitter_tweets");
    //build topology
    KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);
    //start stream app
    kafkaStreams.start();
  }

  private int extractFollowersCount(String tweetJson) {
    JsonParser jsonParser = new JsonParser();
    try {
      return jsonParser.parse(tweetJson)
        .getAsJsonObject().get("user")
        .getAsJsonObject().get("followers_count")
        .getAsInt();
    }catch (NullPointerException e){
      return 0;
    }
  }

  public static void main(String[] args) {
    new TwitterKafkaStream().run();
  }
}
