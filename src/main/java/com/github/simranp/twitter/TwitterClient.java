package com.github.simranp.twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.event.Event;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.OAuth1;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterClient {
  public static BasicClient create(BlockingQueue<String> msgQueue) {
    // Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream
    BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>(1000);

    // Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth)
    Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
    StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
    // Optional: set up some followings and track terms
    List<String> terms = Lists.newArrayList("kafka");
    hosebirdEndpoint.trackTerms(terms);
    String apiKey = System.getenv("TWITTER_API_KEY");
    String apiSecret = System.getenv("TWITTER_API_SECRET_KEY");
    String token = System.getenv("TWITTER_ACCESS_TOKEN");
    String tokenSecret = System.getenv("TWITTER_ACCESS_TOKEN_SECRET");

    System.out.printf("%s %s %s %s %n", apiKey, apiSecret, token, tokenSecret);

    OAuth1 oAuth1 = new OAuth1(apiKey, apiSecret, token, tokenSecret);
    ClientBuilder builder = new ClientBuilder()
      .name("Hosebird-Client-01")                              // optional: mainly for the logs
      .hosts(hosebirdHosts)
      .authentication(oAuth1)
      .endpoint(hosebirdEndpoint)
      .processor(new StringDelimitedProcessor(msgQueue))
      .eventMessageQueue(eventQueue);                          // optional: use this if you want to process client events

    return builder.build();
  }
}
