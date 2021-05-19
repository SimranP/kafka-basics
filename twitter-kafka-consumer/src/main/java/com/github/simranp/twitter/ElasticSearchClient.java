package com.github.simranp.twitter;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;


public class ElasticSearchClient {

  public static RestHighLevelClient createClient() {
    String hostname = "kafka-basics-5435053313.us-east-1.bonsaisearch.net";
    String username = System.getenv("ES_USERNAME");
    String password = System.getenv("ES_PASSWORD");

    BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();

    credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

    RestClientBuilder builder = RestClient.builder(new HttpHost(hostname, 443, "https"))
      .setHttpClientConfigCallback(httpAsyncClientBuilder ->
        httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
      );

    return new RestHighLevelClient(builder);
  }
}