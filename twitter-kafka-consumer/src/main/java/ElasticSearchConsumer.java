import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ElasticSearchConsumer {

  Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class);

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

  public static void main(String[] args) throws IOException {
    new ElasticSearchConsumer().run();
  }

  private void run() throws IOException {
    RestHighLevelClient client = createClient();
    IndexRequest indexRequest = new IndexRequest("twitter");
    String json = "{\n" +
      "  \"foo\": \"bar\"\n" +
      "}";
    indexRequest.source(json, XContentType.JSON);
    IndexResponse index = client.index(indexRequest, RequestOptions.DEFAULT);
    String id = index.getId();
    logger.info(id);

    client.close();
  }
}