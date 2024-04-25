package org.example;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.lucene.index.IndexReader;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.mapper.DynamicTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import org.opensearch.common.xcontent.*;

public class OpenSearchConsumer {

    private static final Logger log = LoggerFactory.getLogger(((OpenSearchConsumer.class.getSimpleName())));

    public static RestHighLevelClient createOpenSearchClient() {
        String connString = "http://localhost:9200";
        // String connString = "https://c9p5mwld41:45zeygn9hy@kafka-course-2322630105.eu-west-1.bonsaisearch.net:443";

        // we build a URI from the connection string
        RestHighLevelClient restHighLevelClient;
        URI connUri = URI.create(connString);
        // extract login information if it exists
        String userInfo = connUri.getUserInfo();

        if (userInfo == null) {
            // REST client without security
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));

        } else {
            // REST client with security
            String[] auth = userInfo.split(":");

            CredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));

        }
        return restHighLevelClient;
    }

    private static KafkaConsumer createKafkaConsumer() {
        log.info("Kafka Consumer Log");

        String groupId = "consumer-opensearch-demo";

        //Local Kafka Server
        Properties props = new Properties();
        //props.setProperty("bootstrap.servers","127.0.0.1:9092");

        //Remote Kafka Server
        props.put("bootstrap.servers", "https://distinct-thrush-9436-us1-kafka.upstash.io:9092");
        props.put("sasl.mechanism", "SCRAM-SHA-256");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"ZGlzdGluY3QtdGhydXNoLTk0MzYkC4zJMwqs1oQvFmZhohOB4rY9nvvRXsN-Xak\" password=\"ZTY2M2I2MzItNWI1Zi00Yzc3LTliMDYtZmNjODlkYzlhNmYx\";");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("group.id", groupId);
        props.put("auto.offset.reset","latest");

        return new KafkaConsumer<String, String>(props);
    }


    public static void main(String[] args) throws IOException {
        System.out.println("hello world");

        RestHighLevelClient openSearchClient = createOpenSearchClient();

        KafkaConsumer<String,String> consumer = createKafkaConsumer();

        try(openSearchClient; consumer) {

            boolean indexExists = openSearchClient.indices().exists(new GetIndexRequest("wikimedia"),
                    RequestOptions.DEFAULT);
            if(!indexExists) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                log.info("The wikimedia index has been created.");
            }
            else {
                log.info("The wikimedia index already exists");
            }
            //suscribe from the consumer
            consumer.subscribe(Collections.singleton("wikimedia.recentchange"));

            while(true) {
                ConsumerRecords<String, String> records = consumer.poll((Duration.ofMillis((3000))));
                int recordCount = records.count();
                log.info("Received: " + recordCount + " records.");

                for (ConsumerRecord<String,String> record : records) {

                    XContentBuilder builder = XContentFactory.jsonBuilder();
                    builder.startObject();
                    builder.field("field_name", record.value()); // Example: Convert value to string
                    builder.endObject();
                    try {
                        IndexRequest indexRequest = new IndexRequest("wikimedia").source(builder);
                        IndexResponse response = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);
                        log.info(response.getId());
                    } catch(Exception e) {

                    }


                }
            }


        }





    }



}