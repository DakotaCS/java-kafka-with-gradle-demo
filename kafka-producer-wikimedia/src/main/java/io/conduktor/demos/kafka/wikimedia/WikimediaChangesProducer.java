package io.conduktor.demos.kafka.wikimedia;

import com.launchdarkly.eventsource.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {
    public static void main(String[] args) throws InterruptedException {

        Properties props = new Properties();
        //props.setProperty("bootstrap.servers","127.0.0.1:9092");

        //Remote Kafka Server
        props.put("bootstrap.servers", "https://distinct-thrush-9436-us1-kafka.upstash.io:9092");
        props.put("sasl.mechanism", "SCRAM-SHA-256");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"ZGlzdGluY3QtdGhydXNoLTk0MzYkC4zJMwqs1oQvFmZhohOB4rY9nvvRXsN-Xak\" password=\"ZTY2M2I2MzItNWI1Zi00Yzc3LTliMDYtZmNjODlkYzlhNmYx\";");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        props.put(ProducerConfig.LINGER_MS_CONFIG,"20");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG,Integer.toString(32 * 1024));
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");

        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(props);

        String topic = "wikimedia.recentchange";

        EventHandler eventHandler = new WikimediaChangeHandler(producer,topic);

        String url = "https://stream.wikimedia.org/v2/stream/recentchange";

        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(url));

        EventSource eventSource = builder.build();

        eventSource.start();

        //produce for 10 minutes
        TimeUnit.MINUTES.sleep(5);

    }
}

