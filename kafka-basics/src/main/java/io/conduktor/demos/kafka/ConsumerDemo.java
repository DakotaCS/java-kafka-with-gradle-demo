package io.conduktor.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Array;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {

    private static final Logger log = LoggerFactory.getLogger((ConsumerDemo.class.getSimpleName()));

    public static void main(String[] args) {
        log.info("Kafka Consumer Log");

        String groupId = "my-java-application";
        String topic = "demo_java";

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
        props.put("auto.offset.reset","earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        consumer.subscribe(Arrays.asList(topic));

        while(true) {
            log.info("Polling");
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            for(ConsumerRecord<String,String> record : records) {
                log.info("Key: " + record.key() + "\n"+
                        "Value: " + record.value() + "\n"+
                        "Partition: " + record.partition() + "\n"+
                        "Offset: " + record.offset() + "\n");
            }


        }


    }
}
