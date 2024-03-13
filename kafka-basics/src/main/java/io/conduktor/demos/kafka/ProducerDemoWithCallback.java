package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    private static final Logger log = LoggerFactory.getLogger((ProducerDemoWithCallback.class.getSimpleName()));

    public static void main(String[] args) {
        log.info("Kafka Producer Log");

        //Local Kafka Server
        Properties props = new Properties();
        //props.setProperty("bootstrap.servers","127.0.0.1:9092");

        //Remote Kafka Server
        props.put("bootstrap.servers", "https://distinct-thrush-9436-us1-kafka.upstash.io:9092");
        props.put("sasl.mechanism", "SCRAM-SHA-256");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"ZGlzdGluY3QtdGhydXNoLTk0MzYkC4zJMwqs1oQvFmZhohOB4rY9nvvRXsN-Xak\" password=\"ZTY2M2I2MzItNWI1Zi00Yzc3LTliMDYtZmNjODlkYzlhNmYx\";");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        props.setProperty("batch.size","400");

        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(props);

        for (int j = 0; j<10;j++) {
            for(int i=0;i<30;i++) {
                ProducerRecord<String,String> producerRecord =
                        new ProducerRecord<>("demo_java","hello world "+i);

                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        if(e == null) {
                            log.info("Received new metadata \n" +
                                    "Topic: " + metadata.topic()+"\n"+
                                    "Partition: " + metadata.partition()+"\n"+
                                    "Offset: " + metadata.offset()+"\n"+
                                    "Timestamp: " + metadata.timestamp());
                        } else {
                            log.error("error while producing " + e.getMessage());
                        }
                    }
                });
            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        producer.close();
    }
}
