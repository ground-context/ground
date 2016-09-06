package edu.berkeley.ground.util;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class KafkaProduce {
    public static void main(String[] args){
        if (args.length != 3) {
            System.out.println("Please provide command line arguments: topic, key, value");
            System.exit(-1);
        }
        String topic = args[0];
        String key = args[1];
        byte[] value = args[2].getBytes(StandardCharsets.UTF_8);

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //TODO serialize json
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        Producer<String, byte[]> producer = new KafkaProducer<>(props);
        producer.send(new ProducerRecord<>(topic, key, value));

        producer.close();

    }
}
