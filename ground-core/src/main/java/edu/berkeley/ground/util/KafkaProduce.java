package edu.berkeley.ground.util;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class KafkaProduce {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProduce.class);

    public static void push(String kafkaHost, String kafkaPort, String topic, String key, String value){
        LOGGER.info("Sending kafka message...");
        byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);

        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaHost+":"+kafkaPort);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //TODO serialize as json not as byte[]
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        Producer<String, byte[]> producer = new KafkaProducer<>(props);
        producer.send(new ProducerRecord<>(topic, key, valueBytes));

        producer.close();
    }



}
