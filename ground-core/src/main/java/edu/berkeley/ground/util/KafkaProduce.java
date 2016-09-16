package edu.berkeley.ground.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.berkeley.ground.GroundServer;
import edu.berkeley.ground.GroundServerConfiguration;
import edu.berkeley.ground.api.models.github.GithubWebhook;
import edu.berkeley.ground.resources.GithubWebhookResource;
import io.dropwizard.Application;
import io.dropwizard.cli.ConfiguredCommand;
import io.dropwizard.setup.Bootstrap;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class KafkaProduce {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProduce.class);

    public static String push(String kafkaHost, String kafkaPort, String topic, String key, String value){

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
        return "pushed";

    }



}
