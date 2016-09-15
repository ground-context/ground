package edu.berkeley.ground.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.berkeley.ground.api.models.github.GithubWebhook;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class KafkaProduce {
    public static String push(String topic, String key, GithubWebhook ghwh){

        ObjectMapper mapper = new ObjectMapper();
        String json = "";
        try {
            json = mapper.writeValueAsString(ghwh);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }


        byte[] value = json.getBytes(StandardCharsets.UTF_8);

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
        return "pushed";

    }
}
