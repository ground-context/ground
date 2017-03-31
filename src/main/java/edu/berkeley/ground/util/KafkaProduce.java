/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.berkeley.ground.util;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaProduce {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProduce.class);

  /**
   * Push a new message to Kafka.
   *
   * @param kafkaHost the kafka host
   * @param kafkaPort the kafka port
   * @param topic the kafka topic
   * @param key the message key
   * @param value the message value
   */
  public static void push(String kafkaHost,
                          String kafkaPort,
                          String topic,
                          String key,
                          String value) {

    LOGGER.info("Sending kafka message...");
    LOGGER.info("Key: " + key);
    LOGGER.info("Topic: " + topic);

    Properties props = new Properties();
    props.put("bootstrap.servers", kafkaHost + ":" + kafkaPort);
    props.put("acks", "all");
    props.put("retries", 0);
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

    byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);

    Producer<String, byte[]> producer = new KafkaProducer<>(props);
    producer.send(new ProducerRecord<>(topic, key, valueBytes));

    producer.close();
  }


}
