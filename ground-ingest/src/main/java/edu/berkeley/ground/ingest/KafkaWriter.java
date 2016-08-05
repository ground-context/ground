package edu.berkeley.ground.ingest;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

import javax.annotation.Nullable;

import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;

import gobblin.configuration.State;
import gobblin.metrics.kafka.ProducerCloseable;
import gobblin.writer.DataWriter;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

class Kafka_Pusher implements Closeable {

  private final String topic;
  private final ProducerCloseable<String, byte[]> producer;
  private final Closer closer;

  public Kafka_Pusher(String brokers, String topic) {
    this.closer = Closer.create();

    this.topic = topic;

    Properties props = new Properties();
    props.put("metadata.broker.list", brokers);
    props.put("serializer.class", "kafka.serializer.DefaultEncoder");
    props.put("request.required.acks", "1");

    ProducerConfig config = new ProducerConfig(props);
    this.producer = createProducer(config);
  }

  /**
   * Push all mbyte array messages to the Kafka topic.
   * @param messages List of byte array messages to push to Kakfa.
   */
  public void pushMessages(List<byte[]> messages) {
    List<KeyedMessage<String, byte[]>> keyedMessages =
        Lists.transform(messages, new Function<byte[], KeyedMessage<String, byte[]>>() {
          @Nullable
          @Override
          public KeyedMessage<String, byte[]> apply(byte[] bytes) {
            return new KeyedMessage<String, byte[]>(topic, bytes);
          }
        });
    this.producer.send(keyedMessages);
  }

  @Override
  public void close() throws IOException {
    this.closer.close();
  }

  /**
   * Actually creates the Kafka producer.
   */
  protected ProducerCloseable<String, byte[]> createProducer(ProducerConfig config) {
    return this.closer.register(new ProducerCloseable<String, byte[]>(config));
  }
}


public class KafkaWriter implements DataWriter<GenericRecord> {
  
  private final Kafka_Pusher pusher;

  public KafkaWriter(State s) throws IOException {
   
    String topic = s.getProp("writer.kafka.topic");
    String brokers = s.getProp("writer.kafka.brokers");
    this.pusher = new Kafka_Pusher(brokers, topic);

  }

  @Override
  public void write(GenericRecord record) throws IOException {
    // TODO Auto-generated method stub

    byte[] array = serialize(record);
    pusher.pushMessages(Lists.newArrayList(array));
    

  }

  public static byte[] serialize(GenericRecord record) {
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
  
    GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(record.getSchema());
    BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);
    try {
      writer.write(record, encoder);
      encoder.flush();
    } catch (IOException e) {
      throw new RuntimeException("Could not serialize in Avro", e);
    
    }
    return out.toByteArray();
  }

  @Override
  public void close() throws IOException {
   pusher.close();

  }

  @Override
  public void commit() throws IOException {
    // always auto-commit
  }

  @Override
  public void cleanup() throws IOException {
  }

  @Override
  public long recordsWritten() {
    return 0;
  }

  @Override
  public long bytesWritten() {
    return 0;
  }


}
