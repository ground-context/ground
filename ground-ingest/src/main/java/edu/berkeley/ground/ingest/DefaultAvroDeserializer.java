package edu.berkeley.ground.ingest;

import java.io.IOException;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.common.serialization.Deserializer;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;


/**
 * Created by sdas on 8/3/16.
 */
@Slf4j
public class DefaultAvroDeserializer implements Deserializer<GenericRecord> {
  private final Schema _schema;
  private final GenericDatumReader<GenericRecord> _recordReader;

  public DefaultAvroDeserializer(Config config) {
    Preconditions.checkArgument(config.hasPath("kafka.serde.schema"), "Could not find a schema to use for deserialization");
    String schemaString = config.getString("kafka.serde.schema");
    _schema = new Schema.Parser().parse(schemaString);
    _recordReader = new GenericDatumReader<>(_schema);
    log.info("DefaultAvroDeserializer initialized successfully");
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    log.warn("Not expecting to be configured!");
  }

  @Override
  public GenericRecord deserialize(String topic, byte[] data) {
    try {
      return _recordReader.read(null, DecoderFactory.get().binaryDecoder(data, null));
    } catch (IOException e) {
      throw new RuntimeException("Could not deserialize in Avro", e);
    }
  }

  @Override
  public void close() {

  }
}

