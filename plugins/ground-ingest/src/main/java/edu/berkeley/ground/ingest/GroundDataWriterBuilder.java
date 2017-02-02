package edu.berkeley.ground.ingest;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import com.typesafe.config.ConfigFactory;

import java.util.Properties;

import gobblin.configuration.State;
import gobblin.writer.DataWriter;
import gobblin.writer.DataWriterBuilder;

import com.typesafe.config.Config;


public class GroundDataWriterBuilder extends DataWriterBuilder<Schema, GenericRecord> {

  @Override
  public DataWriter<GenericRecord> build() throws IOException {

    State state = this.destination.getProperties();
    Properties taskProps = state.getProperties();
    Config writerConfig = ConfigFactory.parseProperties(taskProps);
    return new GroundWriter<>(writerConfig);
  }


}
