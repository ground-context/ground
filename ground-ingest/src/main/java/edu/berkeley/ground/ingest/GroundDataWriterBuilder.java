package edu.berkeley.ground.ingest;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import gobblin.writer.DataWriter;
import gobblin.writer.DataWriterBuilder;


public class GroundDataWriterBuilder extends DataWriterBuilder<Schema, byte[]> {

  @Override
  public DataWriter<byte[]> build() throws IOException {
    return new GroundWriter<>();
  }
  

}
