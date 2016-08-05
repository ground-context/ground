package edu.berkeley.ground.ingest;

import gobblin.configuration.State;
import gobblin.writer.partitioner.WriterPartitioner;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

public class WikiPartitioner implements WriterPartitioner<GenericRecord>{
  
  private static final String TITLE = "title";

  private static final Schema SCHEMA = SchemaBuilder.record("ArticleTitle").namespace("gobblin.example.wikipedia")
      .fields().name(TITLE).type(Schema.create(Schema.Type.STRING)).noDefault().endRecord();

  public WikiPartitioner(State state, int numBranches, int branchId) {}

  @Override
  public Schema partitionSchema() {
    return SCHEMA;
  }

  @Override
  public GenericRecord partitionForRecord(GenericRecord record) {
    GenericRecord partition = new GenericData.Record(SCHEMA);
    partition.put(TITLE, record.get("title"));
    return partition;
  }

}
