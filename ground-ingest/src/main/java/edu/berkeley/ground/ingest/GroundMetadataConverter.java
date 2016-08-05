package edu.berkeley.ground.ingest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import gobblin.configuration.WorkUnitState;
import gobblin.converter.DataConversionException;
import gobblin.converter.SchemaConversionException;
import gobblin.converter.ToAvroConverterBase;

public class GroundMetadataConverter extends ToAvroConverterBase<Schema, GenericRecord>{
  
  private final String schemaString = "{\"namespace\": \"example.avro\", "
      + "\"type\": \"record\","
      + "\"name\": \"User\", "
     + "\"fields\": [ "
          + "{\"name\": \"name\", \"type\": [\"string\", \"null\"]}, "
          + "{\"name\": \"tags\",  \"type\": { \"type\": \"map\", \"values\": \"string\"}}, "
          + "{\"name\": \"StructureVerionId\", \"type\": [\"string\", \"null\"]}, "
          + "{\"name\": \"reference\", \"type\": [\"string\", \"null\"]}, "
          + "{\"name\": \"parameters\",  \"type\": { \"type\": \"map\", \"values\": \"string\"}} "
     + "]"
     + "}";
  private final Schema outputSchema = new Schema.Parser().parse(schemaString);
   
  public GenericRecord recordConverter(GenericRecord record) {
    
    
    GenericRecord outputRecord = new GenericData.Record(outputSchema);
 
    outputRecord.put("name", record.get("name"));
    
    return outputRecord;
  }



  @Override
  public Iterable<GenericRecord> convertRecord(Schema outputSchema, GenericRecord inputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    GenericRecord outputRecord = recordConverter(inputRecord);
    ArrayList<GenericRecord> list = new ArrayList<GenericRecord>(1);
    list.add(outputRecord);
    return list;
  }


  @Override
  public Schema convertSchema(Schema schema, WorkUnitState workUnit) throws SchemaConversionException {
   
    return outputSchema;
  }

}