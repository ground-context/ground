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


public class GroundConverter extends ToAvroConverterBase<Schema, GenericRecord>{
  
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
    Map<String, String> parameters =  new HashMap<String, String>();
    
    outputRecord.put("name", record.get("title"));
    outputRecord.put("tags", new HashMap<String, String>());
    parameters.put("pageid", record.get("pageid").toString());
    parameters.put("user", record.get("user").toString());
    //parameters.put("anon", record.get("anon").toString());
    parameters.put("userid", record.get("userid").toString());
    parameters.put("timestamp", record.get("timestamp").toString());
    parameters.put("size", record.get("size").toString());
    parameters.put("contentformat", record.get("contentformat").toString());
    parameters.put("contentmodel", record.get("contentmodel").toString());
    parameters.put("content", record.get("content").toString());
    
    outputRecord.put("parameters", parameters); 
  
    
    
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
