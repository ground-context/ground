package edu.berkeley.ground.ingest;

import java.lang.reflect.Type;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.reflect.TypeToken;

import gobblin.configuration.WorkUnitState;
import gobblin.converter.SingleRecordIterable;
import gobblin.converter.ToAvroConverterBase;


public class WikiConverter extends ToAvroConverterBase<String, JsonElement>{
  
  private static final String JSON_CONTENT_MEMBER = "content";

  private static final Gson GSON = new Gson();
  // Expect the input JSON string to be key-value pairs
  private static final Type FIELD_ENTRY_TYPE = new TypeToken<Map<String, Object>>() {}.getType();

  @Override
  public Schema convertSchema(String schema, WorkUnitState workUnit) {
    return new Schema.Parser().parse(schema);
  }

  @Override
  public Iterable<GenericRecord> convertRecord(Schema outputSchema, JsonElement inputRecord, WorkUnitState workUnit) {
    JsonElement element = GSON.fromJson(inputRecord, JsonElement.class);
    Map<String, Object> fields = GSON.fromJson(element, FIELD_ENTRY_TYPE);
    GenericRecord record = new GenericData.Record(outputSchema);
    
    
    for (Map.Entry<String, Object> entry : fields.entrySet()) {
      if (entry.getKey().equals("*")) {
        //switch '*' to 'content' since '*' is not a valid avro schema field name
        record.put(JSON_CONTENT_MEMBER, entry.getValue());
        } 
     else {
       record.put(entry.getKey(), entry.getValue());
      }
    }

    return new SingleRecordIterable<>(record);
  }

}
