package edu.berkeley.ground.ingest;


import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.PostMethod;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;


import edu.berkeley.ground.api.models.Node;
import edu.berkeley.ground.api.models.NodeVersion;
import edu.berkeley.ground.api.models.Tag;
import gobblin.writer.DataWriter;

import java.net.URLEncoder;




public class GroundWriter<D> implements DataWriter<byte[]>  {
  
  
  public void write(byte[] array) throws IOException {
    
    
    GenericRecord record = deserialize(array);
    String info = record.get("name").toString();
    
    node(info);
   
  } 

 
   public GenericRecord deserialize(byte[] byteArray) {
     
     String schemaString = "{\"namespace\": \"example.avro\", "
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
     
     
     Schema outputSchema = new Schema.Parser().parse(schemaString);
     
     GenericDatumReader<GenericRecord> serveReader = new GenericDatumReader<>(outputSchema);
     try {
         return serveReader.read(null, DecoderFactory.get().binaryDecoder(byteArray, null));
     } catch (IOException e) {
       throw new RuntimeException("Could not deserialize in Avro", e);
      
     }
       
   }
  
    
   public void node(String name) throws JsonGenerationException, JsonMappingException, IOException {
     
     Node testNode = new Node("id", name);
     ObjectMapper mapper = new ObjectMapper();
     String jsonString = mapper.writeValueAsString(testNode);
     HttpClient client = new HttpClient();
     String uri = "http://localhost:9090/nodes/"+ name;
     String encodedUri = "http://localhost:9090/nodes/" + URLEncoder.encode(name, "UTF-8");
     //String encodedUri = URLEncoder.encode(uri, "UTF-8");
     PostMethod post = new PostMethod(encodedUri);
     post.setRequestHeader("Content-type", "application/json");
     post.setRequestBody(jsonString);
     int statusCode = client.executeMethod(post);
  }
   
   public void nodeVersion(String id, Optional<Map<String, Tag>> tags, Optional<String> structureVersionId, Optional<String> reference, Optional<Map<String, String>> parameters, WebTarget targetnodeversion) {
     
     NodeVersion nodeVersion = new NodeVersion("id", tags, structureVersionId, reference, parameters, id);
     Response response = targetnodeversion.request(MediaType.APPLICATION_JSON_TYPE).post(Entity.json(nodeVersion));
     System.out.println(response.toString());
   }

  @Override
  public void close() throws IOException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public long bytesWritten() throws IOException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void cleanup() throws IOException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void commit() throws IOException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public long recordsWritten() {
    // TODO Auto-generated method stub
    return 0;
  }

 public static void main(String args[]) throws IOException{
   /**
   Client client = new JerseyClientBuilder().build();
   WebTarget targetnode = client.target("http://localhost:9090/nodes/valley");
   Node nodeTest = new Node("id", "valley");
   //Builder builder = targetnode.request(MediaType.APPLICATION_JSON);
   Response response = targetnode.request(MediaType.APPLICATION_JSON).post(Entity.json(nodeTest));
   System.out.println(response.toString()); */
   
   
   Node testNode = new Node("id", "brand newsiliconvalley");
   ObjectMapper mapper = new ObjectMapper();
   String jsonString = mapper.writeValueAsString(testNode);
   HttpClient client = new HttpClient();
   String encodedUri = "http://localhost:9090/nodes/" + URLEncoder.encode("brand newsiliconvalley", "UTF-8");
   System.out.println(encodedUri);
   PostMethod post = new PostMethod(encodedUri);
   post.setRequestHeader("Content-type", "application/json");
   post.setRequestBody(jsonString);
   int statusCode = client.executeMethod(post);
   
 }

 
  
 
}
