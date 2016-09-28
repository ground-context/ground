package edu.berkeley.ground.ingest;


import java.io.IOException;
import java.util.HashMap;
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
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;


import edu.berkeley.ground.api.models.Node;
import edu.berkeley.ground.api.models.NodeVersion;
import edu.berkeley.ground.api.models.Tag;
import gobblin.writer.DataWriter;

import java.net.URLEncoder;




public class GroundWriter<D> implements DataWriter<GenericRecord>  {
  
  
  public void write(GenericRecord record) throws IOException {
    
    Map<String, Tag> tags =  new HashMap<String, Tag>();
    
    Tag timeCreated = new Tag(null, record.get("timeCreated").toString(), null, null);
    Tag length = new Tag(null, record.get("length").toString(), null, null);
    Tag modificationTime = new Tag(null, record.get("modificationTime").toString(), null, null);
    Tag owner = new Tag(null, record.get("owner").toString(), null, null);
    tags.put("timeCreated", timeCreated);
    tags.put("length", length);
    tags.put("modificationTime", modificationTime);
    tags.put("owner", owner);
    
    String name = record.get("name").toString();
    
    node(name, tags);
   
  } 

 
  
  
    
   public void node(String name, Map<String, Tag> tags) throws JsonGenerationException, JsonMappingException, IOException {
     
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
     String text = post.getResponseBodyAsString();
     ObjectMapper objectMapper = new ObjectMapper();
     JsonNode node = objectMapper.readValue(text, JsonNode.class);
     JsonNode nodeId = node.get("id");
     String id = nodeId.asText(); 
     
     NodeVersion nodeVersion = new NodeVersion(null, tags, null, null, null, id);
  }
   
   public void nodeVersion(String id, Map<String, Tag> tags, String structureVersionId, String reference, Map<String, String> referenceParameters) throws JsonGenerationException, JsonMappingException, IOException {
     
     NodeVersion nodeVersion = new NodeVersion("id", tags, structureVersionId, reference, referenceParameters, id);
     ObjectMapper mapper = new ObjectMapper();
     String jsonString = mapper.writeValueAsString(nodeVersion);
     HttpClient client = new HttpClient();
     String uri = "http://localhost:9000/nodes/versions";
     PostMethod post = new PostMethod(uri);
     post.setRequestHeader("Content-type", "application/json");
     post.setRequestBody(jsonString);
     int statusCode = client.executeMethod(post);

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
 
}
