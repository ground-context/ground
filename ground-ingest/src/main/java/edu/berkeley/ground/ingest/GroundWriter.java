package edu.berkeley.ground.ingest;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.PostMethod;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import edu.berkeley.ground.api.models.Node;
import edu.berkeley.ground.api.models.NodeVersion;
import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.api.versions.GroundType;
import gobblin.writer.DataWriter;
import java.net.URLEncoder;


public class GroundWriter<D> implements DataWriter<GenericRecord>  {
  
    public void write(GenericRecord record) throws IOException {
    
      //extracting the metadata from the GenericRecord and storing it in the field tags      
      Map<String, Tag> tags =  new HashMap<String, Tag>();
      GroundType typeString = GroundType.STRING;
      GroundType typeInt = GroundType.INTEGER;
      Tag timeCreated = new Tag("id5", "timeCreated", record.get("timeCreated"), typeInt);
      Tag length = new Tag("id6", "fileLength", record.get("length"), typeInt);
      Tag modificationTime = new Tag("id7", "modificationTime", record.get("modificationTime"), typeInt);
      Tag owner = new Tag("id8", "owner", record.get("owner"), typeString);
      tags.put("timeCreated", timeCreated);
      tags.put("length", length);
      tags.put("modificationTime", modificationTime);
      tags.put("owner", owner);
        
      //extracting the name of the file to create the Node
      String name = record.get("name").toString();
      node(name, tags);
   
  } 
    
    //method to create a Node given the name
    public void node(String name, Map<String, Tag> tags) throws JsonGenerationException, JsonMappingException, IOException {
     
      Node node = new Node("id", name);
      ObjectMapper mapper = new ObjectMapper();
      String jsonString = mapper.writeValueAsString(node);
      HttpClient client = new HttpClient();
      String uri = "http://localhost:9090/nodes/"+ name;
      String encodedUri = "http://localhost:9090/nodes/" + URLEncoder.encode(name, "UTF-8");
      PostMethod post = new PostMethod(encodedUri);
      post.setRequestHeader("Content-type", "application/json");
      post.setRequestBody(jsonString);
      int statusCode = client.executeMethod(post);
      
      //getting the nodeId of the node created
      String text = post.getResponseBodyAsString();
      ObjectMapper objectMapper = new ObjectMapper();
      JsonNode jsonNode = objectMapper.readValue(text, JsonNode.class);
      JsonNode nodeId = jsonNode.get("id");
      String id = nodeId.asText(); 
        
      //creating a NodeVersion using the nodeId and the metadata stored in tags
      Map<String,String> refParameters = new HashMap<String,String>();
      nodeVersion("id",tags,null,null,refParameters,id);
    
  }
   
    //method to create the NodeVersion given the nodeId and the tags
    public void nodeVersion(String id, Map<String, Tag> tags, String structureVersionId, String reference, Map<String, String> referenceParameters, String nodeId) throws JsonGenerationException, JsonMappingException, IOException {
     
      NodeVersion nodeVersion = new NodeVersion(id, tags, structureVersionId, reference, referenceParameters, nodeId);
      ObjectMapper mapper = new ObjectMapper();
      String jsonString = mapper.writeValueAsString(nodeVersion);
      HttpClient client = new HttpClient();
      String uri = "http://localhost:9090/nodes/versions";
      PostMethod post = new PostMethod(uri);
      post.setRequestHeader("Content-type", "application/json");
      post.setRequestBody(jsonString);
      int statusCode = client.executeMethod(post);

   }

    @Override
    public void close() throws IOException {}

    @Override
    public long bytesWritten() throws IOException {
        
      return 0;
      
  }

    @Override
     public void cleanup() throws IOException {}

    @Override
    public void commit() throws IOException {}

    @Override
    public long recordsWritten() {
       
      return 0;
      
  }
 
}
