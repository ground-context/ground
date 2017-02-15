package edu.berkeley.ground.ingest;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.PostMethod;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import edu.berkeley.ground.api.models.Node;
import edu.berkeley.ground.api.models.NodeVersion;
import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.api.versions.GroundType;
import gobblin.util.ConfigUtils;
import gobblin.writer.DataWriter;

import java.net.URLEncoder;

import com.typesafe.config.Config;


public class GroundWriter<D> implements DataWriter<GenericRecord> {

  HttpClient client = new HttpClient();
  private final String groundServerAddress;

  public GroundWriter(Config config) {

    this.groundServerAddress = ConfigUtils.getString(config, GroundWriterConfigurationKeys.GROUND_SERVER_ADDRESS, GroundWriterConfigurationKeys.DEFAULT_GROUND_SERVER_ADDRESS);

  }

  public void write(GenericRecord record) throws IOException {

    //extracting the metadata from the GenericRecord and storing it in the field tags
    Map<String, Tag> tags = new HashMap<String, Tag>();
    GroundType typeString = GroundType.STRING;
    GroundType typeInt = GroundType.INTEGER;
    Tag timeCreated = new Tag(-1, "timeCreated", record.get("timeCreated"), typeInt);
    Tag length = new Tag(-1, "fileLength", record.get("length"), typeInt);
    Tag modificationTime = new Tag(-1, "modificationTime", record.get("modificationTime"), typeInt);
    Tag owner = new Tag(-1, "owner", record.get("owner"), typeString);
    tags.put("timeCreated", timeCreated);
    tags.put("length", length);
    tags.put("modificationTime", modificationTime);
    tags.put("owner", owner);

    //extracting the name of the file to create the Node
    String name = record.get("name").toString();
    node(name, tags);

  }

  //method to create a Node given the name
  public void node(String name, Map<String, Tag> tags) throws IOException {

    Node node = new Node(-1, name);
    ObjectMapper mapper = new ObjectMapper();
    String jsonString = mapper.writeValueAsString(node);
    String uri = groundServerAddress + name;
    String encodedUri = groundServerAddress + "nodes/" + URLEncoder.encode(name, "UTF-8");
    PostMethod post = new PostMethod(encodedUri);
    post.setRequestHeader("Content-type", "application/json");
    post.setRequestBody(jsonString);
    int statusCode = client.executeMethod(post);

    //getting the nodeId of the node created
    String text = post.getResponseBodyAsString();
    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode jsonNode = objectMapper.readValue(text, JsonNode.class);
    JsonNode nodeId = jsonNode.get("id");
    long id = nodeId.asLong();

    //creating a NodeVersion using the nodeId and the metadata stored in tags
    Map<String, String> refParameters = new HashMap<String, String>();
    nodeVersion(-1, tags, -1, null, refParameters, id);

  }

  //method to create the NodeVersion given the nodeId and the tags
  public void nodeVersion(long id, Map<String, Tag> tags, long structureVersionId, String reference, Map<String, String> referenceParameters, long nodeId) throws IOException {

    NodeVersion nodeVersion = new NodeVersion(id, tags, structureVersionId, reference, referenceParameters, nodeId);
    ObjectMapper mapper = new ObjectMapper();
    String jsonString = mapper.writeValueAsString(nodeVersion);
    String uri = groundServerAddress + "nodes/versions";
    PostMethod post = new PostMethod(uri);
    post.setRequestHeader("Content-type", "application/json");
    post.setRequestBody(jsonString);
    int statusCode = client.executeMethod(post);

  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public long bytesWritten() throws IOException {

    return 0;

  }

  @Override
  public void cleanup() throws IOException {
  }

  @Override
  public void commit() throws IOException {
  }

  @Override
  public long recordsWritten() {

    return 0;

  }

}
