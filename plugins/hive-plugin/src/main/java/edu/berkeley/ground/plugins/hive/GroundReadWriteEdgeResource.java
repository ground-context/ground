package edu.berkeley.ground.plugins.hive;

import java.io.IOException;
import java.io.StringReader;
import java.net.URLEncoder;
import java.util.Map;

import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.commons.httpclient.util.HttpURLConnection;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.google.gson.stream.JsonReader;

import edu.berkeley.ground.api.models.Edge;
import edu.berkeley.ground.api.models.EdgeVersion;
import edu.berkeley.ground.api.models.Node;
import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.plugins.hive.util.PluginUtil;

public class GroundReadWriteEdgeResource {

    public Edge getEdge(String edgeName) throws GroundException {
        GetMethod get = new GetMethod(GroundReadWrite.groundServerAddress + "edges/" + edgeName);
        return getEdge(get);
    }

    public EdgeVersion getEdgeVersion(long edgeId) throws GroundException {
        GetMethod get = new GetMethod(GroundReadWrite.groundServerAddress + "edges/versions" + edgeId);
        try {
            return getEdgeVersion(get);
        } catch (IOException e) {
            throw new GroundException(e);
        }
    }

    // create edge for input Tag
    public Edge createEdge(String name, Map<String, Tag> tagMap) throws GroundException {
        try {
            String encodedUri = GroundReadWrite.groundServerAddress + "edges/" + URLEncoder.encode(name, "UTF-8");
            PostMethod post = new PostMethod(encodedUri);
            ObjectMapper objectMapper = new ObjectMapper();
            String jsonString = objectMapper.writeValueAsString(tagMap);
            post.setRequestEntity(GroundReadWrite.createRequestEntity(jsonString));
            String response = GroundReadWrite.execute(post);
            return this.constructEdge(response);
        } catch (IOException e) {
            throw new GroundException(e);
        }
    }

    // method to create the edgeVersion given the nodeId and the tags
    public EdgeVersion createEdgeVersion(long id, Map<String, Tag> tags, long structureVersionId, String reference,
            Map<String, String> referenceParameters, long edgeId, long fromId, long toId) throws GroundException {
        try {
            EdgeVersion edgeVersion = new EdgeVersion(id, tags, structureVersionId, reference, referenceParameters,
                    edgeId, fromId, toId);
            ObjectMapper mapper = new ObjectMapper();
            String jsonRecord = mapper.writeValueAsString(edgeVersion);
            String uri = GroundReadWrite.groundServerAddress + "edges/versions";
            PostMethod post = new PostMethod(uri);
            StringRequestEntity requestEntity = GroundReadWrite.createRequestEntity(jsonRecord);
            post.setRequestEntity(requestEntity);
            return getEdgeVersion(post);
        } catch (IOException e) {
            throw new GroundException(e);
        }
    }

    private Edge getEdge(HttpMethod method) throws GroundException {
        try {
            if (GroundReadWrite.client.executeMethod(method) == HttpURLConnection.HTTP_OK) {
                // getting the nodeId of the node created
                String response = method.getResponseBodyAsString();
                return constructEdge(response);
            }
        } catch (IOException e) {
            throw new GroundException(e);
        }
        return null;
    }

    private EdgeVersion getEdgeVersion(HttpMethod method)
            throws JsonParseException, JsonMappingException, HttpException, IOException {
        if (GroundReadWrite.client.executeMethod(method) == HttpURLConnection.HTTP_OK) {
            // getting the nodeId of the node created
            String text = method.getResponseBodyAsString();
            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.readValue(text, EdgeVersion.class);
        }
        return null;
    }

    private Edge constructEdge(String response) throws GroundException {
        JsonReader reader = new JsonReader(new StringReader(response));
        return PluginUtil.fromJson(reader, Edge.class);
    }

}
