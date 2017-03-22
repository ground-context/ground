package edu.berkeley.ground.plugins.hive;

import java.io.IOException;
import java.io.StringReader;
import java.net.URLEncoder;
import java.util.List;
import java.util.Map;

import edu.berkeley.ground.api.models.Node;
import edu.berkeley.ground.api.models.NodeVersion;
import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.plugins.hive.util.PluginUtil;

import com.google.gson.stream.JsonReader;

import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.codehaus.jackson.map.ObjectMapper;

public class GroundReadWriteNodeResource {

    // method to create the NodeVersion given the nodeId and the tags
    public NodeVersion createNodeVersion(long id, Map<String, Tag> tags, long structureVersionId, String reference,
            Map<String, String> referenceParameters, String name) throws GroundException {
        try {
            Node node = this.createNode(name, tags);
            NodeVersion nodeVersion = new NodeVersion(id, tags, structureVersionId, reference, referenceParameters,
                    node.getId());
            ObjectMapper mapper = new ObjectMapper();
            String jsonString = mapper.writeValueAsString(nodeVersion);
            String uri = PluginUtil.groundServerAddress + "nodes/versions";
            PostMethod post = new PostMethod(uri);
            post.setRequestEntity(PluginUtil.createRequestEntity(jsonString));
            String response = PluginUtil.execute(post);
            return constructNodeVersion(response);
        } catch (IOException e) {
            throw new GroundException(e);
        }
    }

    public NodeVersion getNodeVersion(long nodeVersionId) throws GroundException {
        GetMethod get = new GetMethod(PluginUtil.groundServerAddress + "nodes/versions/" + nodeVersionId);
        try {
            String response = PluginUtil.execute(get);
            return this.constructNodeVersion(response);
        } catch (IOException e) {
            throw new GroundException(e);
        }
    }

    Node getNode(String dbName) throws GroundException {
        HttpMethod get = new GetMethod(PluginUtil.groundServerAddress + "nodes/" + dbName);
        try {
            String response = PluginUtil.execute(get);
            if (response != null) {
                return this.constructNode(response);
            }
            return null;
        } catch (IOException e) {
            throw new GroundException(e);
        }
    }

    public List<Long> getTransitiveClosure(Long nodeVersionId) throws GroundException {
        GetMethod get = new GetMethod(PluginUtil.groundServerAddress + "nodes/closure/" + nodeVersionId);
        try {
            return (List<Long>) PluginUtil.getVersionList(get);
        } catch (IOException e) {
            throw new GroundException(e);
        }
    }

    public List<Long> getAdjacentNodes(Long prevVersionId, String edgeName) throws GroundException {
        GetMethod get = new GetMethod(PluginUtil.groundServerAddress + "adjacent/" + prevVersionId + "/" + edgeName);
        try {
            return (List<Long>) PluginUtil.getVersionList(get);
        } catch (IOException e) {
            throw new GroundException(e);
        }
    }

    // create a node using Tag
    Node createNode(String name, Map<String, Tag> tagMap) throws GroundException {
        try {
            String encodedUri = PluginUtil.groundServerAddress + "nodes/" + URLEncoder.encode(name, "UTF-8");
            PostMethod post = new PostMethod(encodedUri);
            ObjectMapper mapper = new ObjectMapper();
            String jsonString = mapper.writeValueAsString(tagMap);
            post.setRequestEntity(PluginUtil.createRequestEntity(jsonString));
            String response = PluginUtil.execute(post);
            return this.constructNode(response);
        } catch (IOException e) {
            throw new GroundException(e);
        }

    }

    // helper methods for Node and NodeVersion
    private Node constructNode(String response) throws GroundException {
        if (response == null) {
            return null;
        }
        JsonReader reader = new JsonReader(new StringReader(response));
        return PluginUtil.fromJson(reader, Node.class);
    }

    private NodeVersion constructNodeVersion(String response) throws GroundException {
        if (response == null) {
            return null;
        }
        JsonReader reader = new JsonReader(new StringReader(response));
        return PluginUtil.fromJson(reader, NodeVersion.class);
    }
}
