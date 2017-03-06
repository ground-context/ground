/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.berkeley.ground.plugins.hive;

import java.io.IOException;
import java.io.InputStream;
import java.net.URLEncoder;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import edu.berkeley.ground.api.models.*;
import edu.berkeley.ground.api.versions.GroundType;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.util.HttpURLConnection;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import edu.berkeley.ground.exceptions.GroundDBException;
import edu.berkeley.ground.exceptions.GroundException;

public class GroundReadWrite {

    private static final String GROUNDCONF = "ground.properties";
    private static final String DEFAULT_ADDRESS = "http://localhost:9090/";
    public static final String NO_CACHE_CONF = null;
    HttpClient client = new HttpClient();
    private final String groundServerAddress;

    public GroundReadWrite() throws GroundDBException {
        Properties properties = new Properties();
        try {
            String groundPropertyResource = GROUNDCONF; // ground properties
                                                        // from resources
            ClassLoader loader = Thread.currentThread().getContextClassLoader();
            try (InputStream resourceStream = loader.getResourceAsStream(groundPropertyResource)) {
                properties.load(resourceStream);
                resourceStream.close();
            }
            //
            groundServerAddress = properties.getProperty("edu.berkeley.ground.server.address", DEFAULT_ADDRESS);
        } catch (Exception e) {
            throw new GroundDBException(e);
        }
    }

    // create node for input Tag
    public Node createNode(String name) throws GroundException {
        try {
            String encodedUri = groundServerAddress + "nodes/" + URLEncoder.encode(name, "UTF-8");
            PostMethod post = new PostMethod(encodedUri);
            post.setRequestHeader("Content-type", "application/json");
            return getNode(post);
        } catch (IOException e) {
            throw new GroundException(e);
        }

    }

    public Node getNode(String dbName) throws GroundException {
        GetMethod get = new GetMethod(groundServerAddress + "nodes/" + dbName);
        try {
            return getNode(get);
        } catch (IOException e) {
            throw new GroundException(e);
        }
    }

    // method to create the NodeVersion given the nodeId and the tags
    public NodeVersion createNodeVersion(long id, Map<String, Tag> tags, long structureVersionId, String reference,
            Map<String, String> referenceParameters, long nodeId) throws GroundException {
        try {
            NodeVersion nodeVersion = getNodeVersion(nodeId);
            if (nodeVersion != null) return nodeVersion;
            nodeVersion = new NodeVersion(id, tags, structureVersionId, reference, referenceParameters,
                    nodeId);
            ObjectMapper mapper = new ObjectMapper();
            String jsonString = mapper.writeValueAsString(nodeVersion);
            String uri = groundServerAddress + "nodes/versions";
            PostMethod post = new PostMethod(uri);
            post.setRequestHeader("Content-type", "application/json");
            post.setRequestBody(jsonString);
            return getNodeVersion(post);
        } catch (IOException e) {
            throw new GroundException(e);
        }
    }

    public NodeVersion getNodeVersion(long nodeVersionId) throws GroundException {
        GetMethod get = new GetMethod(groundServerAddress + "nodes/versions/" + nodeVersionId);
        try {
            return getNodeVersion(get);
        } catch (IOException e) {
            throw new GroundException(e);
        }
    }

    public List<Long> getLatestVersions(String name) throws GroundException {
        GetMethod get = new GetMethod(groundServerAddress + "nodes/" + name + "/latest");
        try {
            return getList(get);
        } catch (IOException e) {
            throw new GroundException(e);
        }
    }

    // create edge for input Tag
    public Edge createEdge(String name) throws GroundException {
        try {
            String encodedUri = groundServerAddress + "edges/" + URLEncoder.encode(name, "UTF-8");
            PostMethod post = new PostMethod(encodedUri);
            post.setRequestHeader("Content-type", "application/json");
            return getEdge(post);
        } catch (IOException e) {
            throw new GroundException(e);
        }
    }

    public Edge getEdge(String edgeName) throws GroundException {
        GetMethod get = new GetMethod(groundServerAddress + "edges/" + edgeName);
        try {
            return getEdge(get);
        } catch (IOException e) {
            throw new GroundException(e);
        }
    }

    public EdgeVersion getEdgeVersion(long edgeId) throws GroundException {
        GetMethod get = new GetMethod(groundServerAddress + "edges/versions" + edgeId);
        try {
            return getEdgeVersion(get);
        } catch (IOException e) {
            throw new GroundException(e);
        }
    }

    // create node for input Tag
    public Structure createStructure(String name) throws GroundException {
        try {
            String encodedUri = groundServerAddress + "structures/" + URLEncoder.encode(name, "UTF-8");
            PostMethod post = new PostMethod(encodedUri);
            post.setRequestHeader("Content-type", "application/json");
            return getStructure(post);
        } catch (IOException ioe) {
            throw new GroundException(ioe);
        }
    }

    // method to create StructureVersion given the nodeId and the tags
    public StructureVersion createStructureVersion(long id, long structureVersionId, Map<String, GroundType> attributes)
            throws GroundException {
        StructureVersion structureVersion = new StructureVersion(id, structureVersionId, attributes);
        try {
            ObjectMapper mapper = new ObjectMapper();
            String jsonString = mapper.writeValueAsString(structureVersion);
            String uri = groundServerAddress + "structures/versions";
            PostMethod post = new PostMethod(uri);
            post.setRequestHeader("Content-type", "application/json");
            post.setRequestBody(jsonString);
            return getStructureVersion(post);
        } catch (IOException e) {
            throw new GroundException(e);
        }
    }

    public Structure getStructure(String dbName) throws GroundException {
        GetMethod get = new GetMethod(groundServerAddress + "structures/" + dbName);
        try {
            return getStructure(get);
        } catch (IOException e) {
            throw new GroundException(e);
        }
    }

    public StructureVersion getStructureVersion(long id) throws GroundException {
        GetMethod get = new GetMethod(groundServerAddress + "structures/versions/" + id);
        try {
            return getStructureVersion(get);
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
            String jsonString = mapper.writeValueAsString(edgeVersion);
            String uri = groundServerAddress + "edges/versions";
            PostMethod post = new PostMethod(uri);
            post.setRequestHeader("Content-type", "application/json");
            post.setRequestBody(jsonString);
            return getEdgeVersion(post);
        } catch (IOException e) {
            throw new GroundException(e);
        }
    }

    public List<Long> getAdjacentNodes(Long prevVersionId, String edgeName) throws GroundException {
        GetMethod get = new GetMethod(groundServerAddress + "adjacent/" + prevVersionId + "/" + edgeName);
        try {
            return getList(get);
        } catch (IOException e) {
            throw new GroundException(e);
        }
    }

    String checkStatus(HttpMethod method) throws GroundException {
        try {
            if (client.executeMethod(method) == HttpURLConnection.HTTP_OK) {
                ObjectMapper objectMapper = new ObjectMapper();
                String text = method.getResponseBodyAsString();
                JsonNode jsonNode = objectMapper.readValue(text, JsonNode.class);
                JsonNode nodeId = jsonNode.get("id");
                return nodeId.asText();
            }
        } catch (IOException e) {
            throw new GroundException(e);
        }
        return null;
    }

    private Node getNode(HttpMethod method)
            throws IOException, HttpException, JsonParseException, JsonMappingException {
        if (client.executeMethod(method) == HttpURLConnection.HTTP_OK) {
            // getting the nodeId of the node created
            String text = method.getResponseBodyAsString();
            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.readValue(text, Node.class);
        }
        return null;
    }

    private NodeVersion getNodeVersion(HttpMethod method)
            throws IOException, HttpException, JsonParseException, JsonMappingException {
        if (client.executeMethod(method) == HttpURLConnection.HTTP_OK) {
            // getting the nodeId of the node created
            String text = method.getResponseBodyAsString();
            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.readValue(text, NodeVersion.class);
        }
        return null;
    }

    private Structure getStructure(HttpMethod method) throws IOException {
        if (client.executeMethod(method) == HttpURLConnection.HTTP_OK) {
            // getting the nodeId of the node created
            String text = method.getResponseBodyAsString();
            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.readValue(text, Structure.class);
        }
        return null;
    }

    private StructureVersion getStructureVersion(HttpMethod method) throws IOException {
        if (client.executeMethod(method) == HttpURLConnection.HTTP_OK) {
            // getting the nodeId of the node created
            String text = method.getResponseBodyAsString();
            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.readValue(text, StructureVersion.class);
        }
        return null;
    }

    private List<Long> getList(HttpMethod method)
            throws JsonParseException, JsonMappingException, HttpException, IOException {
        if (client.executeMethod(method) == HttpURLConnection.HTTP_OK) {
            // getting the nodeId of the node created
            String text = method.getResponseBodyAsString();
            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.readValue(text, List.class);
        }
        return null;
    }

    private Edge getEdge(HttpMethod method)
            throws JsonParseException, JsonMappingException, HttpException, IOException {
        if (client.executeMethod(method) == HttpURLConnection.HTTP_OK) {
            // getting the nodeId of the node created
            String text = method.getResponseBodyAsString();
            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.readValue(text, Edge.class);
        }
        return null;
    }

    private EdgeVersion getEdgeVersion(HttpMethod method)
            throws JsonParseException, JsonMappingException, HttpException, IOException {
        if (client.executeMethod(method) == HttpURLConnection.HTTP_OK) {
            // getting the nodeId of the node created
            String text = method.getResponseBodyAsString();
            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.readValue(text, EdgeVersion.class);
        }
        return null;
    }

    public List<Long> getTransitiveClosure(Long tableNodeVersionId) throws GroundException {
        GetMethod get = new GetMethod(groundServerAddress + "nodes/" + tableNodeVersionId + "/closure");
        try {
            return getList(get);
        } catch (IOException e) {
            throw new GroundException(e);
        }
    }

}
