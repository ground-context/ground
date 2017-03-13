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
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.HashMap;
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
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.commons.httpclient.util.HttpURLConnection;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.berkeley.ground.exceptions.GroundDBException;
import edu.berkeley.ground.exceptions.GroundException;

public class GroundReadWrite {

    static final private Logger logger = LoggerFactory.getLogger(GroundReadWrite.class.getName());

    private static final String GROUNDCONF = "ground.properties";
    private static final String DEFAULT_ADDRESS = "http://localhost:9090/";
    public static final String NO_CACHE_CONF = "no_cache_conf";
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

    // method to create the NodeVersion given the nodeId and the tags
    public NodeVersion createNodeVersion(long id, Map<String, Tag> tags, long structureVersionId, String reference,
            Map<String, String> referenceParameters, String name) throws GroundException {
        try {
            Node node = this.createNode(name, tags);
            NodeVersion nodeVersion = new NodeVersion(id, tags, structureVersionId, reference, referenceParameters,
                    node.getId());
            ObjectMapper mapper = new ObjectMapper();
            String jsonString = mapper.writeValueAsString(nodeVersion);
            String uri = groundServerAddress + "nodes/versions";
            PostMethod post = new PostMethod(uri);
            post.setRequestEntity(createRequestEntity(jsonString));
            String response = this.execute(post);
            return constructNodeVersion(response);
        } catch (IOException e) {
            throw new GroundException(e);
        }
    }

    public NodeVersion getNodeVersion(long nodeVersionId) throws GroundException {
        GetMethod get = new GetMethod(groundServerAddress + "nodes/versions/" + nodeVersionId);
        try {
            String response = this.execute(get);
            return this.constructNodeVersion(response);
        } catch (IOException e) {
            throw new GroundException(e);
        }
    }

    public List<Long> getLatestVersions(String name, String type) throws GroundException {
        GetMethod get = new GetMethod(groundServerAddress + type + "/" + name + "/latest");
        try {
            return (List<Long>) getVersionList(get);
        } catch (IOException e) {
            throw new GroundException(e);
        }
    }

    // create a node using Tag
    Node createNode(String name, Map<String, Tag> tagMap) throws GroundException {
        try {
            String encodedUri = groundServerAddress + "nodes/" + URLEncoder.encode(name, "UTF-8");
            PostMethod post = new PostMethod(encodedUri);
            ObjectMapper mapper = new ObjectMapper();
            String jsonString = mapper.writeValueAsString(tagMap);
            post.setRequestEntity(createRequestEntity(jsonString));
            String response = this.execute(post);
            return this.constructNode(response);
        } catch (IOException e) {
            throw new GroundException(e);
        }

    }

    Node getNode(String dbName) throws GroundException {
        HttpMethod get = new GetMethod(groundServerAddress + "nodes/" + dbName);
        try {
            String response = this.execute(get);
            if (response != null) {
                return this.constructNode(response);
            }
            return null;
        } catch (IOException e) {
            throw new GroundException(e);
        }
    }

    //helper methods for Node and NodeVersion
    private Node constructNode(String response) throws GroundException {
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode;
        try {
            jsonNode = objectMapper.readValue(response, JsonNode.class);
        } catch (IOException e) {
            throw new GroundException(e);
        }
        long id = jsonNode.get("id").asLong();
        String name = jsonNode.get("name").asText();
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Tag> tags = mapper.convertValue(jsonNode.get("tags"), Map.class);
        return new Node(id, name, tags);
    }

    private NodeVersion constructNodeVersion(String response) throws GroundException {
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode;
        try {
            jsonNode = objectMapper.readValue(response, JsonNode.class);
        } catch (IOException e) {
            throw new GroundException(e);
        }
        long id = jsonNode.get("id").asLong();
        long structureVersionId = jsonNode.get("structureVersionId").asLong();
        Map<String, Tag> tags = objectMapper.convertValue(jsonNode.get("tags"),
                new TypeReference<Map<String, Tag>>() {});
        String reference = jsonNode.get("reference").asText();
        Map<String, String> referenceMap =
                objectMapper.convertValue(jsonNode.get("referenceParameters"), Map.class);
        long nodeId = jsonNode.get("nodeId").asLong();
        return new NodeVersion(id, tags, structureVersionId, reference, referenceMap, nodeId);
    }


    // create edge for input Tag
    public Edge createEdge(String name, Map<String, Tag> tagMap) throws GroundException {
        try {
            String encodedUri = groundServerAddress + "edges/" + URLEncoder.encode(name, "UTF-8");
            PostMethod post = new PostMethod(encodedUri);
            ObjectMapper objectMapper = new ObjectMapper();
            String jsonString = objectMapper.writeValueAsString(tagMap);
            post.setRequestEntity(createRequestEntity(jsonString));
            String response = this.execute(post);
            return this.constructEdge(response);
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

    private Edge constructEdge(String response)
            throws GroundException {
        JsonNode jsonNode;
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            jsonNode = objectMapper.readValue(response, JsonNode.class);
            long id = jsonNode.get("id").asLong();
            String name = jsonNode.get("name").asText();
            Map<String, Tag> tags = objectMapper.convertValue(jsonNode.get("tags"), Map.class);
            return new Edge(id, name, tags);
        } catch (IOException e) {
            throw new GroundException(e);
        }
    }

    // create node for input Tag
    Structure createStructure(String name) throws GroundException {
        try {
            String encodedUri = groundServerAddress + "structures/" + URLEncoder.encode(name, "UTF-8");
            PostMethod post = new PostMethod(encodedUri);
            ObjectMapper mapper = new ObjectMapper();
            Tag tag = new Tag(-1L, name, "active", GroundType.STRING);
            Map<String, Tag>tags = new HashMap<>();
            tags.put(name, tag);
            String jsonRecord = mapper.writeValueAsString(tags);
            StringRequestEntity requestEntity = createRequestEntity(jsonRecord);
            post.setRequestEntity(requestEntity);
            String response = execute(post);
            return constructStructure(response);
        } catch (IOException ioe) {
            throw new GroundException(ioe);
        }
    }

    // method to create StructureVersion given the nodeId and the tags
    StructureVersion createStructureVersion(long id, long structureId, Map<String, GroundType> attributes)
            throws GroundException {
        StructureVersion structureVersion = new StructureVersion(id, structureId, attributes);
        try {
            ObjectMapper mapper = new ObjectMapper();
            String jsonString = mapper.writeValueAsString(structureVersion);
            String uri = groundServerAddress + "structures/versions";
            PostMethod post = new PostMethod(uri);
            post.setRequestEntity(this.createRequestEntity(jsonString));
            String response = this.execute(post);
            return constructStructureVersion(response);
        } catch (IOException e) {
            throw new GroundException(e);
        }
    }

    Structure getStructure(String name) throws GroundException {
        GetMethod get = new GetMethod(groundServerAddress + "structures/" + name);
        try {
            String response = this.execute(get);
            if (response != null) {
                return constructStructure(response);
            }
            return createStructure(name);
        } catch (IOException e) {
            throw new GroundException(e);
        }
    }

    // this is the only public API needed for creating and accessing StructureVersion
    public StructureVersion getStructureVersion(String name, Map<String, GroundType> tags)
            throws GroundException {
        List<Long> versions = (List<Long>) getLatestVersions(name, "structures");
        if (versions != null && !versions.isEmpty()) {
            logger.info("getting versions: {}, {}", versions.size(), versions.get(0));
            return new StructureVersion(versions.get(0), getStructure(name).getId(), tags);
        } else {
            return createStructureVersion(1L, getStructure(name).getId(), tags);
        }
    }

    private Structure constructStructure(String response) throws IOException {
        // getting the nodeId of the node created
        if (response!= null && !response.isEmpty()) {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonNode = objectMapper.readValue(response, JsonNode.class);
            long id = jsonNode.get("id").asLong();
            String name = jsonNode.get("name").asText();
            // ObjectMapper mapper = new ObjectMapper();
            Map<String, Tag> tags = objectMapper.convertValue(jsonNode.get("tags"), Map.class);
            return new Structure(id, name, tags);
        }
        return null;
    }

    private StructureVersion constructStructureVersion(String response)
            throws IOException {
        if (response!= null && !response.isEmpty()) {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonNode = objectMapper.readValue(response, JsonNode.class);
            long id = jsonNode.get("id").asLong();
            long structureId = jsonNode.get("structureId").asLong();
            Map<String, GroundType> attributes =
                    objectMapper.convertValue(jsonNode.get("attributes"), Map.class);
            return new StructureVersion(id, structureId, attributes);
        }
        return null;
    }

    // method to create the edgeVersion given the nodeId and the tags
    public EdgeVersion createEdgeVersion(long id, Map<String, Tag> tags, long structureVersionId, String reference,
            Map<String, String> referenceParameters, long edgeId, long fromId, long toId) throws GroundException {
        try {
            EdgeVersion edgeVersion = new EdgeVersion(id, tags, structureVersionId, reference, referenceParameters,
                    edgeId, fromId, toId);
            ObjectMapper mapper = new ObjectMapper();
            String jsonRecord = mapper.writeValueAsString(edgeVersion);
            String uri = groundServerAddress + "edges/versions";
            PostMethod post = new PostMethod(uri);
            StringRequestEntity requestEntity = createRequestEntity(jsonRecord);
            post.setRequestEntity(requestEntity);
            return getEdgeVersion(post);
        } catch (IOException e) {
            throw new GroundException(e);
        }
    }

    private String execute(HttpMethod method)
            throws IOException, HttpException, JsonParseException, JsonMappingException {
        if (client.executeMethod(method) == HttpURLConnection.HTTP_OK) {
            // getting the nodeId of the node created
            return method.getResponseBodyAsString();
        }
        return null;
    }

    private StringRequestEntity createRequestEntity(String jsonRecord)
            throws UnsupportedEncodingException {
        StringRequestEntity requestEntity = new StringRequestEntity(jsonRecord, "application/json", "UTF-8");
        return requestEntity;
    }

    public List<Long> getAdjacentNodes(Long prevVersionId, String edgeName) throws GroundException {
        GetMethod get = new GetMethod(groundServerAddress + "adjacent/" + prevVersionId + "/" + edgeName);
        try {
            return (List<Long>) getVersionList(get);
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

    private List<Long> getVersionList(HttpMethod method)
            throws JsonParseException, JsonMappingException, HttpException, IOException {
        if (client.executeMethod(method) == HttpURLConnection.HTTP_OK) {
            // getting the nodeId of the node created
            String response = method.getResponseBodyAsString();
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonNode = objectMapper.readValue(response, JsonNode.class);
            List<Long> list = objectMapper.readValue(objectMapper.treeAsTokens(jsonNode),
                    new TypeReference<List<Long>>(){});
            return list;
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
            return (List<Long>) getVersionList(get);
        } catch (IOException e) {
            throw new GroundException(e);
        }
    }

}
