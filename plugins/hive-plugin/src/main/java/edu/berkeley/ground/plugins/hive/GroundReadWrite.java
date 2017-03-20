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
import java.util.List;
import java.util.Properties;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.commons.httpclient.util.HttpURLConnection;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.berkeley.ground.exceptions.GroundException;

public class GroundReadWrite {

    static final private Logger logger = LoggerFactory.getLogger(GroundReadWrite.class.getName());

    private static final String GROUNDCONF = "ground.properties";
    private static final String DEFAULT_ADDRESS = "http://localhost:9090/";
    public static final String NO_CACHE_CONF = "no_cache_conf";
    static HttpClient client = new HttpClient();
    protected static String groundServerAddress;

    private GroundReadWriteNodeResource groundReadWriteNodeResource;
    private GroundReadWriteStructureResource groundReadWriteStructureResource;
    private GroundReadWriteEdgeResource groundReadWriteEdgeResource;

    public GroundReadWrite() throws GroundException {
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
            setGroundReadWriteNodeResource(new GroundReadWriteNodeResource());
            setGroundReadWriteEdgeResource(new GroundReadWriteEdgeResource());
            setGroundReadWriteStructureResource(new GroundReadWriteStructureResource());
        } catch (Exception e) {
            throw new GroundException(e);
        }
    }

    public static List<Long> getLatestVersions(String name, String type) throws GroundException {
        GetMethod get = new GetMethod(groundServerAddress + type + "/" + name + "/latest");
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

    protected static String execute(HttpMethod method)
            throws IOException, HttpException, JsonParseException, JsonMappingException {
        if (client.executeMethod(method) == HttpURLConnection.HTTP_OK) {
            // getting the nodeId of the node created
            return method.getResponseBodyAsString();
        }
        return null;
    }

    protected static StringRequestEntity createRequestEntity(String jsonRecord)
            throws UnsupportedEncodingException {
        StringRequestEntity requestEntity = new StringRequestEntity(jsonRecord, "application/json", "UTF-8");
        return requestEntity;
    }

    protected static List<Long> getVersionList(HttpMethod method)
            throws JsonParseException, JsonMappingException, HttpException, IOException {
        if (client.executeMethod(method) == HttpURLConnection.HTTP_OK) {
            // getting the nodeId of the node created
            String response = method.getResponseBodyAsString();
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonNode = objectMapper.readValue(response, JsonNode.class);
            List<Long> list = objectMapper.readValue(objectMapper.treeAsTokens(jsonNode),
                    new TypeReference<List<Long>>() {
                    });
            return list;
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

    public GroundReadWriteNodeResource getGroundReadWriteNodeResource() {
        return groundReadWriteNodeResource;
    }

    public void setGroundReadWriteNodeResource(GroundReadWriteNodeResource groundReadWriteNodeResource) {
        this.groundReadWriteNodeResource = groundReadWriteNodeResource;
    }

    public GroundReadWriteStructureResource getGroundReadWriteStructureResource() {
        return groundReadWriteStructureResource;
    }

    public void setGroundReadWriteStructureResource(GroundReadWriteStructureResource groundReadWriteStructureResource) {
        this.groundReadWriteStructureResource = groundReadWriteStructureResource;
    }

    public GroundReadWriteEdgeResource getGroundReadWriteEdgeResource() {
        return groundReadWriteEdgeResource;
    }

    public void setGroundReadWriteEdgeResource(GroundReadWriteEdgeResource groundReadWriteEdgeResource) {
        this.groundReadWriteEdgeResource = groundReadWriteEdgeResource;
    }

}
