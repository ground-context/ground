package edu.berkeley.ground.plugins.hive.util;

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

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;

import edu.berkeley.ground.exceptions.GroundException;

public class PluginUtil {

    static final private Logger logger = LoggerFactory.getLogger(PluginUtil.class);
            
    private static final String GROUNDCONF = "ground.properties";
    private static final String DEFAULT_ADDRESS = "http://localhost:9090/";
    public static final HttpClient client = new HttpClient();
    public static final String groundServerAddress;

    static {
        Properties properties = new Properties();
        try {
            String groundPropertyResource = GROUNDCONF; // ground properties
                                                        // from resources
            ClassLoader loader = Thread.currentThread().getContextClassLoader();
            try (InputStream resourceStream = loader.getResourceAsStream(groundPropertyResource)) {
                properties.load(resourceStream);
                resourceStream.close();
            }
        } catch (Exception e) {
            logger.error("error initializing properties: {}", e);
        }
        groundServerAddress = properties.getProperty("edu.berkeley.ground.server.address", DEFAULT_ADDRESS);
    }

    private PluginUtil() {
    }

    public static <T> T fromJson(String json, Class<T> clazz) {
        Gson gson = new Gson();
        return gson.fromJson(json.replace("\\", ""), clazz);
    }

    public static String toJson(Object object) {
        Gson gson = new Gson();
        return gson.toJson(object);
    }

    public static <T> T fromJson(JsonReader reader, Class<T> clazz) {
        reader.setLenient(true);
        Gson gson = new Gson();
        return gson.fromJson(reader, clazz);
    }

    public static List<Long> getLatestVersions(String name, String type) throws GroundException {
        GetMethod get = new GetMethod(groundServerAddress + type + "/" + name + "/latest");
        try {
            return (List<Long>) getVersionList(get);
        } catch (IOException e) {
            throw new GroundException(e);
        }
    }

    public static String execute(HttpMethod method)
            throws IOException, HttpException, JsonParseException, JsonMappingException {
        if (client.executeMethod(method) == HttpURLConnection.HTTP_OK) {
            // getting the nodeId of the node created
            return method.getResponseBodyAsString();
        }
        return null;
    }

    public static StringRequestEntity createRequestEntity(String jsonRecord)
            throws UnsupportedEncodingException {
        StringRequestEntity requestEntity = new StringRequestEntity(jsonRecord, "application/json", "UTF-8");
        return requestEntity;
    }

    public static List<Long> getVersionList(HttpMethod method)
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

}
