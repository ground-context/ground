package edu.berkeley.ground.plugins.hive.util;

import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;

import edu.berkeley.ground.api.models.StructureVersion;
import edu.berkeley.ground.api.versions.GroundType;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.plugins.hive.GroundReadWrite;

public class PluginUtil {

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

    public static StructureVersion getStructureVersion(GroundReadWrite groundReadWrite, String entityType, String state)
            throws GroundException {
        Map<String, GroundType> structureVersionAttribs = new HashMap<>();
        structureVersionAttribs.put(state, GroundType.STRING);
        return groundReadWrite.getGroundReadWriteStructureResource().getStructureVersion(entityType,
                structureVersionAttribs);
    }
}
