package edu.berkeley.ground.plugins.hive.util;

import com.google.gson.Gson;

public class JsonUtil {

  private JsonUtil() {
  }

  public static <T> T fromJSON(String json, Class<T> clazz) {
    Gson gson = new Gson();
    return gson.fromJson(json.replace("\\", ""), clazz);
  }

  public static String toJSON(Object object) {
    Gson gson = new Gson();
    return gson.toJson(object);
  }
}
