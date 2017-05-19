package util;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import models.models.Tag;
import play.libs.Json;

public class ControllerUtils {

  public static Map<String, Tag> getTagsFromJson(JsonNode json) {
    Map<String, Tag> tags = new HashMap<>();
    JsonNode tagsNode = json.get("tags");
    if (tagsNode != null) {
      tagsNode.forEach(tagNode -> {
        Tag tag = Json.fromJson(tagNode, Tag.class);
        tags.put(tag.getKey(), tag);
      });
    }

    return tags;
  }

  public static Map<String, String> getParametersFromJson(JsonNode json) {
    Map<String, String> referenceParameters = new HashMap<>();
    JsonNode paramsNode = json.get("parameters");
    if (paramsNode != null) {
      paramsNode.fieldNames().forEachRemaining(fieldName ->{
        String value = paramsNode.get(fieldName).asText();

        referenceParameters.put(fieldName, value);
      });
    }

    return referenceParameters;
  }

  public static List<Long> getListFromJson(JsonNode jsonNode, String fieldName) {
    List<Long> parents = new ArrayList<>();
    JsonNode listNode = jsonNode.get(fieldName);
    if (listNode != null) {
      listNode.forEach(node -> parents.add(node.asLong()));
    }

    return parents;
  }

  public static long getLongFromJson(JsonNode json, String fieldName) {
    return json.has(fieldName) ? json.get(fieldName).asLong() : -1;
  }

  public static String getStringFromJson(JsonNode json, String fieldName) {
    return json.has(fieldName) ? json.get(fieldName).asText() : null;
  }
}
