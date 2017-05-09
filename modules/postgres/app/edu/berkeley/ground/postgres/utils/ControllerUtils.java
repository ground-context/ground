package edu.berkeley.ground.postgres.utils;

  import com.fasterxml.jackson.databind.JsonNode;
  import java.util.ArrayList;
  import java.util.List;


public class ControllerUtils {

  public static List<Long> getListFromJson(JsonNode jsonNode, String fieldName) {
    List<Long> parents = new ArrayList<>();
    JsonNode listNode = jsonNode.get(fieldName);
    if (listNode != null) {
      listNode.forEach(node -> parents.add(node.asLong()));
    }
    return parents;
  }
}
