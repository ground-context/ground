package edu.berkeley.ground.postgres.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import edu.berkeley.ground.common.exception.GroundException.exceptionType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import play.Logger;
import play.libs.Json;
import play.mvc.Http.Request;

public final class GroundUtils {

  private GroundUtils() {
  }

  public static ObjectNode getServerError(final Request request, final Throwable e) {
    Logger.error(
      "error: Path: {}  Message: {} Trace: {}",
      request.path(),
      e.getMessage(),
      e.getStackTrace());
    ObjectNode result = Json.newObject();
    result.put("groundExceptionMessage", "ERROR: Processing Request");
    result.put(
      "message",
      String.format("Path:%s Trace: %s", request.path(), e.getStackTrace().toString()));
    return result;
  }

  public static ObjectNode getClientError(final Request request, final Throwable e, exceptionType type) {
    Logger.error("error:  ctx: {}  Message: {} Trace: {}", request, e.getMessage(), e.getStackTrace());
    ObjectNode result = Json.newObject();

    result.put("groundExceptionMessage", String.format("Exception: Type: %s, Message: %s", type, e.getMessage()));
    result.put("groundExceptionType", type.toString());
    result.put("message", String.format("Context:%s Cause: %s", request, e.getCause().toString()));

    return result;
  }

  public static String listToJson(final List<Map<String, Object>> objList) {
    try {
      return new ObjectMapper().writeValueAsString(objList);
    } catch (IOException e) {
      throw new RuntimeException("ERROR : listToJson Converting List to Json." + e.getMessage(), e);
    }
  }

  public static List<Long> getListFromJson(JsonNode jsonNode, String fieldName) {
    List<Long> parents = new ArrayList<>();
    JsonNode listNode = jsonNode.get(fieldName);

    if (listNode != null) {
      listNode.forEach(node -> parents.add(node.asLong()));
    }

    return parents;
  }
}
