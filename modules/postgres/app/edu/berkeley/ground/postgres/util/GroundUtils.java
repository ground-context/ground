package edu.berkeley.ground.postgres.util;

import static play.mvc.Results.badRequest;
import static play.mvc.Results.internalServerError;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.exception.GroundException.ExceptionType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import play.Logger;
import play.libs.Json;
import play.mvc.Http.Request;
import play.mvc.Result;

public final class GroundUtils {

  private GroundUtils() {
  }

  public static Result handleException(Throwable e, Request request) {
    if (e.getCause() instanceof GroundException) {
      return badRequest(GroundUtils.getClientError(request, e, ExceptionType.ITEM_NOT_FOUND));
    } else {
      return internalServerError(GroundUtils.getServerError(request, e));
    }
  }

  private static ObjectNode getServerError(final Request request, final Throwable e) {
    Logger.error("Error! Request Path: {}\nError Message: {}\n Stack Trace: {}", request.path(), e.getMessage(), e.getStackTrace());

    ObjectNode result = Json.newObject();
    result.put("Error", "Unexpected error while processing request.");
    result.put("Request Path", request.path());
    result.put("Stack Trace", e.getStackTrace().toString());
    return result;
  }

  private static ObjectNode getClientError(final Request request, final Throwable e, ExceptionType type) {
    Logger.error("Error! Request Path: {}\nError Message: {}\n Stack Trace: {}", request.path(), e.getMessage(), e.getStackTrace());

    ObjectNode result = Json.newObject();

    result.put("Error", String.format("The request to %s was invalid.", request.path()));
    result.put("Message", String.format("%s", e.getMessage()));

    return result;
  }

  static String listToJson(final List<Map<String, Object>> objList) {
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
