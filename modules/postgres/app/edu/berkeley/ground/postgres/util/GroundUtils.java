package edu.berkeley.ground.postgres.util;

import static play.mvc.Results.badRequest;
import static play.mvc.Results.internalServerError;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import edu.berkeley.ground.common.dao.version.VersionDao;
import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.exception.GroundException.ExceptionType;
import edu.berkeley.ground.common.model.core.Edge;
import edu.berkeley.ground.common.model.core.Graph;
import edu.berkeley.ground.common.model.core.Node;
import edu.berkeley.ground.common.model.core.Structure;
import edu.berkeley.ground.common.model.usage.LineageEdge;
import edu.berkeley.ground.common.model.usage.LineageGraph;
import edu.berkeley.ground.common.util.IdGenerator;
import edu.berkeley.ground.postgres.dao.core.PostgresEdgeVersionDao;
import edu.berkeley.ground.postgres.dao.core.PostgresGraphVersionDao;
import edu.berkeley.ground.postgres.dao.core.PostgresNodeVersionDao;
import edu.berkeley.ground.postgres.dao.core.PostgresStructureVersionDao;
import edu.berkeley.ground.postgres.dao.usage.PostgresLineageEdgeVersionDao;
import edu.berkeley.ground.postgres.dao.usage.PostgresLineageGraphVersionDao;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import play.Logger;
import play.db.Database;
import play.libs.Json;
import play.mvc.Http.Request;
import play.mvc.Result;

public final class GroundUtils {

  private GroundUtils() {
  }

  public static Result handleException(Throwable e, Request request) {
    System.out.println(e.getClass().getSimpleName());
    System.out.println(e.getCause().getClass().getSimpleName());
    System.out.println(e.getCause().getCause().getClass().getSimpleName());
    if (e.getCause().getCause() instanceof GroundException) {
      return badRequest(GroundUtils.getClientError(request, e.getCause().getCause(), ExceptionType.ITEM_NOT_FOUND));
    } else {
      return internalServerError(GroundUtils.getServerError(request, e.getCause().getCause()));
    }
  }

  private static ObjectNode getServerError(final Request request, final Throwable e) {
    Logger.error("Error! Request Path: {}\nError Message: {}\n Stack Trace: {}", request.path(), e.getMessage(), e.getStackTrace());

    ObjectNode result = Json.newObject();
    result.put("Error", "Unexpected error while processing request.");
    result.put("Request Path", request.path());

    StringWriter sw = new StringWriter();
    e.printStackTrace(new PrintWriter(sw));
    result.put("Stack Trace", sw.toString());
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

  public static VersionDao<?> getVersionDaoFromItemType(Class<?> klass, Database dbSource, IdGenerator idGenerator) throws GroundException {
    if (klass.equals(Node.class)) {
      return new PostgresNodeVersionDao(dbSource, idGenerator);
    } else if (klass.equals(Edge.class)) {
      return new PostgresEdgeVersionDao(dbSource, idGenerator);
    } else if (klass.equals(Graph.class)) {
      return new PostgresGraphVersionDao(dbSource, idGenerator);
    } else if (klass.equals(Structure.class)) {
      return new PostgresStructureVersionDao(dbSource, idGenerator);
    } else if (klass.equals(LineageEdge.class)) {
      return new PostgresLineageEdgeVersionDao(dbSource, idGenerator);
    } else if (klass.equals(LineageGraph.class)) {
      return new PostgresLineageGraphVersionDao(dbSource, idGenerator);
    } else {
      throw new GroundException(ExceptionType.OTHER, String.format("Unknown class :%s.", klass.getSimpleName()));
    }
  }
}
