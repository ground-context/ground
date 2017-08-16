package edu.berkeley.ground.cassandra.util;

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
import edu.berkeley.ground.cassandra.dao.core.CassandraEdgeVersionDao;
import edu.berkeley.ground.cassandra.dao.core.CassandraGraphVersionDao;
import edu.berkeley.ground.cassandra.dao.core.CassandraNodeVersionDao;
import edu.berkeley.ground.cassandra.dao.core.CassandraStructureVersionDao;
import edu.berkeley.ground.cassandra.dao.usage.CassandraLineageEdgeVersionDao;
import edu.berkeley.ground.cassandra.dao.usage.CassandraLineageGraphVersionDao;
import edu.berkeley.ground.cassandra.util.CassandraDatabase;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
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
      Logger.debug("Andre: " + new ObjectMapper().writeValueAsString(objList));
      return new ObjectMapper().writeValueAsString(objList);
    } catch (IOException e) {
      throw new RuntimeException("ERROR : listToJson Converting List to JSON." + e.getMessage(), e);
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

  public static VersionDao<?> getVersionDaoFromItemType(Class<?> klass, CassandraDatabase dbSource, IdGenerator idGenerator) throws GroundException {
    if (klass.equals(Node.class)) {
      return new CassandraNodeVersionDao(dbSource, idGenerator);
    } else if (klass.equals(Edge.class)) {
      return new CassandraEdgeVersionDao(dbSource, idGenerator);
    } else if (klass.equals(Graph.class)) {
      return new CassandraGraphVersionDao(dbSource, idGenerator);
    } else if (klass.equals(Structure.class)) {
      return new CassandraStructureVersionDao(dbSource, idGenerator);
    } else if (klass.equals(LineageEdge.class)) {
      return new CassandraLineageEdgeVersionDao(dbSource, idGenerator);
    } else if (klass.equals(LineageGraph.class)) {
      return new CassandraLineageGraphVersionDao(dbSource, idGenerator);
    } else {
      throw new GroundException(ExceptionType.OTHER, String.format("Unknown class :%s.", klass.getSimpleName()));
    }
  }
}
