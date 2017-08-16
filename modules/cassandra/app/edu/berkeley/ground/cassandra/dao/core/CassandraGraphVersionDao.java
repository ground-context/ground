package edu.berkeley.ground.cassandra.dao.core;

import com.fasterxml.jackson.databind.JsonNode;
import edu.berkeley.ground.common.dao.core.GraphVersionDao;
import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.exception.GroundException.ExceptionType;
import edu.berkeley.ground.common.model.core.GraphVersion;
import edu.berkeley.ground.common.model.core.RichVersion;
import edu.berkeley.ground.common.util.IdGenerator;
import edu.berkeley.ground.cassandra.dao.CqlConstants;
import edu.berkeley.ground.cassandra.util.CassandraDatabase;
import edu.berkeley.ground.cassandra.util.CassandraStatements;
import edu.berkeley.ground.cassandra.util.CassandraUtils;
import java.util.ArrayList;
import java.util.List;
import play.libs.Json;


public class CassandraGraphVersionDao extends CassandraRichVersionDao<GraphVersion> implements GraphVersionDao {

  private CassandraGraphDao cassandraGraphDao;

  public CassandraGraphVersionDao(CassandraDatabase dbSource, IdGenerator idGenerator) {
    super(dbSource, idGenerator);
    this.cassandraGraphDao = new CassandraGraphDao(dbSource, idGenerator);
  }

  @Override
  public GraphVersion create(final GraphVersion graphVersion, List<Long> parentIds)
    throws GroundException {

    final long uniqueId = idGenerator.generateVersionId();
    GraphVersion newGraphVersion = new GraphVersion(uniqueId, graphVersion);

    CassandraStatements updateVersionList = this.cassandraGraphDao.update(newGraphVersion.getGraphId(), newGraphVersion.getId(), parentIds);

    try {
      CassandraStatements statements = super.insert(newGraphVersion);
      statements.append(String.format(CqlConstants.INSERT_GRAPH_VERSION, uniqueId, graphVersion.getGraphId()));
      statements.merge(updateVersionList);

      for (Long id : newGraphVersion.getEdgeVersionIds()) {
        statements.append(String.format(CqlConstants.INSERT_GRAPH_VERSION_EDGE, newGraphVersion.getId(), id));
      }

      CassandraUtils.executeCqlList(dbSource, statements);
    } catch (Exception e) {
      throw new GroundException(e);
    }
    return newGraphVersion;
  }

  @Override
  public CassandraStatements delete(long id) {
    CassandraStatements statements = new CassandraStatements();
    statements.append(String.format(CqlConstants.DELETE_ALL_GRAPH_VERSION_EDGES, "graph_version_edge", "graph_version_id", id));
    statements.append(String.format(CqlConstants.DELETE_BY_ID, "graph_version", id));

    CassandraStatements superStatements = super.delete(id);
    superStatements.merge(statements);
    return superStatements;
  }

  @Override
  public GraphVersion retrieveFromDatabase(long id) throws GroundException {
    String cql = String.format(CqlConstants.SELECT_STAR_BY_ID, "graph_version", id);
    JsonNode json = Json.parse(CassandraUtils.executeQueryToJson(dbSource, cql));

    if (json.size() == 0) {
      throw new GroundException(ExceptionType.VERSION_NOT_FOUND, this.getType().getSimpleName(), String.format("%d", id));
    }

    GraphVersion graphVersion = Json.fromJson(json.get(0), GraphVersion.class);
    List<Long> edgeIds = new ArrayList<>();
    cql = String.format(CqlConstants.SELECT_GRAPH_VERSION_EDGES, id);

    JsonNode edgeJson = Json.parse(CassandraUtils.executeQueryToJson(dbSource, cql));
    for (JsonNode edge : edgeJson) {
      edgeIds.add(edge.get("edgeVersionId").asLong());
    }

    RichVersion richVersion = super.retrieveFromDatabase(id);
    return new GraphVersion(id, richVersion.getTags(), richVersion.getStructureVersionId(), richVersion.getReference(), richVersion.getParameters(),
                             graphVersion.getGraphId(), edgeIds);
  }
}

