package edu.berkeley.ground.cassandra.dao.core;

import com.fasterxml.jackson.databind.JsonNode;
import edu.berkeley.ground.common.dao.core.NodeVersionDao;
import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.exception.GroundException.ExceptionType;
import edu.berkeley.ground.common.model.core.NodeVersion;
import edu.berkeley.ground.common.model.core.RichVersion;
import edu.berkeley.ground.common.util.IdGenerator;
import edu.berkeley.ground.cassandra.dao.CqlConstants; // Andre - SQL to CQL
import edu.berkeley.ground.cassandra.util.CassandraDatabase;
import edu.berkeley.ground.cassandra.util.CassandraStatements;
import edu.berkeley.ground.cassandra.util.CassandraUtils;
import java.util.List;
//import play.db.Database;
import play.libs.Json;

public class CassandraNodeVersionDao extends CassandraRichVersionDao<NodeVersion> implements NodeVersionDao {

  private CassandraNodeDao cassandraNodeDao;

  public CassandraNodeVersionDao(CassandraDatabase dbSource, IdGenerator idGenerator) {
    super(dbSource, idGenerator);
    this.cassandraNodeDao = new CassandraNodeDao(dbSource, idGenerator);
  }

  @Override
  public NodeVersion create(final NodeVersion nodeVersion, List<Long> parentIds)
    throws GroundException {

    final long uniqueId = idGenerator.generateVersionId();
    NodeVersion newNodeVersion = new NodeVersion(uniqueId, nodeVersion);

    CassandraStatements updateVersionList = this.cassandraNodeDao.update(newNodeVersion.getNodeId(), newNodeVersion.getId(), parentIds); // Andre - Problem line

    try {
      CassandraStatements statements = super.insert(newNodeVersion); // Andre - SQL to CQL
      statements.append(String.format(CqlConstants.INSERT_NODE_VERSION, uniqueId, nodeVersion.getNodeId()));
      statements.merge(updateVersionList);

      CassandraUtils.executeCqlList(dbSource, statements);  // Andre - SQL to CQL
    } catch (Exception e) {
      e.printStackTrace();
      throw new GroundException(e);
    }
    return newNodeVersion;
  }

  @Override
  public CassandraStatements delete(long id) { // Andre - Modify CassandraStatements?
    CassandraStatements statements = new CassandraStatements();
    statements.append(String.format(CqlConstants.DELETE_BY_ID, "node_version", id));

    CassandraStatements superStatements = super.delete(id);
    superStatements.merge(statements);
    return superStatements;
  }

  @Override
  public NodeVersion retrieveFromDatabase(long id) throws GroundException { // Andre - refresh on Cassandra queries
    String cql = String.format(CqlConstants.SELECT_STAR_BY_ID, "node_version", id);
    JsonNode json = Json.parse(CassandraUtils.executeQueryToJson(dbSource, cql));

    if (json.size() == 0) {
      throw new GroundException(ExceptionType.VERSION_NOT_FOUND, this.getType().getSimpleName(), String.format("%d", id));
    }

    NodeVersion nodeVersion = Json.fromJson(json.get(0), NodeVersion.class);
    RichVersion richVersion = super.retrieveFromDatabase(id);

    return new NodeVersion(id, richVersion, nodeVersion);
  }
}
