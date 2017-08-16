/**
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.berkeley.ground.cassandra.dao.core;

import com.fasterxml.jackson.databind.JsonNode;
import edu.berkeley.ground.common.dao.core.EdgeDao;
import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.exception.GroundException.ExceptionType;
import edu.berkeley.ground.common.model.core.Edge;
import edu.berkeley.ground.common.util.IdGenerator;
import edu.berkeley.ground.cassandra.dao.CqlConstants;
import edu.berkeley.ground.cassandra.dao.version.CassandraItemDao;
import edu.berkeley.ground.cassandra.util.CassandraStatements;
import edu.berkeley.ground.cassandra.util.CassandraUtils;
import java.util.List;
import edu.berkeley.ground.cassandra.util.CassandraDatabase;
import play.libs.Json;


public class CassandraEdgeDao extends CassandraItemDao<Edge> implements EdgeDao {

  public CassandraEdgeDao(CassandraDatabase dbSource, IdGenerator idGenerator) {
    super(dbSource, idGenerator);
  }

  @Override
  public Class<Edge> getType() {
    return Edge.class;
  }

  @Override
  public Edge create(Edge edge) throws GroundException {
    super.verifyItemNotExists(edge.getSourceKey());

    CassandraStatements statements;
    long uniqueId = idGenerator.generateItemId();

    Edge newEdge = new Edge(uniqueId, edge);
    try {
      statements = super.insert(newEdge);

      String name = edge.getName();

      if (name != null) {
        statements.append(String.format(CqlConstants.INSERT_EDGE_WITH_NAME, uniqueId, edge.getSourceKey(), edge.getFromNodeId(),
          edge.getToNodeId(), name));
      } else {
        statements.append(String.format(CqlConstants.INSERT_EDGE_WITHOUT_NAME, uniqueId, edge.getSourceKey(), edge.getFromNodeId(),
          edge.getToNodeId()));
      }

    } catch (Exception e) {
      throw new GroundException(e);
    }
    CassandraUtils.executeCqlList(dbSource, statements);
    return newEdge;
  }

  @Override
  protected Edge retrieve(String cql, Object field) throws GroundException {
    JsonNode json = Json.parse(CassandraUtils.executeQueryToJson(dbSource, cql));

    if (json.size() == 0) {
      throw new GroundException(ExceptionType.ITEM_NOT_FOUND, this.getType().getSimpleName(), field.toString());
    }

    Edge edge = Json.fromJson(json.get(0), Edge.class);
    long id = edge.getId();
    return new Edge(id, edge.getName(), edge.getSourceKey(), edge.getFromNodeId(), edge.getToNodeId(),
                     super.cassandraTagDao.retrieveFromDatabaseByItemId(id));
  }

  @Override
  public List<Long> getLeaves(String sourceKey) throws GroundException {
    Edge edge = retrieveFromDatabase(sourceKey);
    return super.getLeaves(edge.getId());
  }

  @Override
  public void truncate(long itemId, int numLevels) throws GroundException {
    super.truncate(itemId, numLevels);
  }
}
