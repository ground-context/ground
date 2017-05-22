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
package edu.berkeley.ground.postgres.dao.core;

import com.fasterxml.jackson.databind.JsonNode;
import edu.berkeley.ground.common.dao.core.EdgeDao;
import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.exception.GroundException.ExceptionType;
import edu.berkeley.ground.common.model.core.Edge;
import edu.berkeley.ground.common.util.IdGenerator;
import edu.berkeley.ground.postgres.dao.SqlConstants;
import edu.berkeley.ground.postgres.dao.version.PostgresItemDao;
import edu.berkeley.ground.postgres.util.PostgresStatements;
import edu.berkeley.ground.postgres.util.PostgresUtils;
import java.util.List;
import play.db.Database;
import play.libs.Json;


public class PostgresEdgeDao extends PostgresItemDao<Edge> implements EdgeDao {

  public PostgresEdgeDao(Database dbSource, IdGenerator idGenerator) {
    super(dbSource, idGenerator);
  }

  @Override
  public Class<Edge> getType() {
    return Edge.class;
  }

  @Override
  public Edge create(Edge edge) throws GroundException {
    super.verifyItemNotExists(edge.getSourceKey());

    PostgresStatements postgresStatements;
    long uniqueId = idGenerator.generateItemId();

    Edge newEdge = new Edge(uniqueId, edge);
    try {
      postgresStatements = super.insert(newEdge);
      postgresStatements.append(String.format(SqlConstants.INSERT_EDGE, uniqueId, edge.getSourceKey(), edge.getFromNodeId(),
        edge.getToNodeId(), edge.getName()));

    } catch (Exception e) {
      throw new GroundException(e);
    }
    PostgresUtils.executeSqlList(dbSource, postgresStatements);
    return newEdge;
  }

  @Override
  protected Edge retrieve(String sql, Object field) throws GroundException {
    JsonNode json = Json.parse(PostgresUtils.executeQueryToJson(dbSource, sql));

    if (json.size() == 0) {
      throw new GroundException(ExceptionType.ITEM_NOT_FOUND, this.getType().getSimpleName(), field.toString());
    }

    Edge edge = Json.fromJson(json.get(0), Edge.class);
    long id = edge.getId();
    return new Edge(id, edge.getName(), edge.getSourceKey(), edge.getFromNodeId(), edge.getToNodeId(),
                     super.postgresTagDao.retrieveFromDatabaseByItemId(id));
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
