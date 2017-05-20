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
import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.factory.core.EdgeFactory;
import edu.berkeley.ground.common.model.core.Edge;
import edu.berkeley.ground.common.util.IdGenerator;
import edu.berkeley.ground.postgres.dao.version.ItemDao;
import edu.berkeley.ground.postgres.util.PostgresStatements;
import edu.berkeley.ground.postgres.util.PostgresUtils;
import java.util.List;
import play.db.Database;
import play.libs.Json;


public class EdgeDao extends ItemDao<Edge> implements EdgeFactory {

  public EdgeDao(Database dbSource, IdGenerator idGenerator) {
    super(dbSource, idGenerator);
  }

  @Override
  public Class<Edge> getType() {
    return Edge.class;
  }

  @Override
  public Edge create(Edge edge) throws GroundException {

    PostgresStatements postgresStatements;
    long uniqueId = idGenerator.generateItemId();

    Edge newEdge = new Edge(uniqueId, edge);
    try {
      postgresStatements = super.insert(newEdge);
      postgresStatements.append(String.format(
        "insert into edge (item_id, source_key, from_node_id, to_node_id, name) values (%d,\'%s\',%d,%d,\'%s\')",
        uniqueId, edge.getSourceKey(), edge.getFromNodeId(), edge.getToNodeId(), edge.getName()));

    } catch (Exception e) {
      throw new GroundException(e);
    }
    PostgresUtils.executeSqlList(dbSource, postgresStatements);
    return newEdge;
  }

  // TODO: Retrieve tags...
  @Override
  public Edge retrieveFromDatabase(String sourceKey) throws GroundException {
    String sql =
      String.format("select * from edge where source_key=\'%s\'", sourceKey);
    JsonNode json = Json.parse(PostgresUtils.executeQueryToJson(dbSource, sql));
    if (json.size() == 0) {
      throw new GroundException(String.format("Edge with source_key %s does not exist.", sourceKey));
    }
    return Json.fromJson(json.get(0), Edge.class);
  }

  @Override
  public Edge retrieveFromDatabase(long id) throws GroundException {
    String sql =
      String.format("select * from edge where item_id=%d", id);
    JsonNode json = Json.parse(PostgresUtils.executeQueryToJson(dbSource, sql));
    if (json.size() == 0) {
      throw new GroundException(String.format("Edge with id %d does not exist.", id));
    }
    return Json.fromJson(json.get(0), Edge.class);
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
