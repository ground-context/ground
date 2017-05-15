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
import edu.berkeley.ground.common.utils.IdGenerator;
import edu.berkeley.ground.postgres.dao.version.ItemDao;
import edu.berkeley.ground.postgres.utils.PostgresStatements;
import edu.berkeley.ground.postgres.utils.PostgresUtils;
import play.db.Database;
import play.libs.Json;

import java.util.List;


// TODO construct me with dbSource and idGenerator thanks
public class EdgeDao extends ItemDao<Edge> implements EdgeFactory {

  public EdgeDao(Database dbSource, IdGenerator idGenerator) {
    super(dbSource, idGenerator);
  }

  public Edge create(Edge edge) throws GroundException {

    PostgresStatements postgresStatements = new PostgresStatements();
    long uniqueId = idGenerator.generateItemId();

    Edge newEdge = new Edge(uniqueId, edge.getName(), edge
      .getSourceKey(), edge.getFromNodeId(), edge.getToNodeId(), edge.getTags());
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

  @Override
  public Edge retrieveFromDatabase(String sourceKey) throws GroundException {
    String sql =
      String.format("select * from edge where source_key=\'%s\'", sourceKey);
    JsonNode json = Json.parse(PostgresUtils.executeQueryToJson(dbSource, sql));
    return Json.fromJson(json, Edge.class);
  }

  @Override
  public Edge retrieveFromDatabase(long id) throws GroundException {
    String sql =
      String.format("select * from edge where item_id=%d", id);
    JsonNode json = Json.parse(PostgresUtils.executeQueryToJson(dbSource, sql));
    return Json.fromJson(json, Edge.class);
  }

  @Override
  public List<Long> getLeaves(String sourceKey) throws GroundException {
    Edge edge  = retrieveFromDatabase(sourceKey);
    return super.getLeaves(edge.getId());
  }

  @Override
  public void truncate(long itemId, int numLevels) throws GroundException {
    super.truncate(itemId, numLevels);
  }

}
