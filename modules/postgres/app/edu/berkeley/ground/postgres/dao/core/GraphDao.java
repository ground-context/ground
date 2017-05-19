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
import edu.berkeley.ground.common.factory.core.GraphFactory;
import edu.berkeley.ground.common.model.core.Graph;
import edu.berkeley.ground.common.utils.IdGenerator;
import edu.berkeley.ground.postgres.dao.version.ItemDao;
import edu.berkeley.ground.postgres.utils.PostgresStatements;
import edu.berkeley.ground.postgres.utils.PostgresUtils;
import play.db.Database;
import play.libs.Json;

import java.util.List;


// TODO construct me with dbSource and idGenerator thanks
public class GraphDao extends ItemDao<Graph> implements GraphFactory {

  public GraphDao(Database dbSource, IdGenerator idGenerator) {
    super(dbSource, idGenerator);
  }

  @Override
  public Class<Graph> getType() {
    return Graph.class;
  }

  public Graph create(Graph graph) throws GroundException {

    PostgresStatements postgresStatements;
    long uniqueId = idGenerator.generateItemId();

    Graph newGraph = new Graph(uniqueId, graph.getName(), graph
      .getSourceKey(), graph.getTags());
    try {
      postgresStatements = super.insert(newGraph);
      postgresStatements.append(String.format(
        "insert into graph (item_id, source_key, name) values (%s,\'%s\',\'%s\')",
        uniqueId, graph.getSourceKey(), graph.getName()));
    } catch (Exception e) {
      throw new GroundException(e);
    }
    PostgresUtils.executeSqlList(dbSource, postgresStatements);
    return newGraph;
  }

  @Override
  public Graph retrieveFromDatabase(String sourceKey) throws GroundException {
    String sql =
      String.format("select * from graph where source_key=\'%s\'", sourceKey);
    JsonNode json = Json.parse(PostgresUtils.executeQueryToJson(dbSource, sql));
    if (json.size() == 0) {
      throw new GroundException(String.format("Graph with source_key %s does not exist.", sourceKey));
    }
    return Json.fromJson(json.get(0), Graph.class);
  }

  @Override
  public Graph retrieveFromDatabase(long id) throws GroundException {
    String sql =
      String.format("select * from graph where item_id=%d", id);
    JsonNode json = Json.parse(PostgresUtils.executeQueryToJson(dbSource, sql));
    if (json.size() == 0) {
      throw new GroundException(String.format("Graph with id %d does not exist.", id));
    }
    return Json.fromJson(json.get(0), Graph.class);
  }

  @Override
  public List<Long> getLeaves(String sourceKey) throws GroundException {
    Graph graph  = retrieveFromDatabase(sourceKey);
    return super.getLeaves(graph.getId());
  }

  @Override
  public void truncate(long itemId, int numLevels) throws GroundException {
    super.truncate(itemId, numLevels);
  }
}
