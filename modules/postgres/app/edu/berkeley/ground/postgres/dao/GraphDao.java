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
package edu.berkeley.ground.postgres.dao;

import edu.berkeley.ground.lib.exception.GroundException;
import edu.berkeley.ground.lib.model.core.Graph;
import edu.berkeley.ground.lib.factory.core.GraphFactory;
import edu.berkeley.ground.postgres.dao.ItemDao;
import edu.berkeley.ground.postgres.utils.IdGenerator;
import edu.berkeley.ground.postgres.utils.PostgresUtils;
import edu.berkeley.ground.lib.model.version.GroundType;
import java.util.ArrayList;
import java.util.List;
import play.db.Database;
import play.libs.Json;
import com.fasterxml.jackson.databind.JsonNode;

public class GraphDao extends ItemDao<Graph> implements GraphFactory {

  @Override
  public void create(final Database dbSource, final Graph graph, final IdGenerator idGenerator) throws GroundException {
    final List<String> sqlList = new ArrayList<>();
    long uniqueId = idGenerator.generateItemId();
    Graph newGraph = new Graph(uniqueId, graph.getName(), graph.getSourceKey(), graph.getTags());
    try {
      sqlList.addAll(super.createSqlList(newGraph));
      sqlList.add(String.format("insert into graph (item_id, source_key, name) values (%d, '%s', '%s')",
          uniqueId, graph.getSourceKey(), graph.getName()));
      PostgresUtils.executeSqlList(dbSource, sqlList);
    } catch (Exception e) {
      throw new GroundException(e);
    }
  }

  @Override
  public Graph retrieveFromDatabase(final Database dbSource, String sourceKey) throws GroundException {
  	String sql = String.format("select * from graph where source_key = \'%s\'", sourceKey);
  	JsonNode json = Json.parse(PostgresUtils.executeQueryToJson(dbSource, sql));
  	return Json.fromJson(json, Graph.class);
  }

  @Override
  public Graph retrieveFromDatabase(final Database dbSource, long id) throws GroundException {
  	String sql = String.format("select * from graph_version where id = %d", id);
  	JsonNode json = Json.parse(PostgresUtils.executeQueryToJson(dbSource, sql));
  	return Json.fromJson(json, Graph.class);
  }

  @Override
  public void update(long itemId, long childId, List<Long> parentIds) throws GroundException {
    //TODO implement
  }

  @Override
  public List<Long> getLeaves(String sourceKey) throws GroundException {
  	return new ArrayList<>();
  }

  public void truncate(long itemId, int numLevels) throws GroundException {
    //TODO implement
  }

}
