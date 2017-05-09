/**
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * <p>
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.berkeley.ground.postgres.dao;

import com.fasterxml.jackson.databind.JsonNode;
import edu.berkeley.ground.lib.exception.GroundException;
import edu.berkeley.ground.lib.factory.usage.LineageGraphFactory;
import edu.berkeley.ground.lib.model.usage.LineageGraph;
import edu.berkeley.ground.lib.model.version.Tag;
import edu.berkeley.ground.lib.utils.IdGenerator;
import edu.berkeley.ground.postgres.utils.PostgresUtils;
import play.db.Database;
import play.libs.Json;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LineageGraphDao extends ItemDao<LineageGraph> implements LineageGraphFactory {

  public LineageGraph createLineageGraph(final Database dbSource, final LineageGraph lineageGraph, final IdGenerator idGenerator) throws GroundException {
    long uniqueId = idGenerator.generateItemId();
    LineageGraph newLineageGraph = new LineageGraph(uniqueId, lineageGraph.getName(), lineageGraph.getSourceKey(), lineageGraph.getTags());
    return create(dbSource, lineageGraph);
  }

  @Override
  public LineageGraph create(Database dbSource, LineageGraph lineageGraph) throws GroundException {
    List<String> sqlList = new ArrayList<>();
    try {
      sqlList.addAll(super.createSqlList(lineageGraph));
      sqlList.add(String.format("insert into lineage_graph (item_id, source_key, name) values (%d, '%s', '%s')",
        lineageGraph.getId(), lineageGraph.getSourceKey(), lineageGraph.getName()));
      PostgresUtils.executeSqlList(dbSource, sqlList);
      return lineageGraph;
    } catch (Exception e) {
      throw new GroundException(e);
    }
  }

  @Override
  public LineageGraph retrieveFromDatabase(final Database dbSource, String sourceKey) throws GroundException {
    String sql = String.format("select * from lineage_graph where source_key = \'%s\'", sourceKey);
    JsonNode json = Json.parse(PostgresUtils.executeQueryToJson(dbSource, sql));
    return Json.fromJson(json, LineageGraph.class);
  }

  @Override
  public LineageGraph retrieveFromDatabase(final Database dbSource, long id) throws GroundException {
    String sql = String.format("select * from lineage_graph where id = %d", id);
    JsonNode json = Json.parse(PostgresUtils.executeQueryToJson(dbSource, sql));
    return Json.fromJson(json, LineageGraph.class);
  }

  @Override
  public void update(long itemId, long childId, List<Long> parentIds) throws GroundException {
    super.update(itemId, childId, parentIds);
  }

  @Override
  public List<Long> getLeaves(Database dbSource, String sourceKey) throws GroundException {
    LineageGraph lineageGraph  = retrieveFromDatabase(dbSource, sourceKey);
    return super.getLeaves(dbSource, lineageGraph.getId());
  }

  @Override
  public void truncate(long itemId, int numLevels) throws GroundException {
    super.truncate(itemId, numLevels);
  }
}
