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
package edu.berkeley.ground.postgres.dao.usage;

import com.fasterxml.jackson.databind.JsonNode;
import edu.berkeley.ground.common.dao.usage.LineageGraphDao;
import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.model.usage.LineageGraph;
import edu.berkeley.ground.common.util.IdGenerator;
import edu.berkeley.ground.postgres.dao.version.PostgresItemDao;
import edu.berkeley.ground.postgres.util.PostgresStatements;
import edu.berkeley.ground.postgres.util.PostgresUtils;
import java.util.List;
import play.db.Database;
import play.libs.Json;

public class PostgresLineageGraphDao extends PostgresItemDao<LineageGraph> implements LineageGraphDao {

  public PostgresLineageGraphDao(Database dbSource, IdGenerator idGenerator) {
    super(dbSource, idGenerator);
  }

  @Override
  public Class<LineageGraph> getType() {
    return LineageGraph.class;
  }

  @Override
  public LineageGraph create(LineageGraph lineageGraph) throws GroundException {
    long uniqueId = idGenerator.generateItemId();
    LineageGraph newLineageGraph = new LineageGraph(uniqueId, lineageGraph);

    PostgresStatements statements = super.insert(newLineageGraph);

    statements.append(String.format("insert into lineage_graph (item_id, source_key, name) values (%d, '%s', '%s')", newLineageGraph.getId(),
      newLineageGraph.getSourceKey(), newLineageGraph.getName()));

    try {
      PostgresUtils.executeSqlList(dbSource, statements);
      return newLineageGraph;
    } catch (Exception e) {
      throw new GroundException(e);
    }
  }

  @Override
  public LineageGraph retrieveFromDatabase(String sourceKey) throws GroundException {
    String sql = String.format("select * from lineage_graph where source_key = \'%s\'", sourceKey);
    JsonNode json = Json.parse(PostgresUtils.executeQueryToJson(dbSource, sql));

    if (json.size() == 0) {
      throw new GroundException(String.format("Lineage Graph with sourceKey %s does not exist.", sourceKey));
    }

    return Json.fromJson(json.get(0), LineageGraph.class);
  }

  @Override
  public LineageGraph retrieveFromDatabase(long id) throws GroundException {
    String sql = String.format("select * from lineage_graph where id = %d", id);
    JsonNode json = Json.parse(PostgresUtils.executeQueryToJson(dbSource, sql));

    if (json.size() == 0) {
      throw new GroundException(String.format("Lineage Graph with id %d does not exist.", id));
    }

    return Json.fromJson(json.get(0), LineageGraph.class);
  }

  @Override
  public List<Long> getLeaves(String sourceKey) throws GroundException {
    LineageGraph lineageGraph = retrieveFromDatabase(sourceKey);
    return super.getLeaves(lineageGraph.getId());
  }

  @Override
  public void truncate(long itemId, int numLevels) throws GroundException {
    super.truncate(itemId, numLevels);
  }
}
