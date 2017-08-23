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

import edu.berkeley.ground.common.dao.usage.LineageGraphDao;
import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.model.usage.LineageGraph;
import edu.berkeley.ground.common.util.IdGenerator;
import edu.berkeley.ground.postgres.dao.SqlConstants;
import edu.berkeley.ground.postgres.dao.version.PostgresItemDao;
import edu.berkeley.ground.postgres.util.PostgresStatements;
import edu.berkeley.ground.postgres.util.PostgresUtils;
import java.util.List;
import java.util.Map;
import play.db.Database;

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
    super.verifyItemNotExists(lineageGraph.getSourceKey());

    long uniqueId = idGenerator.generateItemId();
    LineageGraph newLineageGraph = new LineageGraph(uniqueId, lineageGraph);

    PostgresStatements statements = super.insert(newLineageGraph);

    String name = lineageGraph.getName();
    if (name != null) {
      statements.append(String.format(SqlConstants.INSERT_GENERIC_ITEM_WITH_NAME, "lineage_graph", newLineageGraph.getId(),
        newLineageGraph.getSourceKey(), name));
    } else {
      statements.append(String.format(SqlConstants.INSERT_GENERIC_ITEM_WITHOUT_NAME, "lineage_graph", newLineageGraph.getId(),
        newLineageGraph.getSourceKey()));
    }

    try {
      PostgresUtils.executeSqlList(dbSource, statements);
      return newLineageGraph;
    } catch (Exception e) {
      throw new GroundException(e);
    }
  }

  @Override
  public List<Long> getLeaves(String sourceKey) throws GroundException {
    LineageGraph lineageGraph = retrieveFromDatabase(sourceKey);
    return super.getLeaves(lineageGraph.getId());
  }

  @Override
  public Map<Long, Long> getHistory(String sourceKey) throws GroundException {
    LineageGraph lineageGraph = retrieveFromDatabase(sourceKey);
    return super.getHistory(lineageGraph.getId());
  }

  @Override
  public void truncate(long itemId, int numLevels) throws GroundException {
    super.truncate(itemId, numLevels);
  }
}
