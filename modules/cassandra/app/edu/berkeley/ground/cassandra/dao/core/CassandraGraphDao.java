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

import edu.berkeley.ground.common.dao.core.GraphDao;
import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.model.core.Graph;
import edu.berkeley.ground.common.util.IdGenerator;
import edu.berkeley.ground.cassandra.dao.CqlConstants;
import edu.berkeley.ground.cassandra.dao.version.CassandraItemDao;
import edu.berkeley.ground.cassandra.util.CassandraDatabase;
import edu.berkeley.ground.cassandra.util.CassandraStatements;
import edu.berkeley.ground.cassandra.util.CassandraUtils;
import java.util.List;
import java.util.Map;


public class CassandraGraphDao extends CassandraItemDao<Graph> implements GraphDao {

  public CassandraGraphDao(CassandraDatabase dbSource, IdGenerator idGenerator) {
    super(dbSource, idGenerator);
  }

  @Override
  public Class<Graph> getType() {
    return Graph.class;
  }

  @Override
  public Graph create(Graph graph) throws GroundException {
    super.verifyItemNotExists(graph.getSourceKey());

    CassandraStatements statements;
    long uniqueId = idGenerator.generateItemId();
    Graph newGraph = new Graph(uniqueId, graph);

    try {
      statements = super.insert(newGraph);

      String name = graph.getName();
      if (name != null) {
        statements.append(String.format(CqlConstants.INSERT_GENERIC_ITEM_WITH_NAME, "graph", uniqueId, graph.getSourceKey(), name));
      } else {
        statements.append(String.format(CqlConstants.INSERT_GENERIC_ITEM_WITHOUT_NAME, "graph", uniqueId, graph.getSourceKey()));
      }
    } catch (Exception e) {
      throw new GroundException(e);
    }

    CassandraUtils.executeCqlList(dbSource, statements);
    return newGraph;
  }

  @Override
  public List<Long> getLeaves(String sourceKey) throws GroundException {
    Graph graph = retrieveFromDatabase(sourceKey);
    return super.getLeaves(graph.getId());
  }

  @Override
  public Map<Long, Long> getHistory(String sourceKey) throws GroundException {
    Graph graph = retrieveFromDatabase(sourceKey);
    return super.getHistory(graph.getId());
  }

  @Override
  public void truncate(long itemId, int numLevels) throws GroundException {
    super.truncate(itemId, numLevels);
  }
}
