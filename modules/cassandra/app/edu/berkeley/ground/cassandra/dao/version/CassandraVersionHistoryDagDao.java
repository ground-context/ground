/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.berkeley.ground.cassandra.dao.version;

import edu.berkeley.ground.common.dao.version.VersionHistoryDagDao;
import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.model.version.Item;
import edu.berkeley.ground.common.model.version.VersionHistoryDag;
import edu.berkeley.ground.common.model.version.VersionSuccessor;
import edu.berkeley.ground.common.util.IdGenerator;
import edu.berkeley.ground.cassandra.dao.CqlConstants;
import edu.berkeley.ground.cassandra.util.GroundUtils;
import edu.berkeley.ground.cassandra.util.CassandraDatabase;
import edu.berkeley.ground.cassandra.util.CassandraStatements;
import edu.berkeley.ground.cassandra.util.CassandraUtils;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class CassandraVersionHistoryDagDao implements VersionHistoryDagDao {

  private CassandraVersionSuccessorDao cassandraVersionSuccessorDao;
  private CassandraDatabase dbSource;
  private IdGenerator idGenerator;

  public CassandraVersionHistoryDagDao(CassandraDatabase dbSource, IdGenerator idGenerator) {
    this.dbSource = dbSource;
    this.idGenerator = idGenerator;

    this.cassandraVersionSuccessorDao = new CassandraVersionSuccessorDao(this.dbSource, this.idGenerator);
  }

  @Override
  public VersionHistoryDag create(long itemId) throws GroundException {
    return new VersionHistoryDag(itemId, new ArrayList<>());
  }

  /**
   * Retrieve a DAG from the database.
   *
   * @param itemId the id of the item whose dag we are retrieving
   * @return the retrieved DAG
   * @throws GroundException an error retrieving the DAG
   */
  @Override
  public VersionHistoryDag retrieveFromDatabase(long itemId) throws GroundException {
    String cql = String.format(CqlConstants.SELECT_VERSION_HISTORY_DAG, itemId);

    List<VersionSuccessor> edges = new ArrayList<>();
    
    Session session = this.dbSource.getSession();

    final ResultSet resultSet = session.execute(cql);
    List<Long> successors = new ArrayList<>();
    for (Row row: resultSet.all()) {
      successors.add(row.getLong("version_successor_id"));
    }

    for (Long versionSuccessorId : successors) {
      VersionSuccessor versionSuccessor = cassandraVersionSuccessorDao.retrieveFromDatabase(versionSuccessorId);
      edges.add(versionSuccessor);
    }

    return new VersionHistoryDag(itemId, edges);
  }

  /**
   * Add an edge to the DAG.
   *
   * @param dag the DAG to update
   * @param parentId the parent's id
   * @param childId the child's id
   * @param itemId the id of the Item whose DAG we're updating
   * @throws GroundException an error adding the edge
   */
  @Override
  public CassandraStatements addEdge(VersionHistoryDag dag, long parentId, long childId, long itemId) throws GroundException {
    VersionSuccessor successor = this.cassandraVersionSuccessorDao.instantiateVersionSuccessor(parentId, childId);
    dag.addEdge(parentId, childId, successor.getId());

    CassandraStatements statements = cassandraVersionSuccessorDao.insert(successor);
    statements.append(String.format(CqlConstants.INSERT_VERSION_HISTORY_DAG_EDGE, itemId, successor.getId()));
    return statements;
  }


  /**
   * Truncate the DAG to only have a certain number of levels, removing everything before that.
   *
   * @param dag the DAG to truncate
   * @param numLevels the number of levels to keep
   */
  @Override
  public void truncate(VersionHistoryDag dag, int numLevels, Class<? extends Item> itemType) throws GroundException {

    int keptLevels = 1;
    List<Long> lastLevel = new ArrayList<>();
    List<Long> previousLevel = dag.getLeaves();

    while (keptLevels <= numLevels) {
      List<Long> currentLevel = new ArrayList<>();

      previousLevel.forEach(id -> currentLevel.addAll(dag.getParent(id)));

      lastLevel = previousLevel;
      previousLevel = currentLevel;

      keptLevels++;
    }

    List<Long> deleteQueue = new ArrayList<>(new HashSet<>(previousLevel));
    Set<Long> deleted = new HashSet<>();

    CassandraStatements statements = new CassandraStatements();

    // delete the version successors between the last kept level and the first deleted level
    for (long id : lastLevel) {
      this.cassandraVersionSuccessorDao.deleteFromDestination(statements, id, dag.getItemId());
      this.addEdge(dag, 0, id, dag.getItemId());
    }

    while (deleteQueue.size() > 0) {
      long id = deleteQueue.get(0);

      if (id != 0) {
        this.cassandraVersionSuccessorDao.deleteFromDestination(statements, id, dag.getItemId());
        GroundUtils.getVersionDaoFromItemType(itemType, this.dbSource, this.idGenerator).delete(id);

        deleted.add(id);
        List<Long> parents = dag.getParent(id);

        parents.forEach(parentId -> {
          if (!deleted.contains(parentId)) {
            deleteQueue.add(parentId);
          }
        });
      }

      deleteQueue.remove(0);
    }

    for (long id : lastLevel) {
      statements.merge(this.addEdge(dag, 0, id, dag.getItemId()));
    }

    CassandraUtils.executeCqlList(dbSource, statements);
  }
}
