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
package edu.berkeley.ground.postgres.dao.version;

import edu.berkeley.ground.common.dao.version.VersionHistoryDagDao;
import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.model.version.Item;
import edu.berkeley.ground.common.model.version.VersionHistoryDag;
import edu.berkeley.ground.common.model.version.VersionSuccessor;
import edu.berkeley.ground.common.util.IdGenerator;
import edu.berkeley.ground.postgres.dao.SqlConstants;
import edu.berkeley.ground.postgres.util.GroundUtils;
import edu.berkeley.ground.postgres.util.PostgresStatements;
import edu.berkeley.ground.postgres.util.PostgresUtils;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import play.db.Database;

public class PostgresVersionHistoryDagDao implements VersionHistoryDagDao {

  private PostgresVersionSuccessorDao postgresVersionSuccessorDao;
  private Database dbSource;
  private IdGenerator idGenerator;

  public PostgresVersionHistoryDagDao(Database dbSource, IdGenerator idGenerator) {
    this.dbSource = dbSource;
    this.idGenerator = idGenerator;

    this.postgresVersionSuccessorDao = new PostgresVersionSuccessorDao(this.dbSource, this.idGenerator);
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
    String sql = String.format(SqlConstants.SELECT_VERSION_HISTORY_DAG, itemId);

    List<VersionSuccessor> edges = new ArrayList<>();
    try (Connection con = dbSource.getConnection()) {

      Statement stmt = con.createStatement();
      final ResultSet resultSet = stmt.executeQuery(sql);

      List<Long> successors = new ArrayList<>();
      while (resultSet.next()) {
        successors.add(resultSet.getLong("version_successor_id"));
      }

      stmt.close();
      con.close();

      for (Long versionSuccessorId : successors) {
        VersionSuccessor versionSuccessor = postgresVersionSuccessorDao.retrieveFromDatabase(versionSuccessorId);
        edges.add(versionSuccessor);
      }
    } catch (Exception e) {
      throw new GroundException(e);
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
  public PostgresStatements addEdge(VersionHistoryDag dag, long parentId, long childId, long itemId) throws GroundException {
    VersionSuccessor successor = this.postgresVersionSuccessorDao.instantiateVersionSuccessor(parentId, childId);
    dag.addEdge(parentId, childId, successor.getId());

    PostgresStatements statements = postgresVersionSuccessorDao.insert(successor);
    statements.append(String.format(SqlConstants.INSERT_VERSION_HISTORY_DAG_EDGE, itemId, successor.getId()));
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

    PostgresStatements statements = new PostgresStatements();

    // delete the version successors between the last kept level and the first deleted level
    for (long id : lastLevel) {
      this.postgresVersionSuccessorDao.deleteFromDestination(statements, id, dag.getItemId());
      this.addEdge(dag, 0, id, dag.getItemId());
    }

    while (deleteQueue.size() > 0) {
      long id = deleteQueue.get(0);

      if (id != 0) {
        this.postgresVersionSuccessorDao.deleteFromDestination(statements, id, dag.getItemId());
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

    PostgresUtils.executeSqlList(dbSource, statements);
  }
}
