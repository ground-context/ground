/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.berkeley.ground.postgres.dao.version;

import com.google.common.base.CaseFormat;
import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.factory.version.VersionHistoryDagFactory;
import edu.berkeley.ground.common.model.core.Structure;
import edu.berkeley.ground.common.model.version.*;
import edu.berkeley.ground.postgres.utils.PostgresStatements;
import play.db.Database;


import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class VersionHistoryDagDao implements VersionHistoryDagFactory {
  private VersionSuccessorDao versionSuccessorDao;
  private Database dbSource;

  public VersionHistoryDagDao(Database dbSource, VersionSuccessorDao versionSuccessorDao) {
    this.versionSuccessorDao = versionSuccessorDao;
    this.dbSource = dbSource;
  }

  @Override
  public <T extends Version> VersionHistoryDag<T> create(long itemId) throws GroundException {
    return new VersionHistoryDag<>(itemId, new ArrayList<>());
  }

  public PostgresStatements insert(VersionHistoryDag dag, long parentId, long childId, long itemId)
    throws GroundException {

    long versionSuccessorId = this.versionSuccessorDao.getNewSuccessorId();
    PostgresStatements statements = versionSuccessorDao.insert(parentId, childId, versionSuccessorId);
    String sql = String.format("insert into version_history_dag (item_id, version_successor_id) " +
      "values(%d, %d)", itemId, versionSuccessorId);
    statements.append(sql);
    return statements;
  }

  /**
   * Retrieve a DAG from the database.
   *
   * @param itemId the id of the item whose dag we are retrieving
   * @param <T> the type of the versions in this dag
   * @return the retrieved DAG
   * @throws GroundException an error retrieving the DAG
   */
  @Override
  public <T extends Version> VersionHistoryDag<T> retrieveFromDatabase(long itemId)
    throws GroundException {

    String sql =
      String.format("select * from version_history_dag where item_id=%d", itemId);

    List<VersionSuccessor<T>> edges = new ArrayList<>();
    try (Connection con = dbSource.getConnection()){

      Statement stmt = con.createStatement();
      final ResultSet resultSet = stmt.executeQuery(sql);

      while (resultSet.next()) {
        long versionSuccessorId = resultSet.getLong("version_successor_id");
        VersionSuccessor<T> versionSuccessor = versionSuccessorDao.retrieveFromDatabase(versionSuccessorId);
        edges.add(versionSuccessor);
      }
      stmt.close();

    } catch (Exception e) {
      throw new GroundException(e);
    }
    return new VersionHistoryDag<T>(itemId, edges);
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
  public void addEdge(VersionHistoryDag dag, long parentId, long childId, long itemId)
    throws GroundException {

    VersionSuccessor successor = this.versionSuccessorDao.create(parentId, childId);
    dag.addEdge(parentId, childId, successor.getId());
    //TODO: Refactor to use SQL statement -- Shouldn't rely on dbClient
    /*
    List<DbDataContainer> insertions = new ArrayList<>();
    insertions.add(new DbDataContainer("item_id", GroundType.LONG, itemId));
    insertions.add(new DbDataContainer("version_successor_id", GroundType.LONG, successor.getId()));

    postgresClient.insert("version_history_dag", insertions);
    */

  }


  /**
   * Truncate the DAG to only have a certain number of levels, removing everything before that.
   *
   * TODO: refactor to move specific logic into those classes.
   *
   * @param dag the DAG to truncate
   * @param numLevels the number of levels to keep
   */
  @Override
  public void truncate(VersionHistoryDag dag, int numLevels, Class<? extends Item> itemType)
    throws GroundException {


    int keptLevels = 1;
    List<Long> lastLevel = new ArrayList<>();
    List<Long> previousLevel = dag.getLeaves();

    while (keptLevels <= numLevels) {
      List<Long> currentLevel = new ArrayList<>();

      previousLevel.forEach(id ->
        currentLevel.addAll(dag.getParent(id))
      );

      lastLevel = previousLevel;
      previousLevel = currentLevel;

      keptLevels++;
    }

    List<Long> deleteQueue = new ArrayList<>(new HashSet<>(previousLevel));
    Set<Long> deleted = new HashSet<>();

    List<String> sqlList = new ArrayList<>();

    // delete the version successors between the last kept level and the first deleted level
    for (long id : lastLevel) {
      // TODO: Change to take in a sql list
      this.versionSuccessorDao.deleteFromDestination(sqlList, id, dag.getItemId());
      this.addEdge(dag, 0, id, dag.getItemId());
    }

    while (deleteQueue.size() > 0) {
      long id = deleteQueue.get(0);

      if (id != 0) {
        String[] splits = itemType.getName().split("\\.");
        String tableNamePrefix = splits[splits.length - 1];
        tableNamePrefix = CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, tableNamePrefix);

        if (itemType.equals(Structure.class)) {
          sqlList.add(String.format("DELETE * FROM structure_version_attribute WHERE structure_version_id = %d;", id));
        }

        if (itemType.getName().toLowerCase().contains("graph")) {
          String tableName = tableNamePrefix + "_version_edge";

          sqlList.add(String.format("DELETE * FROM %s WHERE %s_version_id = %d;", tableName,
            tableNamePrefix, id));
        }

        sqlList.add(String.format("DELETE * FROM %s_version where id = %d;", tableNamePrefix, id));

        if (!itemType.equals(Structure.class)) {
          sqlList.add(String.format("DELETE * FROM rich_version where id = %d;", id));
          sqlList.add(String.format("DELETE * FROM rich_version_tag where rich_version_id = %d;", id));
        }

        // TODO: change to take SQL list
        this.versionSuccessorDao.deleteFromDestination(sqlList, id, dag.getItemId());

        sqlList.add(String.format("DELETE * FROM version WHERE id = %d;", id));

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

  }
}
