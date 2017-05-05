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
package edu.berkeley.ground.postgres.dao;


import com.google.common.base.CaseFormat;
import edu.berkeley.ground.lib.exception.GroundException;
import edu.berkeley.ground.lib.factory.version.VersionHistoryDagFactory;
import edu.berkeley.ground.lib.model.core.Structure;
import edu.berkeley.ground.lib.model.version.*;
import edu.berkeley.ground.postgres.utils.DbClient;
import edu.berkeley.ground.postgres.utils.DbDataContainer;
import edu.berkeley.ground.postgres.utils.PostgresResults;
import edu.berkeley.ground.postgres.utils.PostgresClient;


import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class VersionHistoryDagDao implements VersionHistoryDagFactory {
  private VersionSuccessorDao versionSuccessorDao;
  private PostgresClient postgresClient;

  public VersionHistoryDagDao(PostgresClient postgresClient, VersionSuccessorDao versionSuccessorDao) {
    this.versionSuccessorDao = versionSuccessorDao;
    this.postgresClient = postgresClient;
  }

  @Override
  public <T extends Version> VersionHistoryDag<T> create(long itemId) throws GroundException {
    return new VersionHistoryDag<>(itemId, new ArrayList<>());
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

    List<DbDataContainer> predicates = new ArrayList<>();
    predicates.add(new DbDataContainer("item_id", GroundType.LONG, itemId));

    PostgresResults resultSet = postgresClient.equalitySelect("version_history_dag",
      DbClient.SELECT_STAR,
      predicates);
    if (resultSet.isEmpty()) {
      // do nothing' this just means that no versions have been added yet.
      return new VersionHistoryDag<T>(itemId, new ArrayList<>());
    }

    List<VersionSuccessor<T>> edges = new ArrayList<>();
    do {
      edges.add(this.versionSuccessorDao.retrieveFromDatabase(resultSet.getLong(2)));
    } while (resultSet.next());

    return new VersionHistoryDag<>(itemId, edges);
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

    List<DbDataContainer> insertions = new ArrayList<>();
    insertions.add(new DbDataContainer("item_id", GroundType.LONG, itemId));
    insertions.add(new DbDataContainer("version_successor_id", GroundType.LONG, successor.getId()));

    postgresClient.insert("version_history_dag", insertions);

    dag.addEdge(parentId, childId, successor.getId());
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

    List<DbDataContainer> predicates = new ArrayList<>();
    for (long id : lastLevel) {
      this.versionSuccessorDao.deleteFromDestination(id, dag.getItemId());
      this.addEdge(dag, 0, id, dag.getItemId());
    }

    while (deleteQueue.size() > 0) {
      long id = deleteQueue.get(0);

      if (id != 0) {
        String[] splits = itemType.getName().split("\\.");
        String tableNamePrefix = splits[splits.length - 1];
        tableNamePrefix = CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, tableNamePrefix);

        if (itemType.equals(Structure.class)) {
          predicates.add(new DbDataContainer("structure_version_id", GroundType.LONG, id));
          postgresClient.delete(predicates, "structure_version_attribute");
          predicates.clear();
        }

        if (itemType.getName().toLowerCase().contains("graph")) {
          predicates.add(new DbDataContainer(tableNamePrefix + "_version_id", GroundType.LONG, id));
          postgresClient.delete(predicates, tableNamePrefix + "_version_edge");
          predicates.clear();
        }

        predicates.add(new DbDataContainer("id", GroundType.LONG, id));

        postgresClient.delete(predicates, tableNamePrefix + "_version");

        if (!itemType.equals(Structure.class)) {
          postgresClient.delete(predicates, "rich_version");
        }

        this.versionSuccessorDao.deleteFromDestination(id, dag.getItemId());

        predicates.clear();
        predicates.add(new DbDataContainer("rich_version_id", GroundType.LONG, id));
        postgresClient.delete(predicates, "rich_version_tag");

        predicates.clear();
        predicates.add(new DbDataContainer("id", GroundType.LONG, id));
        postgresClient.delete(predicates, "version");

        deleted.add(id);

        List<Long> parents = dag.getParent(id);

        parents.forEach(parentId -> {
          if (!deleted.contains(parentId)) {
            deleteQueue.add(parentId);
          }
        });
        predicates.clear();
      }

      deleteQueue.remove(0);
    }
  }
}
