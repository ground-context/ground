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

package edu.berkeley.ground.dao.versions.postgres;

import edu.berkeley.ground.dao.versions.VersionHistoryDagFactory;
import edu.berkeley.ground.db.DbClient;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.PostgresClient;
import edu.berkeley.ground.db.PostgresResults;
import edu.berkeley.ground.exceptions.EmptyResultException;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.model.versions.GroundType;
import edu.berkeley.ground.model.versions.Version;
import edu.berkeley.ground.model.versions.VersionHistoryDag;
import edu.berkeley.ground.model.versions.VersionSuccessor;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class PostgresVersionHistoryDagFactory extends VersionHistoryDagFactory {
  private final PostgresClient dbClient;
  private final PostgresVersionSuccessorFactory versionSuccessorFactory;

  public PostgresVersionHistoryDagFactory(PostgresClient dbClient,
                                          PostgresVersionSuccessorFactory versionSuccessorFactory) {
    this.dbClient = dbClient;
    this.versionSuccessorFactory = versionSuccessorFactory;
  }

  @Override
  public <T extends Version> VersionHistoryDag<T> create(long itemId) throws GroundException {
    return construct(itemId);
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

    PostgresResults resultSet;
    try {
      resultSet = this.dbClient.equalitySelect("version_history_dag", DbClient.SELECT_STAR,
          predicates);
    } catch (EmptyResultException e) {
      // do nothing' this just means that no versions have been added yet.
      return VersionHistoryDagFactory.construct(itemId, new ArrayList<VersionSuccessor<T>>());
    }

    List<VersionSuccessor<T>> edges = new ArrayList<>();
    do {
      edges.add(this.versionSuccessorFactory.retrieveFromDatabase(resultSet.getLong(2)));
    } while (resultSet.next());

    return VersionHistoryDagFactory.construct(itemId, edges);
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

    VersionSuccessor successor = this.versionSuccessorFactory.create(parentId, childId);

    List<DbDataContainer> insertions = new ArrayList<>();
    insertions.add(new DbDataContainer("item_id", GroundType.LONG, itemId));
    insertions.add(new DbDataContainer("version_successor_id", GroundType.LONG, successor.getId()));

    this.dbClient.insert("version_history_dag", insertions);

    dag.addEdge(parentId, childId, successor.getId());
  }

  /**
   * Truncate the DAG to only have a certain number of levels, removing everything before that.
   *
   * @param dag the DAG to truncate
   * @param numLevels the number of levels to keep
   */
  @Override
  public void truncate(VersionHistoryDag dag, int numLevels, String itemType) throws
      GroundException {

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

    List<Long> deleteQueue = previousLevel;
    Set<Long> deleted = new HashSet<>();

    List<DbDataContainer> predicates = new ArrayList<>();
    for (long id : lastLevel) {
      this.versionSuccessorFactory.deleteFromDestination(id, dag.getItemId());
      this.addEdge(dag, 0, id, dag.getItemId());
    }

    while (deleteQueue.size() > 0) {
      long id = deleteQueue.get(0);

      if (id != 0) {
        predicates.add(new DbDataContainer("id", GroundType.LONG, id));

        this.dbClient.delete(predicates, itemType + "_version");

        if (!itemType.equals("structure")) {
          this.dbClient.delete(predicates, "rich_version");
        }

        this.versionSuccessorFactory.deleteFromDestination(id, dag.getItemId());

        predicates.clear();
        predicates.add(new DbDataContainer("rich_version_id", GroundType.LONG, id));
        this.dbClient.delete(predicates, "rich_version_tag");

        predicates.clear();
        predicates.add(new DbDataContainer("id", GroundType.LONG, id));
        this.dbClient.delete(predicates, "version");

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
