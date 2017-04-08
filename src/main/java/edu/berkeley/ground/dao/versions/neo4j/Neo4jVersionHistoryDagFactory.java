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

package edu.berkeley.ground.dao.versions.neo4j;

import edu.berkeley.ground.dao.versions.VersionHistoryDagFactory;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.Neo4jClient;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.model.versions.GroundType;
import edu.berkeley.ground.model.versions.Version;
import edu.berkeley.ground.model.versions.VersionHistoryDag;
import edu.berkeley.ground.model.versions.VersionSuccessor;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.neo4j.driver.v1.types.Relationship;

public class
Neo4jVersionHistoryDagFactory extends VersionHistoryDagFactory {
  private final Neo4jClient dbClient;
  private final Neo4jVersionSuccessorFactory versionSuccessorFactory;

  public Neo4jVersionHistoryDagFactory(Neo4jClient dbClient,
                                       Neo4jVersionSuccessorFactory versionSuccessorFactory) {
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

    List<Relationship> result = this.dbClient.getDescendantEdgesByLabel(itemId, "VersionSuccessor");

    if (result.isEmpty()) {
      // do nothing' this just means that no versions have been added yet.
      return VersionHistoryDagFactory.construct(itemId, new ArrayList<VersionSuccessor<T>>());
    }

    List<VersionSuccessor<T>> edges = new ArrayList<>();

    for (Relationship relationship : result) {
      edges.add(this.versionSuccessorFactory.retrieveFromDatabase(relationship.get("id").asLong()));
    }

    return construct(itemId, edges);
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

    dag.addEdge(parentId, childId, successor.getId());
  }

  /**
   * Truncate the DAG to only have a certain number of levels, removing everything before that.
   *
   * @param dag the DAG to truncate
   * @param numLevels the number of levels to keep
   */
  @Override
  public void truncate(VersionHistoryDag dag, int numLevels, String itemType)
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

    List<Long> deleteQueue = previousLevel;
    Set<Long> deleted = new HashSet<>();

    for (long id : lastLevel) {
      this.addEdge(dag, dag.getItemId(), id, dag.getItemId());
    }

    List<DbDataContainer> predicates = new ArrayList<>();

    while (deleteQueue.size() > 0) {
      long id = deleteQueue.get(0);

      if (itemType.equals("structure")) {
        predicates.add(new DbDataContainer("structure_version_id", GroundType.LONG, id));
        this.dbClient.deleteNode(predicates, "structure_version_attribute");
        predicates.clear();
      }

      if (itemType.contains("graph")) {
        predicates.add(new DbDataContainer(itemType + "_version_id", GroundType.LONG, id));
        this.dbClient.deleteNode(predicates, itemType + "_version_edge");
        predicates.clear();
      }

      predicates.add(new DbDataContainer("id", GroundType.LONG, id));

      this.dbClient.deleteNode(predicates, itemType.substring(0, 1).toUpperCase() +
          itemType.substring(1) + "Version");

      deleteQueue.remove(0);
      List<Long> parents = dag.getParent(id);

      predicates.clear();
      predicates.add(new DbDataContainer("rich_version_id", GroundType.LONG, id));
      this.dbClient.deleteNode(predicates, "RichVersionTag");
      deleted.add(id);

      parents.forEach(parentId -> {
        if (!deleted.contains(parentId)) {
          deleteQueue.add(parentId);
        }
      });
    }
  }
}
