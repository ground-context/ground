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

package dao.versions.neo4j;

import com.google.common.base.CaseFormat;

import dao.versions.VersionHistoryDagFactory;
import db.DbDataContainer;
import db.Neo4jClient;
import exceptions.GroundException;
import models.models.Structure;
import models.versions.GroundType;
import models.versions.Item;
import models.versions.Version;
import models.versions.VersionHistoryDag;
import models.versions.VersionSuccessor;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.driver.v1.types.Relationship;

public class Neo4jVersionHistoryDagFactory implements VersionHistoryDagFactory {
  private final Neo4jClient dbClient;
  private final Neo4jVersionSuccessorFactory versionSuccessorFactory;

  public Neo4jVersionHistoryDagFactory(Neo4jClient dbClient,
                                       Neo4jVersionSuccessorFactory versionSuccessorFactory) {
    this.dbClient = dbClient;
    this.versionSuccessorFactory = versionSuccessorFactory;
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

    List<Relationship> result = this.dbClient.getDescendantEdgesByLabel(itemId, "VersionSuccessor");

    if (result.isEmpty()) {
      // do nothing' this just means that no versions have been added yet.
      return new VersionHistoryDag<>(itemId, new ArrayList<>());
    }

    List<VersionSuccessor<T>> edges = new ArrayList<>();

    for (Relationship relationship : result) {
      edges.add(this.versionSuccessorFactory.retrieveFromDatabase(relationship.get("id").asLong()));
    }

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
  public <T extends Version> void addEdge(VersionHistoryDag<T> dag, long parentId,
                                          long childId, long itemId)
      throws GroundException {

    VersionSuccessor<T> successor = this.versionSuccessorFactory.create(parentId, childId);

    dag.addEdge(parentId, childId, successor.getId());
  }

  /**
   * Truncate the DAG to only have a certain number of levels, removing everything before that.
   *
   * @param dag the DAG to truncate
   * @param numLevels the number of levels to keep
   */
  @Override
  public <T extends Version> void truncate(VersionHistoryDag<T> dag, int numLevels,
                                           Class<? extends Item> itemType)
      throws GroundException {

    Set<Long> level = new HashSet<>(dag.getLeaves());
    Set<Long> lastLevel = Collections.emptySet();
    for (int keptLevels = 0; keptLevels < numLevels; keptLevels++) {
      Set<Long> newLevel = level.stream()
          .flatMap(id -> dag.getParent(id).stream())
          .collect(Collectors.toSet());

      lastLevel = level;
      level = newLevel;
    }

    Queue<Long> deleteQueue = new ArrayDeque<>(level);
    Set<Long> deleted = new HashSet<>();

    for (long id : lastLevel) {
      this.addEdge(dag, dag.getItemId(), id, dag.getItemId());
    }

    List<DbDataContainer> predicates = new ArrayList<>();

    while (!deleteQueue.isEmpty()) {
      long id = deleteQueue.remove();

      String[] splits = itemType.getName().split("\\.");
      String className = splits[splits.length - 1];
      String tableNamePrefix = CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE,
          className);

      if (itemType.equals(Structure.class)) {
        predicates.add(new DbDataContainer("structure_version_id", GroundType.LONG, id));
        this.dbClient.deleteNode(predicates, "structure_version_attribute");
        predicates.clear();
      }

      if (itemType.getName().toLowerCase().contains("graph")) {
        predicates.add(new DbDataContainer(tableNamePrefix + "_version_id", GroundType.LONG, id));
        this.dbClient.deleteNode(predicates, tableNamePrefix + "_version_edge");
        predicates.clear();
      }

      predicates.add(new DbDataContainer("id", GroundType.LONG, id));

      this.dbClient.deleteNode(predicates, className + "Version");

      Collection<Long> parents = dag.getParent(id);

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
