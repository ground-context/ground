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

package edu.berkeley.ground.api.versions.neo4j;

import edu.berkeley.ground.api.versions.Version;
import edu.berkeley.ground.api.versions.VersionHistoryDAG;
import edu.berkeley.ground.api.versions.VersionHistoryDAGFactory;
import edu.berkeley.ground.api.versions.VersionSuccessor;
import edu.berkeley.ground.db.Neo4jClient;
import edu.berkeley.ground.exceptions.GroundDBException;
import edu.berkeley.ground.exceptions.GroundException;

import org.neo4j.driver.internal.value.StringValue;
import org.neo4j.driver.v1.types.Relationship;

import java.util.ArrayList;
import java.util.List;

public class Neo4jVersionHistoryDAGFactory extends VersionHistoryDAGFactory {
  private final Neo4jClient dbClient;
  private final Neo4jVersionSuccessorFactory versionSuccessorFactory;

  public Neo4jVersionHistoryDAGFactory(Neo4jClient dbClient,
                                       Neo4jVersionSuccessorFactory versionSuccessorFactory) {
    this.dbClient = dbClient;
    this.versionSuccessorFactory = versionSuccessorFactory;
  }

  public <T extends Version> VersionHistoryDAG<T> create(long itemId) throws GroundException {
    return construct(itemId);
  }

  public <T extends Version> VersionHistoryDAG<T> retrieveFromDatabase(long itemId) throws GroundException {
    List<Relationship> result = this.dbClient.getDescendantEdgesByLabel(itemId, "VersionSuccessor");

    if (result.isEmpty()) {
      // do nothing' this just means that no versions have been added yet.
      return VersionHistoryDAGFactory.construct(itemId, new ArrayList<VersionSuccessor<T>>());
    }

    List<VersionSuccessor<T>> edges = new ArrayList<>();

    for (Relationship relationship : result) {
      edges.add(this.versionSuccessorFactory.retrieveFromDatabase(relationship.get("id").asLong()));
    }

    return construct(itemId, edges);
  }

  public void addEdge(VersionHistoryDAG dag, long parentId, long childId, long itemId) throws GroundException {
    VersionSuccessor successor = this.versionSuccessorFactory.create(parentId, childId);

    dag.addEdge(parentId, childId, successor.getId());
  }
}
