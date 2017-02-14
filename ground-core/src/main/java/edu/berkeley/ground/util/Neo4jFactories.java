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

package edu.berkeley.ground.util;

import edu.berkeley.ground.api.models.*;
import edu.berkeley.ground.api.models.neo4j.*;
import edu.berkeley.ground.api.usage.LineageEdgeFactory;
import edu.berkeley.ground.api.usage.LineageEdgeVersionFactory;
import edu.berkeley.ground.api.usage.neo4j.Neo4jLineageEdgeFactory;
import edu.berkeley.ground.api.usage.neo4j.Neo4jLineageEdgeVersionFactory;
import edu.berkeley.ground.api.versions.neo4j.Neo4jItemFactory;
import edu.berkeley.ground.api.versions.neo4j.Neo4jVersionHistoryDAGFactory;
import edu.berkeley.ground.api.versions.neo4j.Neo4jVersionSuccessorFactory;
import edu.berkeley.ground.db.Neo4jClient;

public class Neo4jFactories {
  private Neo4jStructureFactory structureFactory;
  private Neo4jStructureVersionFactory structureVersionFactory;
  private Neo4jEdgeFactory edgeFactory;
  private Neo4jEdgeVersionFactory edgeVersionFactory;
  private Neo4jGraphFactory graphFactory;
  private Neo4jGraphVersionFactory graphVersionFactory;
  private Neo4jNodeFactory nodeFactory;
  private Neo4jNodeVersionFactory nodeVersionFactory;

  private Neo4jLineageEdgeFactory lineageEdgeFactory;
  private Neo4jLineageEdgeVersionFactory lineageEdgeVersionFactory;

  public Neo4jFactories(Neo4jClient neo4jClient, int machineId, int numMachines) {
    IdGenerator idGenerator = new IdGenerator(machineId, numMachines, true);

    Neo4jVersionSuccessorFactory versionSuccessorFactory = new Neo4jVersionSuccessorFactory(idGenerator);
    Neo4jVersionHistoryDAGFactory versionHistoryDAGFactory = new Neo4jVersionHistoryDAGFactory(versionSuccessorFactory);
    Neo4jItemFactory itemFactory = new Neo4jItemFactory(versionHistoryDAGFactory);

    this.structureFactory = new Neo4jStructureFactory(neo4jClient, itemFactory, idGenerator);
    this.structureVersionFactory = new Neo4jStructureVersionFactory(neo4jClient, this.structureFactory, idGenerator);
    Neo4jTagFactory tagFactory = new Neo4jTagFactory();
    Neo4jRichVersionFactory richVersionFactory = new Neo4jRichVersionFactory(structureVersionFactory, tagFactory);
    this.edgeFactory = new Neo4jEdgeFactory(itemFactory, neo4jClient, idGenerator);
    this.edgeVersionFactory = new Neo4jEdgeVersionFactory(this.edgeFactory, richVersionFactory, neo4jClient, idGenerator);
    this.graphFactory = new Neo4jGraphFactory(neo4jClient, itemFactory, idGenerator);
    this.graphVersionFactory = new Neo4jGraphVersionFactory(neo4jClient, this.graphFactory, richVersionFactory, idGenerator);
    this.nodeFactory = new Neo4jNodeFactory(itemFactory, neo4jClient, idGenerator);
    this.nodeVersionFactory = new Neo4jNodeVersionFactory(this.nodeFactory, richVersionFactory, neo4jClient, idGenerator);

    this.lineageEdgeFactory = new Neo4jLineageEdgeFactory(itemFactory, neo4jClient, idGenerator);
    this.lineageEdgeVersionFactory = new Neo4jLineageEdgeVersionFactory(this.lineageEdgeFactory, richVersionFactory, neo4jClient, idGenerator);
  }

  public EdgeFactory getEdgeFactory() {
    return edgeFactory;
  }

  public EdgeVersionFactory getEdgeVersionFactory() {
    return edgeVersionFactory;
  }

  public GraphFactory getGraphFactory() {
    return graphFactory;
  }

  public GraphVersionFactory getGraphVersionFactory() {
    return graphVersionFactory;
  }

  public NodeFactory getNodeFactory() {
    return nodeFactory;
  }

  public NodeVersionFactory getNodeVersionFactory() {
    return nodeVersionFactory;
  }

  public LineageEdgeFactory getLineageEdgeFactory() {
    return lineageEdgeFactory;
  }

  public LineageEdgeVersionFactory getLineageEdgeVersionFactory() {
    return lineageEdgeVersionFactory;
  }

  public StructureFactory getStructureFactory() {
    return structureFactory;
  }

  public StructureVersionFactory getStructureVersionFactory() {
    return structureVersionFactory;
  }
}
