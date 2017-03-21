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
import edu.berkeley.ground.api.usage.LineageGraphFactory;
import edu.berkeley.ground.api.usage.LineageGraphVersionFactory;
import edu.berkeley.ground.api.usage.neo4j.Neo4jLineageEdgeFactory;
import edu.berkeley.ground.api.usage.neo4j.Neo4jLineageEdgeVersionFactory;
import edu.berkeley.ground.api.usage.neo4j.Neo4jLineageGraphFactory;
import edu.berkeley.ground.api.usage.neo4j.Neo4jLineageGraphVersionFactory;
import edu.berkeley.ground.api.versions.neo4j.Neo4jItemFactory;
import edu.berkeley.ground.api.versions.neo4j.Neo4jVersionHistoryDAGFactory;
import edu.berkeley.ground.api.versions.neo4j.Neo4jVersionSuccessorFactory;
import edu.berkeley.ground.db.Neo4jClient;

public class Neo4jFactories implements FactoryGenerator {

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
  private Neo4jLineageGraphFactory lineageGraphFactory;
  private Neo4jLineageGraphVersionFactory lineageGraphVersionFactory;

  public Neo4jFactories(Neo4jClient neo4jClient, int machineId, int numMachines) {
    IdGenerator idGenerator = new IdGenerator(machineId, numMachines, true);

    Neo4jVersionSuccessorFactory versionSuccessorFactory = new Neo4jVersionSuccessorFactory
        (neo4jClient, idGenerator);
    Neo4jVersionHistoryDAGFactory versionHistoryDAGFactory = new Neo4jVersionHistoryDAGFactory
        (neo4jClient, versionSuccessorFactory);
    Neo4jTagFactory tagFactory = new Neo4jTagFactory(neo4jClient);
    Neo4jItemFactory itemFactory = new Neo4jItemFactory(neo4jClient, versionHistoryDAGFactory,
        tagFactory);

    this.structureFactory = new Neo4jStructureFactory(neo4jClient, itemFactory, idGenerator);
    this.structureVersionFactory = new Neo4jStructureVersionFactory(neo4jClient, this
        .structureFactory, idGenerator);
    Neo4jRichVersionFactory richVersionFactory = new Neo4jRichVersionFactory(neo4jClient,
        structureVersionFactory, tagFactory);
    this.edgeFactory = new Neo4jEdgeFactory(itemFactory, neo4jClient, idGenerator);
    this.edgeVersionFactory = new Neo4jEdgeVersionFactory(this.edgeFactory, richVersionFactory,
        neo4jClient, idGenerator);
    this.graphFactory = new Neo4jGraphFactory(neo4jClient, itemFactory, idGenerator);
    this.graphVersionFactory = new Neo4jGraphVersionFactory(neo4jClient, this.graphFactory,
        richVersionFactory, idGenerator);
    this.nodeFactory = new Neo4jNodeFactory(itemFactory, neo4jClient, idGenerator);
    this.nodeVersionFactory = new Neo4jNodeVersionFactory(this.nodeFactory, richVersionFactory,
        neo4jClient, idGenerator);

    this.lineageEdgeFactory = new Neo4jLineageEdgeFactory(itemFactory, neo4jClient, idGenerator);
    this.lineageEdgeVersionFactory = new Neo4jLineageEdgeVersionFactory(this.lineageEdgeFactory,
        richVersionFactory, neo4jClient, idGenerator);
    this.lineageGraphFactory = new Neo4jLineageGraphFactory(neo4jClient, itemFactory, idGenerator);
    this.lineageGraphVersionFactory = new Neo4jLineageGraphVersionFactory(neo4jClient, this
        .lineageGraphFactory, richVersionFactory, idGenerator);
  }

  public EdgeFactory getEdgeFactory() {
    return this.edgeFactory;
  }

  public EdgeVersionFactory getEdgeVersionFactory() {
    return this.edgeVersionFactory;
  }

  public GraphFactory getGraphFactory() {
    return this.graphFactory;
  }

  public GraphVersionFactory getGraphVersionFactory() {
    return this.graphVersionFactory;
  }

  public NodeFactory getNodeFactory() {
    return this.nodeFactory;
  }

  public NodeVersionFactory getNodeVersionFactory() {
    return this.nodeVersionFactory;
  }

  public LineageEdgeFactory getLineageEdgeFactory() {
    return this.lineageEdgeFactory;
  }

  public LineageEdgeVersionFactory getLineageEdgeVersionFactory() {
    return this.lineageEdgeVersionFactory;
  }

  public StructureFactory getStructureFactory() {
    return this.structureFactory;
  }

  public StructureVersionFactory getStructureVersionFactory() {
    return this.structureVersionFactory;
  }

  public LineageGraphFactory getLineageGraphFactory() {
    return this.lineageGraphFactory;
  }

  public LineageGraphVersionFactory getLineageGraphVersionFactory() {
    return this.lineageGraphVersionFactory;
  }
}
