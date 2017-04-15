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

package edu.berkeley.ground.util;

import edu.berkeley.ground.dao.models.EdgeFactory;
import edu.berkeley.ground.dao.models.EdgeVersionFactory;
import edu.berkeley.ground.dao.models.GraphFactory;
import edu.berkeley.ground.dao.models.GraphVersionFactory;
import edu.berkeley.ground.dao.models.NodeFactory;
import edu.berkeley.ground.dao.models.NodeVersionFactory;
import edu.berkeley.ground.dao.models.StructureFactory;
import edu.berkeley.ground.dao.models.StructureVersionFactory;
import edu.berkeley.ground.dao.models.neo4j.Neo4jEdgeFactory;
import edu.berkeley.ground.dao.models.neo4j.Neo4jEdgeVersionFactory;
import edu.berkeley.ground.dao.models.neo4j.Neo4jGraphFactory;
import edu.berkeley.ground.dao.models.neo4j.Neo4jGraphVersionFactory;
import edu.berkeley.ground.dao.models.neo4j.Neo4jNodeFactory;
import edu.berkeley.ground.dao.models.neo4j.Neo4jNodeVersionFactory;
import edu.berkeley.ground.dao.models.neo4j.Neo4jRichVersionFactory;
import edu.berkeley.ground.dao.models.neo4j.Neo4jStructureFactory;
import edu.berkeley.ground.dao.models.neo4j.Neo4jStructureVersionFactory;
import edu.berkeley.ground.dao.models.neo4j.Neo4jTagFactory;
import edu.berkeley.ground.dao.usage.LineageEdgeFactory;
import edu.berkeley.ground.dao.usage.LineageEdgeVersionFactory;
import edu.berkeley.ground.dao.usage.LineageGraphFactory;
import edu.berkeley.ground.dao.usage.LineageGraphVersionFactory;
import edu.berkeley.ground.dao.usage.neo4j.Neo4jLineageEdgeFactory;
import edu.berkeley.ground.dao.usage.neo4j.Neo4jLineageEdgeVersionFactory;
import edu.berkeley.ground.dao.usage.neo4j.Neo4jLineageGraphFactory;
import edu.berkeley.ground.dao.usage.neo4j.Neo4jLineageGraphVersionFactory;
import edu.berkeley.ground.dao.versions.neo4j.Neo4jItemFactory;
import edu.berkeley.ground.dao.versions.neo4j.Neo4jVersionHistoryDagFactory;
import edu.berkeley.ground.dao.versions.neo4j.Neo4jVersionSuccessorFactory;
import edu.berkeley.ground.db.Neo4jClient;

public class Neo4jFactories implements FactoryGenerator {

  private final Neo4jStructureFactory structureFactory;
  private final Neo4jStructureVersionFactory structureVersionFactory;
  private final Neo4jEdgeFactory edgeFactory;
  private final Neo4jEdgeVersionFactory edgeVersionFactory;
  private final Neo4jGraphFactory graphFactory;
  private final Neo4jGraphVersionFactory graphVersionFactory;
  private final Neo4jNodeFactory nodeFactory;
  private final Neo4jNodeVersionFactory nodeVersionFactory;

  private final Neo4jLineageEdgeFactory lineageEdgeFactory;
  private final Neo4jLineageEdgeVersionFactory lineageEdgeVersionFactory;
  private final Neo4jLineageGraphFactory lineageGraphFactory;
  private final Neo4jLineageGraphVersionFactory lineageGraphVersionFactory;

  /**
   * Create the Neo4j factories.
   *
   * @param neo4jClient the Neo4jClient
   * @param machineId the id of this machine
   * @param numMachines the total number of machines
   */
  public Neo4jFactories(Neo4jClient neo4jClient, int machineId, int numMachines) {
    IdGenerator idGenerator = new IdGenerator(machineId, numMachines, true);

    Neo4jVersionSuccessorFactory versionSuccessorFactory =
        new Neo4jVersionSuccessorFactory(neo4jClient, idGenerator);
    Neo4jVersionHistoryDagFactory versionHistoryDagFactory =
        new Neo4jVersionHistoryDagFactory(neo4jClient, versionSuccessorFactory);
    Neo4jTagFactory tagFactory = new Neo4jTagFactory(neo4jClient);

    this.structureFactory = new Neo4jStructureFactory(neo4jClient, versionHistoryDagFactory,
        tagFactory, idGenerator);
    this.structureVersionFactory = new Neo4jStructureVersionFactory(neo4jClient,
        this.structureFactory, idGenerator);
    this.edgeFactory = new Neo4jEdgeFactory(neo4jClient, versionHistoryDagFactory,
        tagFactory, idGenerator);
    this.edgeVersionFactory = new Neo4jEdgeVersionFactory(neo4jClient, this.edgeFactory,
        this.structureVersionFactory, tagFactory, idGenerator);
    this.edgeFactory.setEdgeVersionFactory(this.edgeVersionFactory);

    this.graphFactory = new Neo4jGraphFactory(neo4jClient, versionHistoryDagFactory,
        tagFactory, idGenerator);
    this.graphVersionFactory = new Neo4jGraphVersionFactory(neo4jClient, this.graphFactory,
        this.structureVersionFactory, tagFactory, idGenerator);
    this.nodeFactory = new Neo4jNodeFactory(neo4jClient, versionHistoryDagFactory,
        tagFactory, idGenerator);
    this.nodeVersionFactory = new Neo4jNodeVersionFactory(neo4jClient, this.nodeFactory,
        this.structureVersionFactory, tagFactory, idGenerator);

    this.lineageEdgeFactory = new Neo4jLineageEdgeFactory(neo4jClient,
        versionHistoryDagFactory, tagFactory, idGenerator);
    this.lineageEdgeVersionFactory = new Neo4jLineageEdgeVersionFactory(neo4jClient,
        this.lineageEdgeFactory, structureVersionFactory, tagFactory, idGenerator);
    this.lineageGraphFactory = new Neo4jLineageGraphFactory(neo4jClient,
        versionHistoryDagFactory, tagFactory, idGenerator);
    this.lineageGraphVersionFactory = new Neo4jLineageGraphVersionFactory(neo4jClient,
        this.lineageGraphFactory, this.structureVersionFactory, tagFactory, idGenerator);
  }

  @Override
  public EdgeFactory getEdgeFactory() {
    return this.edgeFactory;
  }

  @Override
  public EdgeVersionFactory getEdgeVersionFactory() {
    return this.edgeVersionFactory;
  }

  @Override
  public GraphFactory getGraphFactory() {
    return this.graphFactory;
  }

  @Override
  public GraphVersionFactory getGraphVersionFactory() {
    return this.graphVersionFactory;
  }

  @Override
  public NodeFactory getNodeFactory() {
    return this.nodeFactory;
  }

  @Override
  public NodeVersionFactory getNodeVersionFactory() {
    return this.nodeVersionFactory;
  }

  @Override
  public LineageEdgeFactory getLineageEdgeFactory() {
    return this.lineageEdgeFactory;
  }

  @Override
  public LineageEdgeVersionFactory getLineageEdgeVersionFactory() {
    return this.lineageEdgeVersionFactory;
  }

  @Override
  public StructureFactory getStructureFactory() {
    return this.structureFactory;
  }

  @Override
  public StructureVersionFactory getStructureVersionFactory() {
    return this.structureVersionFactory;
  }

  @Override
  public LineageGraphFactory getLineageGraphFactory() {
    return this.lineageGraphFactory;
  }

  @Override
  public LineageGraphVersionFactory getLineageGraphVersionFactory() {
    return this.lineageGraphVersionFactory;
  }
}
