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

package util;

import dao.models.EdgeFactory;
import dao.models.EdgeVersionFactory;
import dao.models.GraphFactory;
import dao.models.GraphVersionFactory;
import dao.models.NodeFactory;
import dao.models.NodeVersionFactory;
import dao.models.StructureFactory;
import dao.models.StructureVersionFactory;
import dao.models.neo4j.Neo4jEdgeFactory;
import dao.models.neo4j.Neo4jEdgeVersionFactory;
import dao.models.neo4j.Neo4jGraphFactory;
import dao.models.neo4j.Neo4jGraphVersionFactory;
import dao.models.neo4j.Neo4jNodeFactory;
import dao.models.neo4j.Neo4jNodeVersionFactory;
import dao.models.neo4j.Neo4jRichVersionFactory;
import dao.models.neo4j.Neo4jStructureFactory;
import dao.models.neo4j.Neo4jStructureVersionFactory;
import dao.models.neo4j.Neo4jTagFactory;
import dao.usage.LineageEdgeFactory;
import dao.usage.LineageEdgeVersionFactory;
import dao.usage.LineageGraphFactory;
import dao.usage.LineageGraphVersionFactory;
import dao.usage.neo4j.Neo4jLineageEdgeFactory;
import dao.usage.neo4j.Neo4jLineageEdgeVersionFactory;
import dao.usage.neo4j.Neo4jLineageGraphFactory;
import dao.usage.neo4j.Neo4jLineageGraphVersionFactory;
import dao.versions.neo4j.Neo4jItemFactory;
import dao.versions.neo4j.Neo4jVersionHistoryDagFactory;
import dao.versions.neo4j.Neo4jVersionSuccessorFactory;
import db.DbClient;
import db.Neo4jClient;
import exceptions.GroundDbException;
import javax.inject.Inject;
import javax.inject.Singleton;
import play.Configuration;

@Singleton
public class Neo4jFactories implements FactoryGenerator {
  private final Neo4jClient neo4jClient;

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
   * @param configuration the Play app configuration
   */
  @Inject
  public Neo4jFactories(Configuration configuration) throws GroundDbException {
    Configuration dbConf = configuration.getConfig("db");
    Configuration machineConf = configuration.getConfig("machine");

    this.neo4jClient = new Neo4jClient(dbConf.getString("host"),
        dbConf.getString("user"),
        dbConf.getString("password"));

    int machineId = machineConf.getInt("id");
    int numMachines = machineConf.getInt("count");


    IdGenerator idGenerator = new IdGenerator(machineId, numMachines, true);

    Neo4jVersionSuccessorFactory versionSuccessorFactory =
        new Neo4jVersionSuccessorFactory(this.neo4jClient, idGenerator);
    Neo4jVersionHistoryDagFactory versionHistoryDagFactory =
        new Neo4jVersionHistoryDagFactory(this.neo4jClient, versionSuccessorFactory);
    Neo4jTagFactory tagFactory = new Neo4jTagFactory(this.neo4jClient);

    this.structureFactory = new Neo4jStructureFactory(this.neo4jClient, versionHistoryDagFactory,
        tagFactory, idGenerator);
    this.structureVersionFactory = new Neo4jStructureVersionFactory(this.neo4jClient,
        this.structureFactory, idGenerator);
    this.edgeFactory = new Neo4jEdgeFactory(this.neo4jClient, versionHistoryDagFactory,
        tagFactory, idGenerator);
    this.edgeVersionFactory = new Neo4jEdgeVersionFactory(this.neo4jClient, this.edgeFactory,
        this.structureVersionFactory, tagFactory, idGenerator);
    this.edgeFactory.setEdgeVersionFactory(this.edgeVersionFactory);

    this.graphFactory = new Neo4jGraphFactory(this.neo4jClient, versionHistoryDagFactory,
        tagFactory, idGenerator);
    this.graphVersionFactory = new Neo4jGraphVersionFactory(this.neo4jClient, this.graphFactory,
        this.structureVersionFactory, tagFactory, idGenerator);
    this.nodeFactory = new Neo4jNodeFactory(this.neo4jClient, versionHistoryDagFactory,
        tagFactory, idGenerator);
    this.nodeVersionFactory = new Neo4jNodeVersionFactory(this.neo4jClient, this.nodeFactory,
        this.structureVersionFactory, tagFactory, idGenerator);

    this.lineageEdgeFactory = new Neo4jLineageEdgeFactory(this.neo4jClient,
        versionHistoryDagFactory, tagFactory, idGenerator);
    this.lineageEdgeVersionFactory = new Neo4jLineageEdgeVersionFactory(this.neo4jClient,
        this.lineageEdgeFactory, structureVersionFactory, tagFactory, idGenerator);
    this.lineageGraphFactory = new Neo4jLineageGraphFactory(this.neo4jClient,
        versionHistoryDagFactory, tagFactory, idGenerator);
    this.lineageGraphVersionFactory = new Neo4jLineageGraphVersionFactory(this.neo4jClient,
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

  @Override
  public DbClient getDbClient() {
    return this.neo4jClient;
  }
}
