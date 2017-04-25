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
import dao.models.postgres.PostgresEdgeFactory;
import dao.models.postgres.PostgresEdgeVersionFactory;
import dao.models.postgres.PostgresGraphFactory;
import dao.models.postgres.PostgresGraphVersionFactory;
import dao.models.postgres.PostgresNodeFactory;
import dao.models.postgres.PostgresNodeVersionFactory;
import dao.models.postgres.PostgresRichVersionFactory;
import dao.models.postgres.PostgresStructureFactory;
import dao.models.postgres.PostgresStructureVersionFactory;
import dao.models.postgres.PostgresTagFactory;
import dao.usage.LineageEdgeFactory;
import dao.usage.LineageEdgeVersionFactory;
import dao.usage.LineageGraphFactory;
import dao.usage.LineageGraphVersionFactory;
import dao.usage.postgres.PostgresLineageEdgeFactory;
import dao.usage.postgres.PostgresLineageEdgeVersionFactory;
import dao.usage.postgres.PostgresLineageGraphFactory;
import dao.usage.postgres.PostgresLineageGraphVersionFactory;
import dao.versions.postgres.PostgresItemFactory;
import dao.versions.postgres.PostgresVersionFactory;
import dao.versions.postgres.PostgresVersionHistoryDagFactory;
import dao.versions.postgres.PostgresVersionSuccessorFactory;
import db.DbClient;
import db.PostgresClient;
import exceptions.GroundDbException;
import javax.inject.Inject;
import javax.inject.Singleton;
import play.Configuration;

@Singleton
public class PostgresFactories implements FactoryGenerator {
  PostgresClient postgresClient;

  private final PostgresStructureFactory structureFactory;
  private final PostgresStructureVersionFactory structureVersionFactory;
  private final PostgresEdgeFactory edgeFactory;
  private final PostgresEdgeVersionFactory edgeVersionFactory;
  private final PostgresGraphFactory graphFactory;
  private final PostgresGraphVersionFactory graphVersionFactory;
  private final PostgresNodeFactory nodeFactory;
  private final PostgresNodeVersionFactory nodeVersionFactory;

  private final PostgresLineageEdgeFactory lineageEdgeFactory;
  private final PostgresLineageEdgeVersionFactory lineageEdgeVersionFactory;
  private final PostgresLineageGraphFactory lineageGraphFactory;
  private final PostgresLineageGraphVersionFactory lineageGraphVersionFactory;

  /**
   * Create the Postgres factories.
   *
   * @param configuration the Play app configuration
   */
  @Inject
  public PostgresFactories(Configuration configuration) throws GroundDbException {
    Configuration dbConf = configuration.getConfig("db");
    Configuration machineConf = configuration.getConfig("machine");

    this.postgresClient = new PostgresClient(dbConf.getString("host"),
        dbConf.getInt("port"),
        dbConf.getString("name"),
        dbConf.getString("user"),
        dbConf.getString("password"));

    int numMachines = machineConf.getInt("count");
    int machineId = machineConf.getInt("id");

    IdGenerator idGenerator = new IdGenerator(machineId, numMachines, false);

    PostgresVersionSuccessorFactory versionSuccessorFactory =
        new PostgresVersionSuccessorFactory(this.postgresClient, idGenerator);
    PostgresVersionHistoryDagFactory versionHistoryDagFactory =
        new PostgresVersionHistoryDagFactory(this.postgresClient, versionSuccessorFactory);
    PostgresTagFactory tagFactory = new PostgresTagFactory(this.postgresClient);

    this.structureFactory = new PostgresStructureFactory(this.postgresClient, versionHistoryDagFactory,
        tagFactory, idGenerator);
    this.structureVersionFactory = new PostgresStructureVersionFactory(this.postgresClient,
        this.structureFactory, idGenerator);
    this.edgeFactory = new PostgresEdgeFactory(this.postgresClient, versionHistoryDagFactory,
        tagFactory, idGenerator);
    this.edgeVersionFactory = new PostgresEdgeVersionFactory(this.postgresClient, this.edgeFactory,
        this.structureVersionFactory, tagFactory, idGenerator);
    this.edgeFactory.setEdgeVersionFactory(this.edgeVersionFactory);

    this.graphFactory = new PostgresGraphFactory(this.postgresClient, versionHistoryDagFactory,
        tagFactory, idGenerator);
    this.graphVersionFactory = new PostgresGraphVersionFactory(this.postgresClient, this.graphFactory,
        this.structureVersionFactory, tagFactory, idGenerator);
    this.nodeFactory = new PostgresNodeFactory(this.postgresClient, versionHistoryDagFactory,
        tagFactory, idGenerator);
    this.nodeVersionFactory = new PostgresNodeVersionFactory(this.postgresClient, this.nodeFactory,
        this.structureVersionFactory, tagFactory, idGenerator);

    this.lineageEdgeFactory = new PostgresLineageEdgeFactory(this.postgresClient,
        versionHistoryDagFactory, tagFactory, idGenerator);
    this.lineageEdgeVersionFactory = new PostgresLineageEdgeVersionFactory(this.postgresClient,
        this.lineageEdgeFactory, structureVersionFactory, tagFactory, idGenerator);
    this.lineageGraphFactory = new PostgresLineageGraphFactory(this.postgresClient,
        versionHistoryDagFactory, tagFactory, idGenerator);
    this.lineageGraphVersionFactory = new PostgresLineageGraphVersionFactory(this.postgresClient,
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
    return this.postgresClient;
  }
}
