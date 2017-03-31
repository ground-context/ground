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

import edu.berkeley.ground.dao.models.*;
import edu.berkeley.ground.dao.models.postgres.PostgresEdgeFactory;
import edu.berkeley.ground.dao.models.postgres.PostgresEdgeVersionFactory;
import edu.berkeley.ground.dao.models.postgres.PostgresGraphFactory;
import edu.berkeley.ground.dao.models.postgres.PostgresGraphVersionFactory;
import edu.berkeley.ground.dao.models.postgres.PostgresNodeFactory;
import edu.berkeley.ground.dao.models.postgres.PostgresNodeVersionFactory;
import edu.berkeley.ground.dao.models.postgres.PostgresRichVersionFactory;
import edu.berkeley.ground.dao.models.postgres.PostgresStructureFactory;
import edu.berkeley.ground.dao.models.postgres.PostgresStructureVersionFactory;
import edu.berkeley.ground.dao.models.postgres.PostgresTagFactory;
import edu.berkeley.ground.dao.usage.LineageEdgeFactory;
import edu.berkeley.ground.dao.usage.LineageEdgeVersionFactory;
import edu.berkeley.ground.dao.usage.LineageGraphFactory;
import edu.berkeley.ground.dao.usage.LineageGraphVersionFactory;
import edu.berkeley.ground.dao.usage.postgres.PostgresLineageEdgeFactory;
import edu.berkeley.ground.dao.usage.postgres.PostgresLineageEdgeVersionFactory;
import edu.berkeley.ground.dao.usage.postgres.PostgresLineageGraphFactory;
import edu.berkeley.ground.dao.usage.postgres.PostgresLineageGraphVersionFactory;
import edu.berkeley.ground.dao.versions.postgres.PostgresItemFactory;
import edu.berkeley.ground.dao.versions.postgres.PostgresVersionFactory;
import edu.berkeley.ground.dao.versions.postgres.PostgresVersionHistoryDagFactory;
import edu.berkeley.ground.dao.versions.postgres.PostgresVersionSuccessorFactory;
import edu.berkeley.ground.db.PostgresClient;

public class PostgresFactories implements FactoryGenerator {
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
  private final PostgresTagFactory tagFactory;

  /**
   * Create the Postgres factories.
   *
   * @param postgresClient the Postgres client
   * @param machineId the id of this machine
   * @param numMachines the total number of machines
   */
  public PostgresFactories(PostgresClient postgresClient, int machineId, int numMachines) {
    IdGenerator idGenerator = new IdGenerator(machineId, numMachines, false);

    PostgresVersionFactory versionFactory = new PostgresVersionFactory(postgresClient);
    PostgresVersionSuccessorFactory versionSuccessorFactory =
        new PostgresVersionSuccessorFactory(postgresClient, idGenerator);
    PostgresVersionHistoryDagFactory versionHistoryDagFactory =
        new PostgresVersionHistoryDagFactory(postgresClient, versionSuccessorFactory);
    PostgresTagFactory tagFactory = new PostgresTagFactory(postgresClient);
    PostgresItemFactory itemFactory =
        new PostgresItemFactory(postgresClient, versionHistoryDagFactory, tagFactory);

    this.structureFactory = new PostgresStructureFactory(itemFactory, postgresClient, idGenerator);
    this.structureVersionFactory = new PostgresStructureVersionFactory(this.structureFactory,
        versionFactory, postgresClient, idGenerator);
    PostgresRichVersionFactory richVersionFactory = new PostgresRichVersionFactory(postgresClient,
        versionFactory, structureVersionFactory, tagFactory);
    this.edgeFactory = new PostgresEdgeFactory(itemFactory, postgresClient, idGenerator,
        versionHistoryDagFactory);
    this.edgeVersionFactory = new PostgresEdgeVersionFactory(this.edgeFactory,
        richVersionFactory, postgresClient, idGenerator);
    this.edgeFactory.setEdgeVersionFactory(this.edgeVersionFactory);

    this.graphFactory = new PostgresGraphFactory(itemFactory, postgresClient, idGenerator);
    this.graphVersionFactory = new PostgresGraphVersionFactory(this.graphFactory,
        richVersionFactory, postgresClient, idGenerator);
    this.nodeFactory = new PostgresNodeFactory(itemFactory, postgresClient, idGenerator);
    this.nodeVersionFactory = new PostgresNodeVersionFactory(this.nodeFactory,
        richVersionFactory, postgresClient, idGenerator);

    this.lineageEdgeFactory = new PostgresLineageEdgeFactory(itemFactory,
        postgresClient, idGenerator);
    this.lineageEdgeVersionFactory = new PostgresLineageEdgeVersionFactory(this.lineageEdgeFactory,
        richVersionFactory, postgresClient, idGenerator);
    this.lineageGraphFactory = new PostgresLineageGraphFactory(itemFactory,
        postgresClient, idGenerator);
    this.lineageGraphVersionFactory = new PostgresLineageGraphVersionFactory(
        this.lineageGraphFactory, richVersionFactory, postgresClient, idGenerator);
    this.tagFactory = new PostgresTagFactory(postgresClient);
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
  public TagFactory getTagFactory() {
    return this.tagFactory;
  }
}
