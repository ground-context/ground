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
import edu.berkeley.ground.api.models.postgres.*;
import edu.berkeley.ground.api.usage.LineageEdgeFactory;
import edu.berkeley.ground.api.usage.LineageEdgeVersionFactory;
import edu.berkeley.ground.api.usage.LineageGraphFactory;
import edu.berkeley.ground.api.usage.LineageGraphVersionFactory;
import edu.berkeley.ground.api.usage.postgres.PostgresLineageEdgeFactory;
import edu.berkeley.ground.api.usage.postgres.PostgresLineageEdgeVersionFactory;
import edu.berkeley.ground.api.usage.postgres.PostgresLineageGraphFactory;
import edu.berkeley.ground.api.usage.postgres.PostgresLineageGraphVersionFactory;
import edu.berkeley.ground.api.versions.postgres.PostgresItemFactory;
import edu.berkeley.ground.api.versions.postgres.PostgresVersionFactory;
import edu.berkeley.ground.api.versions.postgres.PostgresVersionHistoryDAGFactory;
import edu.berkeley.ground.api.versions.postgres.PostgresVersionSuccessorFactory;
import edu.berkeley.ground.db.PostgresClient;

public class PostgresFactories implements FactoryGenerator {
  private PostgresStructureFactory structureFactory;
  private PostgresStructureVersionFactory structureVersionFactory;
  private PostgresEdgeFactory edgeFactory;
  private PostgresEdgeVersionFactory edgeVersionFactory;
  private PostgresGraphFactory graphFactory;
  private PostgresGraphVersionFactory graphVersionFactory;
  private PostgresNodeFactory nodeFactory;
  private PostgresNodeVersionFactory nodeVersionFactory;

  private PostgresLineageEdgeFactory lineageEdgeFactory;
  private PostgresLineageEdgeVersionFactory lineageEdgeVersionFactory;
  private PostgresLineageGraphFactory lineageGraphFactory;
  private PostgresLineageGraphVersionFactory lineageGraphVersionFactory;

  public PostgresFactories(PostgresClient postgresClient, int machineId, int numMachines) {
    IdGenerator idGenerator = new IdGenerator(machineId, numMachines, false);

    PostgresVersionFactory versionFactory = new PostgresVersionFactory(postgresClient);
    PostgresVersionSuccessorFactory versionSuccessorFactory = new PostgresVersionSuccessorFactory(postgresClient, idGenerator);
    PostgresVersionHistoryDAGFactory versionHistoryDAGFactory = new PostgresVersionHistoryDAGFactory(postgresClient, versionSuccessorFactory);
    PostgresTagFactory tagFactory = new PostgresTagFactory(postgresClient);
    PostgresItemFactory itemFactory = new PostgresItemFactory(postgresClient, versionHistoryDAGFactory, tagFactory);

    this.structureFactory = new PostgresStructureFactory(itemFactory, postgresClient, idGenerator);
    this.structureVersionFactory = new PostgresStructureVersionFactory(this.structureFactory,
        versionFactory, postgresClient, idGenerator);
    PostgresRichVersionFactory richVersionFactory = new PostgresRichVersionFactory
        (postgresClient, versionFactory, structureVersionFactory, tagFactory);
    this.edgeFactory = new PostgresEdgeFactory(itemFactory, postgresClient, idGenerator);
    this.edgeVersionFactory = new PostgresEdgeVersionFactory(this.edgeFactory,
        richVersionFactory, postgresClient, idGenerator);
    this.graphFactory = new PostgresGraphFactory(itemFactory, postgresClient, idGenerator);
    this.graphVersionFactory = new PostgresGraphVersionFactory(this.graphFactory,
        richVersionFactory, postgresClient, idGenerator);
    this.nodeFactory = new PostgresNodeFactory(itemFactory, postgresClient, idGenerator);
    this.nodeVersionFactory = new PostgresNodeVersionFactory(this.nodeFactory,
        richVersionFactory, postgresClient, idGenerator);

    this.lineageEdgeFactory = new PostgresLineageEdgeFactory(itemFactory, postgresClient, idGenerator);
    this.lineageEdgeVersionFactory = new PostgresLineageEdgeVersionFactory(this
        .lineageEdgeFactory, richVersionFactory, postgresClient, idGenerator);
    this.lineageGraphFactory = new PostgresLineageGraphFactory(itemFactory, postgresClient, idGenerator);
    this.lineageGraphVersionFactory = new PostgresLineageGraphVersionFactory(this
        .lineageGraphFactory, richVersionFactory, postgresClient, idGenerator);
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
