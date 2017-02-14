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
import edu.berkeley.ground.api.usage.postgres.PostgresLineageEdgeFactory;
import edu.berkeley.ground.api.usage.postgres.PostgresLineageEdgeVersionFactory;
import edu.berkeley.ground.api.versions.postgres.PostgresItemFactory;
import edu.berkeley.ground.api.versions.postgres.PostgresVersionFactory;
import edu.berkeley.ground.api.versions.postgres.PostgresVersionHistoryDAGFactory;
import edu.berkeley.ground.api.versions.postgres.PostgresVersionSuccessorFactory;
import edu.berkeley.ground.db.PostgresClient;

public class PostgresFactories {
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

  public PostgresFactories(PostgresClient postgresClient, int machineId, int numMachines) {
    IdGenerator idGenerator = new IdGenerator(machineId, numMachines, false);

    PostgresVersionFactory versionFactory = new PostgresVersionFactory();
    PostgresVersionSuccessorFactory versionSuccessorFactory = new PostgresVersionSuccessorFactory(idGenerator);
    PostgresVersionHistoryDAGFactory versionHistoryDAGFactory = new PostgresVersionHistoryDAGFactory(versionSuccessorFactory);
    PostgresItemFactory itemFactory = new PostgresItemFactory(versionHistoryDAGFactory);

    this.structureFactory = new PostgresStructureFactory(itemFactory, postgresClient, idGenerator);
    this.structureVersionFactory = new PostgresStructureVersionFactory(this.structureFactory, versionFactory, postgresClient, idGenerator);
    PostgresTagFactory tagFactory = new PostgresTagFactory();
    PostgresRichVersionFactory richVersionFactory = new PostgresRichVersionFactory(versionFactory, structureVersionFactory, tagFactory);
    this.edgeFactory = new PostgresEdgeFactory(itemFactory, postgresClient, idGenerator);
    this.edgeVersionFactory = new PostgresEdgeVersionFactory(this.edgeFactory, richVersionFactory, postgresClient, idGenerator);
    this.graphFactory = new PostgresGraphFactory(itemFactory, postgresClient, idGenerator);
    this.graphVersionFactory = new PostgresGraphVersionFactory(this.graphFactory, richVersionFactory, postgresClient, idGenerator);
    this.nodeFactory = new PostgresNodeFactory(itemFactory, postgresClient, idGenerator);
    this.nodeVersionFactory = new PostgresNodeVersionFactory(this.nodeFactory, richVersionFactory, postgresClient, idGenerator);

    this.lineageEdgeFactory = new PostgresLineageEdgeFactory(itemFactory, postgresClient, idGenerator);
    this.lineageEdgeVersionFactory = new PostgresLineageEdgeVersionFactory(this.lineageEdgeFactory, richVersionFactory, postgresClient, idGenerator);
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
