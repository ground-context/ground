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
import edu.berkeley.ground.api.models.cassandra.*;
import edu.berkeley.ground.api.usage.LineageEdgeFactory;
import edu.berkeley.ground.api.usage.LineageEdgeVersionFactory;
import edu.berkeley.ground.api.usage.LineageGraphFactory;
import edu.berkeley.ground.api.usage.LineageGraphVersionFactory;
import edu.berkeley.ground.api.usage.cassandra.CassandraLineageEdgeFactory;
import edu.berkeley.ground.api.usage.cassandra.CassandraLineageEdgeVersionFactory;
import edu.berkeley.ground.api.usage.cassandra.CassandraLineageGraphFactory;
import edu.berkeley.ground.api.usage.cassandra.CassandraLineageGraphVersionFactory;
import edu.berkeley.ground.api.versions.cassandra.CassandraItemFactory;
import edu.berkeley.ground.api.versions.cassandra.CassandraVersionFactory;
import edu.berkeley.ground.api.versions.cassandra.CassandraVersionHistoryDAGFactory;
import edu.berkeley.ground.api.versions.cassandra.CassandraVersionSuccessorFactory;
import edu.berkeley.ground.db.CassandraClient;

public class CassandraFactories implements FactoryGenerator {
  private CassandraStructureFactory structureFactory;
  private CassandraStructureVersionFactory structureVersionFactory;
  private CassandraEdgeFactory edgeFactory;
  private CassandraEdgeVersionFactory edgeVersionFactory;
  private CassandraGraphFactory graphFactory;
  private CassandraGraphVersionFactory graphVersionFactory;
  private CassandraNodeFactory nodeFactory;
  private CassandraNodeVersionFactory nodeVersionFactory;

  private CassandraLineageEdgeFactory lineageEdgeFactory;
  private CassandraLineageEdgeVersionFactory lineageEdgeVersionFactory;
  private CassandraLineageGraphFactory lineageGraphFactory;
  private CassandraLineageGraphVersionFactory lineageGraphVersionFactory;

  public CassandraFactories(CassandraClient cassandraClient, int machineId, int numMachines) {
    IdGenerator idGenerator = new IdGenerator(machineId, numMachines, false);

    CassandraVersionFactory versionFactory = new CassandraVersionFactory(cassandraClient);
    CassandraVersionSuccessorFactory versionSuccessorFactory = new CassandraVersionSuccessorFactory(cassandraClient, idGenerator);
    CassandraVersionHistoryDAGFactory versionHistoryDAGFactory = new CassandraVersionHistoryDAGFactory(cassandraClient, versionSuccessorFactory);
    CassandraTagFactory tagFactory = new CassandraTagFactory(cassandraClient);
    CassandraItemFactory itemFactory = new CassandraItemFactory(cassandraClient, versionHistoryDAGFactory, tagFactory);

    this.structureFactory = new CassandraStructureFactory(itemFactory, cassandraClient, idGenerator);
    this.structureVersionFactory = new CassandraStructureVersionFactory(this.structureFactory, versionFactory, cassandraClient, idGenerator);
    CassandraRichVersionFactory richVersionFactory = new CassandraRichVersionFactory
        (cassandraClient, versionFactory, structureVersionFactory, tagFactory);
    this.edgeFactory = new CassandraEdgeFactory(itemFactory, cassandraClient, idGenerator,
        versionHistoryDAGFactory);
    this.edgeVersionFactory = new CassandraEdgeVersionFactory(this.edgeFactory, richVersionFactory, cassandraClient, idGenerator);
    this.edgeFactory.setEdgeVersionFactory(this.edgeVersionFactory);

    this.graphFactory = new CassandraGraphFactory(itemFactory, cassandraClient, idGenerator);
    this.graphVersionFactory = new CassandraGraphVersionFactory(this.graphFactory, richVersionFactory, cassandraClient, idGenerator);
    this.nodeFactory = new CassandraNodeFactory(itemFactory, cassandraClient, idGenerator);
    this.nodeVersionFactory = new CassandraNodeVersionFactory(this.nodeFactory, richVersionFactory, cassandraClient, idGenerator);

    this.lineageEdgeFactory = new CassandraLineageEdgeFactory(itemFactory, cassandraClient, idGenerator);
    this.lineageEdgeVersionFactory = new CassandraLineageEdgeVersionFactory(this.lineageEdgeFactory, richVersionFactory, cassandraClient, idGenerator);
    this.lineageGraphFactory = new CassandraLineageGraphFactory(itemFactory, cassandraClient,
        idGenerator);
    this.lineageGraphVersionFactory = new CassandraLineageGraphVersionFactory(
        this.lineageGraphFactory, richVersionFactory, cassandraClient, idGenerator);
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
