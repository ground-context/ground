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
import edu.berkeley.ground.dao.models.cassandra.CassandraEdgeFactory;
import edu.berkeley.ground.dao.models.cassandra.CassandraEdgeVersionFactory;
import edu.berkeley.ground.dao.models.cassandra.CassandraGraphFactory;
import edu.berkeley.ground.dao.models.cassandra.CassandraGraphVersionFactory;
import edu.berkeley.ground.dao.models.cassandra.CassandraNodeFactory;
import edu.berkeley.ground.dao.models.cassandra.CassandraNodeVersionFactory;
import edu.berkeley.ground.dao.models.cassandra.CassandraRichVersionFactory;
import edu.berkeley.ground.dao.models.cassandra.CassandraStructureFactory;
import edu.berkeley.ground.dao.models.cassandra.CassandraStructureVersionFactory;
import edu.berkeley.ground.dao.models.cassandra.CassandraTagFactory;
import edu.berkeley.ground.dao.usage.LineageEdgeFactory;
import edu.berkeley.ground.dao.usage.LineageEdgeVersionFactory;
import edu.berkeley.ground.dao.usage.LineageGraphFactory;
import edu.berkeley.ground.dao.usage.LineageGraphVersionFactory;
import edu.berkeley.ground.dao.usage.cassandra.CassandraLineageEdgeFactory;
import edu.berkeley.ground.dao.usage.cassandra.CassandraLineageEdgeVersionFactory;
import edu.berkeley.ground.dao.usage.cassandra.CassandraLineageGraphFactory;
import edu.berkeley.ground.dao.usage.cassandra.CassandraLineageGraphVersionFactory;
import edu.berkeley.ground.dao.versions.cassandra.CassandraItemFactory;
import edu.berkeley.ground.dao.versions.cassandra.CassandraVersionFactory;
import edu.berkeley.ground.dao.versions.cassandra.CassandraVersionHistoryDagFactory;
import edu.berkeley.ground.dao.versions.cassandra.CassandraVersionSuccessorFactory;
import edu.berkeley.ground.db.CassandraClient;

public class CassandraFactories implements FactoryGenerator {
  private final CassandraStructureFactory structureFactory;
  private final CassandraStructureVersionFactory structureVersionFactory;
  private final CassandraEdgeFactory edgeFactory;
  private final CassandraEdgeVersionFactory edgeVersionFactory;
  private final CassandraGraphFactory graphFactory;
  private final CassandraGraphVersionFactory graphVersionFactory;
  private final CassandraNodeFactory nodeFactory;
  private final CassandraNodeVersionFactory nodeVersionFactory;

  private final CassandraLineageEdgeFactory lineageEdgeFactory;
  private final CassandraLineageEdgeVersionFactory lineageEdgeVersionFactory;
  private final CassandraLineageGraphFactory lineageGraphFactory;
  private final CassandraLineageGraphVersionFactory lineageGraphVersionFactory;

  /**
   * Create all the Cassandra factories.
   *
   * @param cassandraClient the database client
   * @param machineId the id of this machine
   * @param numMachines the total number of machines
   */
  public CassandraFactories(CassandraClient cassandraClient, int machineId, int numMachines) {
    IdGenerator idGenerator = new IdGenerator(machineId, numMachines, false);

    CassandraVersionFactory versionFactory = new CassandraVersionFactory(cassandraClient);
    CassandraVersionSuccessorFactory versionSuccessorFactory =
        new CassandraVersionSuccessorFactory(cassandraClient, idGenerator);
    CassandraVersionHistoryDagFactory versionHistoryDagFactory =
        new CassandraVersionHistoryDagFactory(cassandraClient, versionSuccessorFactory);
    CassandraTagFactory tagFactory = new CassandraTagFactory(cassandraClient);
    CassandraItemFactory itemFactory =
        new CassandraItemFactory(cassandraClient, versionHistoryDagFactory, tagFactory);

    this.structureFactory =
        new CassandraStructureFactory(itemFactory, cassandraClient, idGenerator);
    this.structureVersionFactory = new CassandraStructureVersionFactory(this.structureFactory,
        versionFactory, cassandraClient, idGenerator);
    CassandraRichVersionFactory richVersionFactory = new CassandraRichVersionFactory(
        cassandraClient, versionFactory, structureVersionFactory, tagFactory);
    this.edgeFactory = new CassandraEdgeFactory(itemFactory, cassandraClient, idGenerator,
        versionHistoryDagFactory);
    this.edgeVersionFactory = new CassandraEdgeVersionFactory(this.edgeFactory, richVersionFactory,
        cassandraClient, idGenerator);
    this.edgeFactory.setEdgeVersionFactory(this.edgeVersionFactory);

    this.graphFactory = new CassandraGraphFactory(itemFactory, cassandraClient, idGenerator);
    this.graphVersionFactory = new CassandraGraphVersionFactory(this.graphFactory,
        richVersionFactory, cassandraClient, idGenerator);
    this.nodeFactory = new CassandraNodeFactory(itemFactory, cassandraClient, idGenerator);
    this.nodeVersionFactory = new CassandraNodeVersionFactory(this.nodeFactory, richVersionFactory,
        cassandraClient, idGenerator);

    this.lineageEdgeFactory =
        new CassandraLineageEdgeFactory(itemFactory, cassandraClient, idGenerator);
    this.lineageEdgeVersionFactory = new CassandraLineageEdgeVersionFactory(this.lineageEdgeFactory,
        richVersionFactory, cassandraClient, idGenerator);
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
