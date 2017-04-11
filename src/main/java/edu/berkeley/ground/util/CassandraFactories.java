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

    CassandraVersionSuccessorFactory versionSuccessorFactory =
        new CassandraVersionSuccessorFactory(cassandraClient, idGenerator);
    CassandraVersionHistoryDagFactory versionHistoryDagFactory =
        new CassandraVersionHistoryDagFactory(cassandraClient, versionSuccessorFactory);
    CassandraTagFactory tagFactory = new CassandraTagFactory(cassandraClient);

    this.structureFactory = new CassandraStructureFactory(cassandraClient, versionHistoryDagFactory,
        tagFactory, idGenerator);
    this.structureVersionFactory = new CassandraStructureVersionFactory(cassandraClient,
        this.structureFactory, idGenerator);
    this.edgeFactory = new CassandraEdgeFactory(cassandraClient, versionHistoryDagFactory,
        tagFactory, idGenerator);
    this.edgeVersionFactory = new CassandraEdgeVersionFactory(cassandraClient, this.edgeFactory,
        this.structureVersionFactory, tagFactory, idGenerator);
    this.edgeFactory.setEdgeVersionFactory(this.edgeVersionFactory);

    this.graphFactory = new CassandraGraphFactory(cassandraClient, versionHistoryDagFactory,
        tagFactory, idGenerator);
    this.graphVersionFactory = new CassandraGraphVersionFactory(cassandraClient, this.graphFactory,
        this.structureVersionFactory, tagFactory, idGenerator);
    this.nodeFactory = new CassandraNodeFactory(cassandraClient, versionHistoryDagFactory,
        tagFactory, idGenerator);
    this.nodeVersionFactory = new CassandraNodeVersionFactory(cassandraClient, this.nodeFactory,
        this.structureVersionFactory, tagFactory, idGenerator);

    this.lineageEdgeFactory = new CassandraLineageEdgeFactory(cassandraClient,
        versionHistoryDagFactory, tagFactory, idGenerator);
    this.lineageEdgeVersionFactory = new CassandraLineageEdgeVersionFactory(cassandraClient,
        this.lineageEdgeFactory, structureVersionFactory, tagFactory, idGenerator);
    this.lineageGraphFactory = new CassandraLineageGraphFactory(cassandraClient,
        versionHistoryDagFactory, tagFactory, idGenerator);
    this.lineageGraphVersionFactory = new CassandraLineageGraphVersionFactory(cassandraClient,
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
