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

import com.google.common.annotations.VisibleForTesting;
import dao.models.EdgeFactory;
import dao.models.EdgeVersionFactory;
import dao.models.GraphFactory;
import dao.models.GraphVersionFactory;
import dao.models.NodeFactory;
import dao.models.NodeVersionFactory;
import dao.models.StructureFactory;
import dao.models.StructureVersionFactory;
import dao.models.cassandra.CassandraEdgeFactory;
import dao.models.cassandra.CassandraEdgeVersionFactory;
import dao.models.cassandra.CassandraGraphFactory;
import dao.models.cassandra.CassandraGraphVersionFactory;
import dao.models.cassandra.CassandraNodeFactory;
import dao.models.cassandra.CassandraNodeVersionFactory;
import dao.models.cassandra.CassandraRichVersionFactory;
import dao.models.cassandra.CassandraStructureFactory;
import dao.models.cassandra.CassandraStructureVersionFactory;
import dao.models.cassandra.CassandraTagFactory;
import dao.usage.LineageEdgeFactory;
import dao.usage.LineageEdgeVersionFactory;
import dao.usage.LineageGraphFactory;
import dao.usage.LineageGraphVersionFactory;
import dao.usage.cassandra.CassandraLineageEdgeFactory;
import dao.usage.cassandra.CassandraLineageEdgeVersionFactory;
import dao.usage.cassandra.CassandraLineageGraphFactory;
import dao.usage.cassandra.CassandraLineageGraphVersionFactory;
import dao.versions.cassandra.CassandraItemFactory;
import dao.versions.cassandra.CassandraVersionFactory;
import dao.versions.cassandra.CassandraVersionHistoryDagFactory;
import dao.versions.cassandra.CassandraVersionSuccessorFactory;
import db.CassandraClient;
import db.DbClient;
import exceptions.GroundDbException;
import javax.inject.Inject;
import javax.inject.Singleton;
import play.Configuration;

@Singleton
public class CassandraFactories implements FactoryGenerator {
  private final CassandraClient cassandraClient;

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
   * @param configuration the Play app configuration
   */
  @Inject
  public CassandraFactories(Configuration configuration) throws GroundDbException {
    Configuration dbConf = configuration.getConfig("db");
    Configuration machineConf = configuration.getConfig("machine");

    this.cassandraClient = new CassandraClient(dbConf.getString("host"),
        dbConf.getInt("port"),
        dbConf.getString("name"),
        dbConf.getString("user"),
        dbConf.getString("password"));

    int machineId = machineConf.getInt("id");
    int numMachines = machineConf.getInt("count");

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

  @Override
  public DbClient getDbClient() {
    return this.cassandraClient;
  }
}
