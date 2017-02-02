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
import edu.berkeley.ground.api.models.gremlin.*;
import edu.berkeley.ground.api.usage.LineageEdgeFactory;
import edu.berkeley.ground.api.usage.LineageEdgeVersionFactory;
import edu.berkeley.ground.api.usage.gremlin.GremlinLineageEdgeFactory;
import edu.berkeley.ground.api.usage.gremlin.GremlinLineageEdgeVersionFactory;
import edu.berkeley.ground.api.versions.gremlin.GremlinItemFactory;
import edu.berkeley.ground.api.versions.gremlin.GremlinVersionHistoryDAGFactory;
import edu.berkeley.ground.api.versions.gremlin.GremlinVersionSuccessorFactory;
import edu.berkeley.ground.db.GremlinClient;

public class GremlinFactories {
  private GremlinStructureFactory structureFactory;
  private GremlinStructureVersionFactory structureVersionFactory;
  private GremlinEdgeFactory edgeFactory;
  private GremlinEdgeVersionFactory edgeVersionFactory;
  private GremlinGraphFactory graphFactory;
  private GremlinGraphVersionFactory graphVersionFactory;
  private GremlinNodeFactory nodeFactory;
  private GremlinNodeVersionFactory nodeVersionFactory;

  private GremlinLineageEdgeFactory lineageEdgeFactory;
  private GremlinLineageEdgeVersionFactory lineageEdgeVersionFactory;

  public GremlinFactories(GremlinClient cassandraClient) {
    GremlinVersionSuccessorFactory versionSuccessorFactory = new GremlinVersionSuccessorFactory();
    GremlinVersionHistoryDAGFactory versionHistoryDAGFactory = new GremlinVersionHistoryDAGFactory(versionSuccessorFactory);
    GremlinItemFactory itemFactory = new GremlinItemFactory(versionHistoryDAGFactory);

    this.structureFactory = new GremlinStructureFactory(itemFactory, cassandraClient);
    this.structureVersionFactory = new GremlinStructureVersionFactory(this.structureFactory, cassandraClient);
    GremlinTagFactory tagFactory = new GremlinTagFactory();
    GremlinRichVersionFactory richVersionFactory = new GremlinRichVersionFactory(structureVersionFactory, tagFactory);
    this.edgeFactory = new GremlinEdgeFactory(itemFactory, cassandraClient);
    this.edgeVersionFactory = new GremlinEdgeVersionFactory(this.edgeFactory, richVersionFactory, cassandraClient);
    this.graphFactory = new GremlinGraphFactory(itemFactory, cassandraClient);
    this.graphVersionFactory = new GremlinGraphVersionFactory(this.graphFactory, richVersionFactory, cassandraClient);
    this.nodeFactory = new GremlinNodeFactory(itemFactory, cassandraClient);
    this.nodeVersionFactory = new GremlinNodeVersionFactory(this.nodeFactory, richVersionFactory, cassandraClient);

    this.lineageEdgeFactory = new GremlinLineageEdgeFactory(itemFactory, cassandraClient);
    this.lineageEdgeVersionFactory = new GremlinLineageEdgeVersionFactory(this.lineageEdgeFactory, richVersionFactory, cassandraClient);
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
