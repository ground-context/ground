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

package edu.berkeley.ground.dao;

import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import edu.berkeley.ground.dao.models.neo4j.Neo4jRichVersionFactory;
import edu.berkeley.ground.dao.models.neo4j.Neo4jStructureVersionFactory;
import edu.berkeley.ground.dao.models.neo4j.Neo4jTagFactory;
import edu.berkeley.ground.dao.versions.neo4j.Neo4jItemFactory;
import edu.berkeley.ground.dao.versions.neo4j.Neo4jVersionHistoryDAGFactory;
import edu.berkeley.ground.dao.versions.neo4j.Neo4jVersionSuccessorFactory;
import edu.berkeley.ground.db.Neo4jClient;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.util.IdGenerator;
import edu.berkeley.ground.util.Neo4jFactories;

public class Neo4jTest {
    /* Note: In Neo4j, we don't create explicit (Rich)Versions because all of the logic is wrapped in
     * FooVersions. We are using NodeVersions as stand-ins because they are the most simple kind of
     * Versions. */

  protected Neo4jClient neo4jClient;
  protected Neo4jFactories factories;
  protected Neo4jVersionSuccessorFactory versionSuccessorFactory;
  protected Neo4jVersionHistoryDAGFactory versionHistoryDAGFactory;
  protected Neo4jItemFactory itemFactory;
  protected Neo4jTagFactory tagFactory;
  protected Neo4jRichVersionFactory richVersionFactory;

  public Neo4jTest() {
    this.neo4jClient = new Neo4jClient("localhost", "neo4j", "password");
    this.factories = new Neo4jFactories(this.neo4jClient, 0, 1);
    this.versionSuccessorFactory = new Neo4jVersionSuccessorFactory(this.neo4jClient, new IdGenerator(0, 1, true));
    this.versionHistoryDAGFactory = new Neo4jVersionHistoryDAGFactory(this.neo4jClient, this.versionSuccessorFactory);
    this.tagFactory = new Neo4jTagFactory(this.neo4jClient);
    this.itemFactory = new Neo4jItemFactory(this.neo4jClient, this.versionHistoryDAGFactory, tagFactory);
    this.richVersionFactory = new Neo4jRichVersionFactory(this.neo4jClient, (Neo4jStructureVersionFactory)
        this.factories.getStructureVersionFactory(), this.tagFactory);
  }

  @Before
  public void setup() throws IOException, InterruptedException {
    this.neo4jClient.dropData();
  }

  /**
   * Because we don't have simple versions, we're masking the creation of RichVersions by creating
   * empty NodeVersions. This is a bad hack that requires us creating a Node for each NodeVersion,
   * but tests right now are small. Eventually, if we mock a database, we can avoid this.
   *
   * @return A new, random NodeVersion's id.
   */
  protected long createNodeVersion(long nodeId) throws GroundException {
    return this.factories.getNodeVersionFactory().create(new HashMap<>(), -1, null, new HashMap<>(), nodeId, new ArrayList<>()).getId();
  }
}
