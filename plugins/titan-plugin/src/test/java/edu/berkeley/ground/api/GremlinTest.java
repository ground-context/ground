package edu.berkeley.ground.api;

import org.junit.Before;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import edu.berkeley.ground.api.models.gremlin.GremlinRichVersionFactory;
import edu.berkeley.ground.api.models.gremlin.GremlinStructureVersionFactory;
import edu.berkeley.ground.api.models.gremlin.GremlinTagFactory;
import edu.berkeley.ground.api.versions.gremlin.GremlinItemFactory;
import edu.berkeley.ground.api.versions.gremlin.GremlinVersionHistoryDAGFactory;
import edu.berkeley.ground.api.versions.gremlin.GremlinVersionSuccessorFactory;
import edu.berkeley.ground.db.GremlinClient;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.util.GremlinFactories;

public class GremlinTest {
    /* Note: In Gremlin, we don't create explicit (Rich)Versions because all of the logic is wrapped in
     * FooVersions. We are using NodeVersions as stand-ins because they are the most simple kind of
     * Versions. */

  protected GremlinClient gremlinClient;
  protected GremlinFactories factories;
  protected GremlinVersionSuccessorFactory versionSuccessorFactory;
  protected GremlinVersionHistoryDAGFactory versionHistoryDAGFactory;
  protected GremlinItemFactory itemFactory;
  protected GremlinTagFactory tagFactory;
  protected GremlinRichVersionFactory richVersionFactory;

  public GremlinTest() throws GroundException {
    this.gremlinClient = new GremlinClient();
    this.factories = new GremlinFactories(this.gremlinClient);

    this.versionSuccessorFactory = new GremlinVersionSuccessorFactory();
    this.versionHistoryDAGFactory = new GremlinVersionHistoryDAGFactory(this.versionSuccessorFactory);
    this.itemFactory = new GremlinItemFactory(this.versionHistoryDAGFactory);
    this.tagFactory = new GremlinTagFactory();
    this.richVersionFactory = new GremlinRichVersionFactory(
        (GremlinStructureVersionFactory) factories.getStructureVersionFactory(), this.tagFactory);
  }

  @Before
  public void setup() throws IOException, InterruptedException {
    Process p = Runtime.getRuntime().exec(System.getenv("TITAN_HOME") + "/bin/gremlin.sh -e delete_gremlin.groovy",
        null, new File("scripts/gremlin/"));

    p.waitFor();
  }


  /**
   * Because we don't have simple versions, we're masking the creation of RichVersions with
   * NodeVersions. This is bad hack and should be removed once we can mock databases.
   *
   * @return A new, random NodeVersion's id.
   */
  protected String createNodeVersion(String nodeId) throws GroundException {
    return this.factories.getNodeVersionFactory().create(new HashMap<>(), null, null,
        new HashMap<>(), nodeId, new ArrayList<>()).getId();
  }
}
