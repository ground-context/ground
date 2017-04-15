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

package edu.berkeley.ground.resources;

import com.codahale.metrics.annotation.Timed;

import edu.berkeley.ground.dao.models.NodeFactory;
import edu.berkeley.ground.dao.models.NodeVersionFactory;
import edu.berkeley.ground.db.DbClient;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.model.models.Node;
import edu.berkeley.ground.model.models.NodeVersion;
import edu.berkeley.ground.model.models.Tag;

import io.swagger.annotations.Api;

import java.util.List;
import java.util.Map;

import javax.validation.Valid;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/nodes")
@Api(value = "/nodes", description = "Interact with the nodes in the node")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class NodesResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(NodesResource.class);

  private final NodeFactory nodeFactory;
  private final NodeVersionFactory nodeVersionFactory;
  private final DbClient dbClient;

  public NodesResource(NodeFactory nodeFactory,
                       NodeVersionFactory nodeVersionFactory,
                       DbClient dbClient) {
    this.nodeFactory = nodeFactory;
    this.nodeVersionFactory = nodeVersionFactory;
    this.dbClient = dbClient;
  }

  @GET
  @Timed
  @Path("/{name}")
  public Node getNode(@PathParam("sourceKey") String sourceKey) throws GroundException {
    try {
      LOGGER.info("Retrieving node " + sourceKey + ".");
      return this.nodeFactory.retrieveFromDatabase(sourceKey);
    } finally {
      this.dbClient.commit();
    }
  }

  @GET
  @Timed
  @Path("/versions/{id}")
  public NodeVersion getNodeVersion(@PathParam("id") long id) throws GroundException {
    try {
      LOGGER.info("Retrieving node version " + id + ".");
      return this.nodeVersionFactory.retrieveFromDatabase(id);
    } finally {
      this.dbClient.commit();
    }
  }

  @GET
  @Timed
  @Path("/{sourceKey}/latest")
  public List<Long> getLatestVersions(@PathParam("sourceKey") String sourceKey) throws GroundException {
    try {
      LOGGER.info("Retrieving the latest version of node " + sourceKey + ".");
      return this.nodeFactory.getLeaves(sourceKey);
    } finally {
      this.dbClient.commit();
    }
  }

  @POST
  @Timed
  @Path("/{name}/{key}")
  public Node createNode(@PathParam("name") String name,
                         @PathParam("key") String sourceKey,
                         @Valid Map<String, Tag> tags) throws GroundException {
    try {
      LOGGER.info("Creating node " + name + ".");
      Node created = this.nodeFactory.create(name, sourceKey, tags);

      this.dbClient.commit();
      return created;
    } catch (Exception e) {
      this.dbClient.abort();

      throw e;
    }
  }

  /**
   * Create a node version.
   *
   * @param nodeId the id of the node to create this version in
   * @param tags the version's tags
   * @param referenceParameters optional reference access parameters
   * @param structureVersionId the id of the structure version associated with this version
   * @param reference an optional reference
   * @param parentIds the ids of the parent(s) of this version
   * @return the newly created version along with an id
   * @throws GroundException an error while creating the version
   */
  @POST
  @Timed
  @Path("/{id}/versions")
  public NodeVersion createNodeVersion(@PathParam("id") long nodeId,
                                       @Valid Map<String, Tag> tags,
                                       @Valid Map<String, String> referenceParameters,
                                       long structureVersionId,
                                       String reference,
                                       @QueryParam("parents") List<Long> parentIds)
      throws GroundException {
    try {
      LOGGER.info("Creating node version in node " + nodeId + ".");
      NodeVersion created = this.nodeVersionFactory.create(tags,
          structureVersionId,
          reference,
          referenceParameters,
          nodeId,
          parentIds);

      this.dbClient.commit();

      return created;
    } catch (Exception e) {
      this.dbClient.abort();

      throw e;
    }
  }

  /**
   * Truncate a node's history to be of a certain height, only keeping the most recent levels.
   *
   * @param name the name of the node to truncate
   * @param height the number of levels to keep
   * @throws GroundException an error while truncating this node
   */
  @POST
  @Timed
  @Path("/truncate/{name}/{height}")
  public void truncateNode(@PathParam("name") String name, @PathParam("height") int height)
      throws GroundException {
    try {
      LOGGER.info("Truncating node " + name + " to height " + height + ".");
      long id = this.nodeFactory.retrieveFromDatabase(name).getId();

      this.nodeFactory.truncate(id, height);
      this.dbClient.commit();
    } catch (Exception e) {
      this.dbClient.abort();

      throw e;
    }
  }
}
