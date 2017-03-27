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
@Api(value = "/nodes", description = "Interact with the nodes in the graph")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class NodesResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(NodesResource.class);

  private NodeFactory nodeFactory;
  private NodeVersionFactory nodeVersionFactory;

  public NodesResource(NodeFactory nodeFactory, NodeVersionFactory nodeVersionFactory) {
    this.nodeFactory = nodeFactory;
    this.nodeVersionFactory = nodeVersionFactory;
  }

  @GET
  @Timed
  @Path("/{name}")
  public Node getNode(@PathParam("name") String name) throws GroundException {
    LOGGER.info("Retrieving node " + name + ".");
    return this.nodeFactory.retrieveFromDatabase(name);
  }

  @GET
  @Timed
  @Path("/versions/{id}")
  public NodeVersion getNodeVersion(@PathParam("id") long id) throws GroundException {
    LOGGER.info("Retrieving node version " + id + ".");
    return this.nodeVersionFactory.retrieveFromDatabase(id);
  }

  @POST
  @Timed
  @Path("/{name}/{key}")
  public Node createNode(@PathParam("name") String name,
                         @PathParam("key") String sourceKey,
                         @Valid Map<String, Tag> tags) throws GroundException {
    LOGGER.info("Creating node " + name + ".");
    return this.nodeFactory.create(name, sourceKey, tags);
  }

  /**
   * Create a node version.
   *
   * @param nodeVersion the data to create the version with
   * @param parentIds the ids of the parent(s) of this version
   * @return the newly created version along with an id
   * @throws GroundException an error while creating the version
   */
  @POST
  @Timed
  @Path("/versions")
  public NodeVersion createNodeVersion(@Valid NodeVersion nodeVersion,
                                       @QueryParam("parents") List<Long> parentIds)
      throws GroundException {
    LOGGER.info("Creating node version in node " + nodeVersion.getNodeId() + ".");
    return this.nodeVersionFactory.create(nodeVersion.getTags(),
        nodeVersion.getStructureVersionId(),
        nodeVersion.getReference(),
        nodeVersion.getParameters(),
        nodeVersion.getNodeId(),
        parentIds);
  }

  @GET
  @Timed
  @Path("/{name}/latest")
  public List<Long> getLatestVersions(@PathParam("name") String name) throws GroundException {
    LOGGER.info("Retrieving the latest version of node " + name + ".");
    return this.nodeFactory.getLeaves(name);
  }

  /**
   * Return the nodes adjacent to this one, filtered by the edge name.
   *
   * @param nodeVersionId the source id of the query
   * @param edgeNameRegex the edge name to filter by
   * @return the list of adjacent version
   * @throws GroundException the version doesn't exist or the query couldn't be run
   */
  @GET
  @Timed
  @Path("/adjacent/{id}/{edgeName}")
  public List<Long> adjacentNodes(@PathParam("id") long nodeVersionId,
                                  @PathParam("edgeName") String edgeNameRegex)
      throws GroundException {
    LOGGER.info("Retrieving adjacent nodes to node version  " + nodeVersionId + ".");

    return this.nodeVersionFactory.getAdjacentNodes(nodeVersionId, edgeNameRegex);
  }
}
