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

package edu.berkeley.ground.resources;

import com.codahale.metrics.annotation.Timed;

import edu.berkeley.ground.api.models.Node;
import edu.berkeley.ground.api.models.NodeFactory;
import edu.berkeley.ground.api.models.NodeVersion;
import edu.berkeley.ground.api.models.NodeVersionFactory;
import edu.berkeley.ground.exceptions.GroundException;
import io.swagger.annotations.Api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.Valid;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

import java.util.List;

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
  @Path("/{name}")
  public Node createNode(@PathParam("name") String name) throws GroundException {
    LOGGER.info("Creating node " + name + ".");
    return this.nodeFactory.create(name);
  }

  @POST
  @Timed
  @Path("/versions")
  public NodeVersion createNodeVersion(@Valid NodeVersion nodeVersion, @QueryParam("parents") List<Long> parentIds) throws GroundException {
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

  @GET
  @Timed
  @Path("/closure/{id}")
  public List<Long> transitiveClosure(@PathParam("id") long nodeVersionId) throws GroundException {
    LOGGER.info("Running transitive closure on node version  " + nodeVersionId + ".");

    return this.nodeVersionFactory.getTransitiveClosure(nodeVersionId);
  }

  @GET
  @Timed
  @Path("/adjacent/{id}/{edgeName}")
  public List<Long> adjacentNodes(@PathParam("id") long nodeVersionId, @PathParam("edgeName") String edgeNameRegex) throws GroundException {
    LOGGER.info("Retrieving adjancent nodes to node version  " + nodeVersionId + ".");

    return this.nodeVersionFactory.getAdjacentNodes(nodeVersionId, edgeNameRegex);
  }
}
