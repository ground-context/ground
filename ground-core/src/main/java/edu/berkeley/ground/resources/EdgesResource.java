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

import edu.berkeley.ground.model.models.Edge;
import edu.berkeley.ground.dao.models.EdgeFactory;
import edu.berkeley.ground.model.models.EdgeVersion;
import edu.berkeley.ground.dao.models.EdgeVersionFactory;
import edu.berkeley.ground.model.models.Node;
import edu.berkeley.ground.dao.models.NodeFactory;
import edu.berkeley.ground.model.models.Tag;
import edu.berkeley.ground.exceptions.GroundException;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.Valid;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

import java.util.List;
import java.util.Map;

@Path("/edges")
@Api(value = "/edges", description = "Interact with the edges in the graph")
@Produces(MediaType.APPLICATION_JSON)
public class EdgesResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(EdgesResource.class);

  private EdgeFactory edgeFactory;
  private EdgeVersionFactory edgeVersionFactory;

  private NodeFactory nodeFactory;

  public EdgesResource(EdgeFactory edgeFactory,
                       EdgeVersionFactory edgeVersionFactory,
                       NodeFactory nodeFactory) {
    this.edgeFactory = edgeFactory;
    this.edgeVersionFactory = edgeVersionFactory;
    this.nodeFactory = nodeFactory;
  }

  @GET
  @Timed
  @ApiOperation(value = "Get an edge")
  @Path("/{name}/{key}")
  public Edge getEdge(@PathParam("name") String name) throws GroundException {
    LOGGER.info("Retrieving edge " + name + ".");
    return this.edgeFactory.retrieveFromDatabase(name);
  }

  @GET
  @Timed
  @Path("/versions/{id}")
  public EdgeVersion getEdgeVersion(@PathParam("id") long id) throws GroundException {
    LOGGER.info("Retrieving edge version " + id + ".");
    return this.edgeVersionFactory.retrieveFromDatabase(id);
  }

  @POST
  @Timed
  @Path("/{name}")
  public Edge createEdge(@PathParam("name") String name,
                         @PathParam("fromNodeName") String fromNodeName,
                         @PathParam("toNodeName") String toNodeName,
                         @PathParam("key") String sourceKey,
                         @Valid Map<String, Tag> tags)
      throws GroundException {
    LOGGER.info("Creating edge " + name + ".");

    Node fromNode = this.nodeFactory.retrieveFromDatabase(fromNodeName);
    Node toNode = this.nodeFactory.retrieveFromDatabase(toNodeName);

    return this.edgeFactory.create(name, sourceKey, fromNode.getId(), toNode.getId(), tags);
  }

  @POST
  @Timed
  @Path("/versions")
  public EdgeVersion createEdgeVersion(@Valid EdgeVersion edgeVersion, @QueryParam("parent") List<Long> parentIds) throws GroundException {
    LOGGER.info("Creating edge version in edge " + edgeVersion.getEdgeId() + ".");
    return this.edgeVersionFactory.create(edgeVersion.getTags(),
        edgeVersion.getStructureVersionId(),
        edgeVersion.getReference(),
        edgeVersion.getParameters(),
        edgeVersion.getEdgeId(),
        edgeVersion.getFromNodeVersionStartId(),
        edgeVersion.getFromNodeVersionEndId(),
        edgeVersion.getToNodeVersionStartId(),
        edgeVersion.getToNodeVersionEndId(),
        parentIds);
  }
}
