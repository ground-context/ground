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

import edu.berkeley.ground.api.models.Edge;
import edu.berkeley.ground.api.models.EdgeFactory;
import edu.berkeley.ground.api.models.EdgeVersion;
import edu.berkeley.ground.api.models.EdgeVersionFactory;
import edu.berkeley.ground.exceptions.GroundException;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.Valid;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

import java.util.List;

@Path("/edges")
@Api(value = "/edges", description = "Interact with the edges in the graph")
@Produces(MediaType.APPLICATION_JSON)
public class EdgesResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(EdgesResource.class);

  private EdgeFactory edgeFactory;
  private EdgeVersionFactory edgeVersionFactory;

  public EdgesResource(EdgeFactory edgeFactory, EdgeVersionFactory edgeVersionFactory) {
    this.edgeFactory = edgeFactory;
    this.edgeVersionFactory = edgeVersionFactory;
  }

  @GET
  @Timed
  @ApiOperation(value = "Get an edge")
  @Path("/{name}")
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
  public Edge createEdge(@PathParam("name") String name) throws GroundException {
    LOGGER.info("Creating edge " + name + ".");
    return this.edgeFactory.create(name);
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
        edgeVersion.getFromId(),
        edgeVersion.getToId(),
        parentIds);
  }
}
