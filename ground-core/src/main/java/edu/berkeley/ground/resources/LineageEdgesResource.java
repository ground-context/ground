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

import edu.berkeley.ground.api.usage.LineageEdge;
import edu.berkeley.ground.api.usage.LineageEdgeFactory;
import edu.berkeley.ground.api.usage.LineageEdgeVersion;
import edu.berkeley.ground.api.usage.LineageEdgeVersionFactory;
import edu.berkeley.ground.exceptions.GroundException;
import io.swagger.annotations.Api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.Valid;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

import java.util.List;

@Path("/lineage")
@Api(value = "/lineage", description = "Interact with lineage edges")
@Produces(MediaType.APPLICATION_JSON)
public class LineageEdgesResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(LineageEdgesResource.class);

  private LineageEdgeFactory lineageEdgeFactory;
  private LineageEdgeVersionFactory lineageEdgeVersionFactory;

  public LineageEdgesResource(LineageEdgeFactory lineageEdgeFactory, LineageEdgeVersionFactory lineageEdgeVersionFactory) {
    this.lineageEdgeFactory = lineageEdgeFactory;
    this.lineageEdgeVersionFactory = lineageEdgeVersionFactory;
  }

  @GET
  @Timed
  @Path("/{name}")
  public LineageEdge getLineageEdge(@PathParam("name") String name) throws GroundException {
    LOGGER.info("Retrieving lineage edge " + name + ".");
    return this.lineageEdgeFactory.retrieveFromDatabase(name);
  }

  @GET
  @Timed
  @Path("/versions/{id}")
  public LineageEdgeVersion getLineageEdgeVersion(@PathParam("id") long id) throws GroundException {
    LOGGER.info("Retrieving lineage edge version " + id + ".");
    return this.lineageEdgeVersionFactory.retrieveFromDatabase(id);
  }

  @POST
  @Timed
  @Path("/{name}")
  public LineageEdge createLineageEdge(@PathParam("name") String name) throws GroundException {
    LOGGER.info("Creating lineage edge " + name + ".");
    return this.lineageEdgeFactory.create(name);
  }

  @POST
  @Timed
  @Path("/versions")
  public LineageEdgeVersion createLineageEdgeVersion(@Valid LineageEdgeVersion lineageEdgeVersion, @QueryParam("parent") List<Long> parentIds) throws GroundException {
    LOGGER.info("Creating lineage edge version in lineage edge " + lineageEdgeVersion.getLineageEdgeId() + ".");
    return this.lineageEdgeVersionFactory.create(lineageEdgeVersion.getTags(),
        lineageEdgeVersion.getStructureVersionId(),
        lineageEdgeVersion.getReference(),
        lineageEdgeVersion.getParameters(),
        lineageEdgeVersion.getFromId(),
        lineageEdgeVersion.getToId(),
        lineageEdgeVersion.getLineageEdgeId(),
        parentIds);
  }
}
