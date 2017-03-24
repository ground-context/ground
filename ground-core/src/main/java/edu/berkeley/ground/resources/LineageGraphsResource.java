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

import javax.validation.Valid;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import edu.berkeley.ground.model.models.Tag;
import edu.berkeley.ground.model.usage.LineageGraph;
import edu.berkeley.ground.dao.usage.LineageGraphFactory;
import edu.berkeley.ground.model.usage.LineageGraphVersion;
import edu.berkeley.ground.dao.usage.LineageGraphVersionFactory;
import edu.berkeley.ground.exceptions.GroundException;
import io.swagger.annotations.Api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

@Path("/lineage_graph")
@Api(value = "/lineage_graph", description = "Interact with lineage edges")
@Produces(MediaType.APPLICATION_JSON)
public class LineageGraphsResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(GraphsResource.class);

  private LineageGraphFactory lineageGraphFactory;
  private LineageGraphVersionFactory lineageGraphVersionFactory;

  public LineageGraphsResource(
      LineageGraphFactory lineageGraphFactory,
      LineageGraphVersionFactory lineageGraphVersionFactory) {
    this.lineageGraphFactory = lineageGraphFactory;
    this.lineageGraphVersionFactory = lineageGraphVersionFactory;
  }

  @GET
  @Timed
  @Path("/{name}")
  public LineageGraph getLineageGraph(@PathParam("name") String name) throws GroundException {
    LOGGER.info("Retrieving graph " + name + ".");
    return this.lineageGraphFactory.retrieveFromDatabase(name);
  }

  @GET
  @Timed
  @Path("/versions/{id}")
  public LineageGraphVersion getLineageGraphVersion(@PathParam("id") long id)
      throws GroundException {
    LOGGER.info("Retrieving graph version " + id + ".");
    return this.lineageGraphVersionFactory.retrieveFromDatabase(id);
  }

  @POST
  @Timed
  @Path("/{name}/{key}")
  public LineageGraph createGraph(@PathParam("name") String name,
                                  @PathParam("key") String sourceKey,
                                  @Valid Map<String, Tag> tags) throws GroundException {
    LOGGER.info("Creating graph " + name + ".");
    return this.lineageGraphFactory.create(name, sourceKey, tags);
  }

  @POST
  @Timed
  @Path("/versions")
  public LineageGraphVersion createGraphVersion(@Valid LineageGraphVersion lineageGraphVersion,
                                                @QueryParam("parent") List<Long> parentIds)
      throws GroundException {
    LOGGER.info("Creating graph version in graph " + lineageGraphVersion.getLineageGraphId() + ".");
    return this.lineageGraphVersionFactory.create(lineageGraphVersion.getTags(),
        lineageGraphVersion.getStructureVersionId(),
        lineageGraphVersion.getReference(),
        lineageGraphVersion.getParameters(),
        lineageGraphVersion.getLineageGraphId(),
        lineageGraphVersion.getLineageEdgeVersionIds(),
        parentIds);
  }
}
