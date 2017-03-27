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

import edu.berkeley.ground.dao.models.GraphFactory;
import edu.berkeley.ground.dao.models.GraphVersionFactory;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.model.models.Graph;
import edu.berkeley.ground.model.models.GraphVersion;
import edu.berkeley.ground.model.models.Tag;

import io.swagger.annotations.Api;

import java.util.List;
import java.util.Map;

import javax.validation.Valid;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/graphs")
@Api(value = "/graphs", description = "Interact with the graphs in ground")
@Produces(MediaType.APPLICATION_JSON)
public class GraphsResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(GraphsResource.class);

  private GraphFactory graphFactory;
  private GraphVersionFactory graphVersionFactory;

  public GraphsResource(GraphFactory graphFactory, GraphVersionFactory graphVersionFactory) {
    this.graphFactory = graphFactory;
    this.graphVersionFactory = graphVersionFactory;
  }

  @GET
  @Timed
  @Path("/{name}")
  public Graph getGraph(@PathParam("name") String name) throws GroundException {
    LOGGER.info("Retrieving graph " + name + ".");
    return this.graphFactory.retrieveFromDatabase(name);
  }

  @GET
  @Timed
  @Path("/versions/{id}")
  public GraphVersion getGraphVersion(@PathParam("id") long id) throws GroundException {
    LOGGER.info("Retrieving graph version " + id + ".");
    return this.graphVersionFactory.retrieveFromDatabase(id);
  }

  @POST
  @Timed
  @Path("/{name}/{key}")
  public Graph createGraph(@PathParam("name") String name,
                           @PathParam("key") String sourceKey,
                           @Valid Map<String, Tag> tags) throws
      GroundException {
    LOGGER.info("Creating graph " + name + ".");
    return this.graphFactory.create(name, sourceKey, tags);
  }

  /**
   * Create a new graph version.
   *
   * @param graphVersion the data to create the graph version with
   * @param parentIds the ids of the parents of this version
   * @return the created version along with an id
   * @throws GroundException an error while creating the graph version
   */
  @POST
  @Timed
  @Path("/versions")
  public GraphVersion createGraphVersion(@Valid GraphVersion graphVersion,
                                         @QueryParam("parent") List<Long> parentIds)
      throws GroundException {

    LOGGER.info("Creating graph version in graph " + graphVersion.getGraphId() + ".");
    return this.graphVersionFactory.create(graphVersion.getTags(),
        graphVersion.getStructureVersionId(),
        graphVersion.getReference(),
        graphVersion.getParameters(),
        graphVersion.getGraphId(),
        graphVersion.getEdgeVersionIds(),
        parentIds);
  }
}
