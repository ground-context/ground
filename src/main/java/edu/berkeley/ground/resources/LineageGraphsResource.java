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

import edu.berkeley.ground.dao.usage.LineageGraphFactory;
import edu.berkeley.ground.dao.usage.LineageGraphVersionFactory;
import edu.berkeley.ground.db.DbClient;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.model.models.Tag;
import edu.berkeley.ground.model.usage.LineageGraph;
import edu.berkeley.ground.model.usage.LineageGraphVersion;

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

@Path("/lineage_graph")
@Api(value = "/lineage_graph", description = "Interact with lineage edges")
@Produces(MediaType.APPLICATION_JSON)
public class LineageGraphsResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(GraphsResource.class);

  private final LineageGraphFactory lineageGraphFactory;
  private final LineageGraphVersionFactory lineageGraphVersionFactory;
  private final DbClient dbClient;

  public LineageGraphsResource(
      LineageGraphFactory lineageGraphFactory,
      LineageGraphVersionFactory lineageGraphVersionFactory,
      DbClient dbClient) {
    this.lineageGraphFactory = lineageGraphFactory;
    this.lineageGraphVersionFactory = lineageGraphVersionFactory;
    this.dbClient = dbClient;
  }

  @GET
  @Timed
  @Path("/{name}")
  public LineageGraph getLineageGraph(@PathParam("sourceKey") String sourceKey)
      throws GroundException {
    try {
      LOGGER.info("Retrieving graph " + sourceKey + ".");
      return this.lineageGraphFactory.retrieveFromDatabase(sourceKey);
    } finally {
      this.dbClient.commit();
    }
  }

  @GET
  @Timed
  @Path("/versions/{id}")
  public LineageGraphVersion getLineageGraphVersion(@PathParam("id") long id)
      throws GroundException {
    try {
      LOGGER.info("Retrieving graph version " + id + ".");
      return this.lineageGraphVersionFactory.retrieveFromDatabase(id);
    } finally {
      this.dbClient.abort();
    }
  }

  @POST
  @Timed
  @Path("/{name}/{key}")
  public LineageGraph createLineageGraph(@PathParam("name") String name,
                                         @PathParam("key") String sourceKey,
                                         @Valid Map<String, Tag> tags) throws GroundException {
    try {
      LOGGER.info("Creating graph " + name + ".");
      LineageGraph created = this.lineageGraphFactory.create(name, sourceKey, tags);

      this.dbClient.commit();
      return created;
    } catch (Exception e) {
      this.dbClient.abort();

      throw e;
    }
  }

  /**
   *
   * @param lineageGraphVersion the data to create the version with
   */

  /**
   * Create a new linea graph version.
   *
   * @param lineageGraphId the id of the lineage graph to create this version in
   * @param tags the version's tags
   * @param referenceParameters optional reference access parameters
   * @param structureVersionId the id of the structure version associated with this version
   * @param reference an optional reference
   * @param lineageEdgeVersionIds the ids of the lineage edge versions in this graph
   * @param parentIds the ids of the parent(s) of this version
   * @return the newly created version along with an id
   * @throws GroundException an error while creating the version
   */
  @POST
  @Timed
  @Path("/{id}/versions")
  public LineageGraphVersion createLineageGraphVersion(
      @PathParam("id") long lineageGraphId,
      @Valid Map<String, Tag> tags,
      @Valid Map<String, String> referenceParameters,
      long structureVersionId,
      String reference,
      @Valid List<Long> lineageEdgeVersionIds,
      @QueryParam("parent") List<Long> parentIds) throws GroundException {

    try {
      LOGGER.info("Creating graph version in graph " + lineageGraphId + ".");
      LineageGraphVersion created = this.lineageGraphVersionFactory.create(tags,
          structureVersionId,
          reference,
          referenceParameters,
          lineageGraphId,
          lineageEdgeVersionIds,
          parentIds);

      this.dbClient.commit();
      return created;
    } catch (Exception e) {
      this.dbClient.abort();

      throw e;
    }
  }

  /**
   * Truncate a lineage graph's history to be of a certain height, only keeping the most recent
   * levels.
   *
   * @param name the name of the lineage graph to truncate
   * @param height the number of levels to keep
   * @throws GroundException an error while truncating this lineage graph
   */
  @POST
  @Timed
  @Path("/truncate/{name}/{height}")
  public void truncateLineageGraph(@PathParam("name") String name, @PathParam("height") int height)
      throws GroundException {
    try {
      LOGGER.info("Truncating lineage graph " + name + " to height " + height + ".");
      long id = this.lineageGraphFactory.retrieveFromDatabase(name).getId();

      this.lineageGraphFactory.truncate(id, height);
      this.dbClient.commit();
    } catch (Exception e) {
      this.dbClient.abort();

      throw e;
    }
  }
}
