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

import edu.berkeley.ground.dao.usage.LineageEdgeFactory;
import edu.berkeley.ground.dao.usage.LineageEdgeVersionFactory;
import edu.berkeley.ground.db.DbClient;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.model.models.Tag;
import edu.berkeley.ground.model.usage.LineageEdge;
import edu.berkeley.ground.model.usage.LineageEdgeVersion;

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

@Path("/lineage")
@Api(value = "/lineage", description = "Interact with lineage edges")
@Produces(MediaType.APPLICATION_JSON)
public class LineageEdgesResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(LineageEdgesResource.class);

  private final LineageEdgeFactory lineageEdgeFactory;
  private final LineageEdgeVersionFactory lineageEdgeVersionFactory;
  private final DbClient dbClient;

  public LineageEdgesResource(LineageEdgeFactory lineageEdgeFactory,
                              LineageEdgeVersionFactory lineageEdgeVersionFactory,
                              DbClient dbClient) {
    this.lineageEdgeFactory = lineageEdgeFactory;
    this.lineageEdgeVersionFactory = lineageEdgeVersionFactory;
    this.dbClient = dbClient;
  }

  @GET
  @Timed
  @Path("/{name}")
  public LineageEdge getLineageEdge(@PathParam("sourceKey") String sourceKey)
      throws GroundException {
    try {
      LOGGER.info("Retrieving lineage edge " + sourceKey + ".");
      return this.lineageEdgeFactory.retrieveFromDatabase(sourceKey);
    } finally {
      this.dbClient.commit();
    }
  }

  @GET
  @Timed
  @Path("/versions/{id}")
  public LineageEdgeVersion getLineageEdgeVersion(@PathParam("id") long id) throws GroundException {
    try {
      LOGGER.info("Retrieving lineage edge version " + id + ".");
      return this.lineageEdgeVersionFactory.retrieveFromDatabase(id);
    } finally {
      this.dbClient.commit();
    }
  }

  @POST
  @Timed
  @Path("/{name}/{key}")
  public LineageEdge createLineageEdge(@PathParam("name") String name,
                                       @PathParam("key") String sourceKey,
                                       @Valid Map<String, Tag> tags) throws GroundException {
    try {
      LOGGER.info("Creating lineage edge " + name + ".");
      LineageEdge lineageEdge = this.lineageEdgeFactory.create(name, sourceKey, tags);

      this.dbClient.commit();
      return lineageEdge;
    } catch (Exception e){
      this.dbClient.abort();

      throw e;
    }
  }

  /**
   *
   * @param lineageEdgeVersion the data to create the version with
   */

  /**
   * Create a lineage edge version.
   *
   * @param lineageEdgeId the id of the lineage edge to create this version in
   * @param tags the version's tags
   * @param referenceParameters optional reference access parameters
   * @param structureVersionId the id of the structure version associated with this version
   * @param reference an optional reference
   * @param fromId the source version of this edge
   * @param toId the destination version of this edge
   * @param parentIds the ids of the parent(s) of this version
   * @return the newly created version along with an id
   * @throws GroundException an error while creating the version
   */
  @POST
  @Timed
  @Path("/{id}/versions")
  public LineageEdgeVersion createLineageEdgeVersion(@PathParam("id") long lineageEdgeId,
                                                     @Valid Map<String, Tag> tags,
                                                     @Valid Map<String, String> referenceParameters,
                                                     long structureVersionId,
                                                     String reference,
                                                     long fromId,
                                                     long toId,
                                                     @QueryParam("parent") List<Long> parentIds)
      throws GroundException {

    try {
      LOGGER.info("Creating lineage edge version in lineage edge "
          + lineageEdgeId + ".");

      LineageEdgeVersion created = this.lineageEdgeVersionFactory.create(tags,
          structureVersionId,
          reference,
          referenceParameters,
          fromId,
          toId,
          lineageEdgeId,
          parentIds);

      this.dbClient.commit();
      return created;
    } catch (Exception e) {
      this.dbClient.abort();

      throw e;
    }
  }

  /**
   * Truncate a lineage edge's history to be of a certain height, only keeping the most recent
   * levels.
   *
   * @param name the name of the lineage edge to truncate
   * @param height the number of levels to keep
   * @throws GroundException an error while truncating this lineage edge
   */
  @POST
  @Timed
  @Path("/truncate/{name}/{height}")
  public void truncateLineageEdge(@PathParam("name") String name, @PathParam("height") int height)
      throws GroundException {
    try {
      LOGGER.info("Truncating lineage edge " + name + " to height " + height + ".");
      long id = this.lineageEdgeFactory.retrieveFromDatabase(name).getId();

      this.lineageEdgeFactory.truncate(id, height);
      this.dbClient.commit();
    } catch (Exception e) {
      this.dbClient.abort();

      throw e;
    }
  }
}
