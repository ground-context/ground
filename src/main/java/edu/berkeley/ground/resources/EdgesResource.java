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

import edu.berkeley.ground.dao.models.EdgeFactory;
import edu.berkeley.ground.dao.models.EdgeVersionFactory;
import edu.berkeley.ground.dao.models.NodeFactory;
import edu.berkeley.ground.db.DbClient;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.model.models.Edge;
import edu.berkeley.ground.model.models.EdgeVersion;
import edu.berkeley.ground.model.models.Node;
import edu.berkeley.ground.model.models.Tag;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

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


@Path("/edges")
@Api(value = "/edges", description = "Interact with the edges in the graph")
@Produces(MediaType.APPLICATION_JSON)
public class EdgesResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(EdgesResource.class);

  private final EdgeFactory edgeFactory;
  private final EdgeVersionFactory edgeVersionFactory;
  private final DbClient dbClient;

  private final NodeFactory nodeFactory;

  /**
   * Constructor for EdgesResource.
   *
   * @param edgeFactory the database edge factory
   * @param edgeVersionFactory the database edge version factory
   * @param nodeFactory the database node factory
   */
  public EdgesResource(EdgeFactory edgeFactory,
                       EdgeVersionFactory edgeVersionFactory,
                       NodeFactory nodeFactory,
                       DbClient dbClient) {
    this.edgeFactory = edgeFactory;
    this.edgeVersionFactory = edgeVersionFactory;
    this.nodeFactory = nodeFactory;
    this.dbClient = dbClient;
  }

  @GET
  @Timed
  @ApiOperation(value = "Get an edge")
  @Path("/{sourceKey}/{key}")
  public Edge getEdge(@PathParam("sourceKey") String sourceKey) throws GroundException {
    try {
      LOGGER.info("Retrieving edge " + sourceKey + ".");
      return this.edgeFactory.retrieveFromDatabase(sourceKey);
    } finally {
      this.dbClient.commit();
    }
  }

  @GET
  @Timed
  @Path("/versions/{id}")
  public EdgeVersion getEdgeVersion(@PathParam("id") long id) throws GroundException {
    try {
      LOGGER.info("Retrieving edge version " + id + ".");
      return this.edgeVersionFactory.retrieveFromDatabase(id);
    } finally {
      this.dbClient.commit();
    }
  }

  /**
   * Create a new edge.
   *
   * @param name the name of the edge
   * @param fromNodeName the name of the source node
   * @param toNodeName the name of the destination node
   * @param sourceKey the user-generated unique key for this edge
   * @param tags the tags associated with this edge
   * @return the created edge
   * @throws GroundException an error while creating this edge
   */
  @POST
  @Timed
  @Path("/{name}")
  public Edge createEdge(@PathParam("name") String name,
                         @PathParam("fromNodeName") String fromNodeName,
                         @PathParam("toNodeName") String toNodeName,
                         @PathParam("key") String sourceKey,
                         @Valid Map<String, Tag> tags)
      throws GroundException {
    try {
      LOGGER.info("Creating edge " + name + ".");

      Node fromNode = this.nodeFactory.retrieveFromDatabase(fromNodeName);
      Node toNode = this.nodeFactory.retrieveFromDatabase(toNodeName);

      Edge edge = this.edgeFactory.create(name, sourceKey, fromNode.getId(), toNode.getId(), tags);
      this.dbClient.commit();

      return edge;
    } catch (Exception e) {
      this.dbClient.abort();

      throw e;
    }
  }

  /**
   * Create a new edge version.
   *
   * @param edgeId the id of the edge in which we're creating a version
   * @param tags the version's tags
   * @param referenceParameters optional reference access parameters
   * @param structureVersionId the id of the structure version associated with this version
   * @param reference an optional reference
   * @param fromNodeVersionStartId the start version in the from node
   * @param fromNodeVersionEndId the end version in the from node
   * @param toNodeVersionStartId the start version in the to node
   * @param toNodeVersionEndId the end version in the to node
   * @param parentIds the id of the parents of this version
   * @return the created edge version
   * @throws GroundException an error while creating the version
   */
  @POST
  @Timed
  @Path("/{id}/versions")
  public EdgeVersion createEdgeVersion(@PathParam("id") long edgeId,
                                       @Valid Map<String, Tag> tags,
                                       @Valid Map<String, String> referenceParameters,
                                       long structureVersionId,
                                       String reference,
                                       long fromNodeVersionStartId,
                                       long fromNodeVersionEndId,
                                       long toNodeVersionStartId,
                                       long toNodeVersionEndId,
                                       @QueryParam("parent") List<Long> parentIds)
      throws GroundException {

    try {
      LOGGER.info("Creating edge version in edge " + edgeId + ".");
      EdgeVersion created = this.edgeVersionFactory.create(tags,
          structureVersionId,
          reference,
          referenceParameters,
          edgeId,
          fromNodeVersionStartId,
          fromNodeVersionEndId,
          toNodeVersionStartId,
          toNodeVersionEndId,
          parentIds);

      this.dbClient.commit();

      return created;
    } catch (Exception e) {
      this.dbClient.abort();

      throw e;
    }
  }

  /**
   * Truncate an edge's history to be of a certain height, only keeping the most recent levels.
   *
   * @param sourceKey the name of the edge to truncate
   * @param height the number of levels to keep
   * @throws GroundException an error while truncating this edge
   */
  @POST
  @Timed
  @Path("/truncate/{sourceKey}/{height}")
  public void truncateEdge(@PathParam("sourceKey") String sourceKey, @PathParam("height") int height)
      throws GroundException {

    try {
      LOGGER.info("Truncating edge " + sourceKey + " to height " + height + ".");

      long id = this.edgeFactory.retrieveFromDatabase(sourceKey).getId();
      this.edgeFactory.truncate(id, height);
      this.dbClient.commit();

    } catch (Exception e) {
      this.dbClient.abort();

      throw e;
    }
  }
}
