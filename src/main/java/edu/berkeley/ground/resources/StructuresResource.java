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
import edu.berkeley.ground.dao.models.StructureFactory;
import edu.berkeley.ground.dao.models.StructureVersionFactory;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.model.models.Structure;
import edu.berkeley.ground.model.models.StructureVersion;
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

@Path("/structures")
@Api(value = "/structures", description = "Interact with the structures in the structure")
@Produces(MediaType.APPLICATION_JSON)
public class StructuresResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(StructuresResource.class);

  private final StructureFactory structureFactory;
  private final StructureVersionFactory structureVersionFactory;

  public StructuresResource(StructureFactory structureFactory,
                            StructureVersionFactory structureVersionFactory) {
    this.structureFactory = structureFactory;
    this.structureVersionFactory = structureVersionFactory;
  }

  @GET
  @Timed
  @Path("/{name}")
  public Structure getStructure(@PathParam("name") String name) throws GroundException {
    LOGGER.info("Retrieving structure " + name + ".");
    return this.structureFactory.retrieveFromDatabase(name);
  }

  @GET
  @Timed
  @Path("/versions/{id}")
  public StructureVersion getStructureVersion(@PathParam("id") long id) throws GroundException {
    LOGGER.info("Retrieving structure version " + id + ".");
    return this.structureVersionFactory.retrieveFromDatabase(id);
  }

  @POST
  @Timed
  @Path("/{name}/{key}")
  public Structure createStructure(@PathParam("name") String name,
                                   @PathParam("key") String sourceKey,
                                   @Valid Map<String, Tag> tags) throws GroundException {
    LOGGER.info("Creating structure " + name + ".");
    return this.structureFactory.create(name, sourceKey, tags);
  }

  /**
   * Create a structure version.
   *
   * @param structureVersion the data to create the version with
   * @param parentIds the ids of the parent(s) of this version
   * @return the newly created version along with an id
   * @throws GroundException an error while creating the version
   */
  @POST
  @Timed
  @Path("/versions")
  public StructureVersion createStructureVersion(@Valid StructureVersion structureVersion,
                                                 @QueryParam("parent") List<Long> parentIds)
      throws GroundException {

    LOGGER.info("Creating structure version in structure "
        + structureVersion.getStructureId()
        + ".");

    return this.structureVersionFactory.create(structureVersion.getStructureId(),
        structureVersion.getAttributes(),
        parentIds);
  }

  @GET
  @Timed
  @Path("/{name}/latest")
  public List<Long> getLatestVersions(@PathParam("name") String name) throws GroundException {
    LOGGER.info("Retrieving the latest version of node " + name + ".");
    return this.structureFactory.getLeaves(name);
  }

  /**
   * Truncate a structure's history to be of a certain height, only keeping the most recent levels.
   *
   * @param name the name of the structure to truncate
   * @param height the number of levels to keep
   * @throws GroundException an error while truncating this structure
   */
  @POST
  @Timed
  @Path("/truncate/{name}/{height}")
  public void truncateEdge(@PathParam("name") String name, @PathParam("height") int height)
      throws GroundException {
    LOGGER.info("Truncating structure " + name + " to height " + height + ".");

    long id = this.structureFactory.retrieveFromDatabase(name).getId();

    this.structureFactory.truncate(id, height);
  }
}
