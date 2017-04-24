package edu.berkeley.ground.resources;

import com.codahale.metrics.annotation.Timed;
import edu.berkeley.ground.dao.models.TagFactory;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.model.models.Tag;
import io.swagger.annotations.Api;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.List;
import java.util.Map;


@Path("/tags")
@Api(value = "/tags", description = "Interact with tags")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class TagsResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(NodesResource.class);

  private final TagFactory tagFactory;
  public TagsResource(TagFactory tagFactory) {
    this.tagFactory = tagFactory;

  }

  @GET
  @Timed
  @Path("/versions/{id}")
  public Map<String, Tag> retrieveFromDatabaseByVersionId(@PathParam("id") long id) throws GroundException {
    LOGGER.info("Retrieving version with id: " + id + ".");
    return this.tagFactory.retrieveFromDatabaseByVersionId(id);
  }

  @GET
  @Timed
  @Path("/items/{id}")
  public Map<String, Tag> retrieveFromDatabaseByItemId(@PathParam("id") long id) throws GroundException {
    LOGGER.info("Retrieving item with id: " + id + ".");
    return this.tagFactory.retrieveFromDatabaseByItemId(id);
  }

  @GET
  @Timed
  @Path("/versions/{name}/{elasticSearchOn}")
  public List<Long> getVersionIdsByTag(@PathParam("name") String tag, @PathParam("elasticSearchOn") boolean elasticSearchOn) throws GroundException {
    LOGGER.info("Retrieving all version ids with tag: " + tag + ".");
    return this.tagFactory.getVersionIdsByTag(tag, elasticSearchOn);
  }

  @GET
  @Timed
  @Path("/items/{name}")
  public List<Long> getItemIdsByTag(@PathParam("name") String tag, @PathParam("elasticSearchOn") boolean elasticSearchOn) throws GroundException {
    LOGGER.info("Retrieving all item ids with tag: " + tag + ".");
    return this.tagFactory.getItemIdsByTag(tag, elasticSearchOn);
  }
}
