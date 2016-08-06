package edu.berkeley.ground.resources;

import com.codahale.metrics.annotation.Timed;
import edu.berkeley.ground.api.models.Graph;
import edu.berkeley.ground.api.models.GraphFactory;
import edu.berkeley.ground.api.models.GraphVersion;
import edu.berkeley.ground.api.models.GraphVersionFactory;
import edu.berkeley.ground.exceptions.GroundException;
import io.dropwizard.jersey.params.NonEmptyStringParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.Valid;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.List;

@Path("/graphs")
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
    public GraphVersion getGraphVersion(@PathParam("id") String id) throws GroundException {
        LOGGER.info("Retrieving graph version " + id + ".");
        return this.graphVersionFactory.retrieveFromDatabase(id);
    }

    @POST
    @Timed
    @Path("/{name}")
    public Graph createGraph(@PathParam("name") String name) throws GroundException {
        LOGGER.info("Creating graph " + name + ".");
        return this.graphFactory.create(name);
    }

    @POST
    @Timed
    @Path("/versions")
    public GraphVersion createGraphVersion(@Valid GraphVersion graphVersion, @QueryParam("parent") List<String> parentIds) throws GroundException {
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
