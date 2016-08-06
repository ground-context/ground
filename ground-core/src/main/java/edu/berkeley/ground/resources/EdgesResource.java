package edu.berkeley.ground.resources;

import com.codahale.metrics.annotation.Timed;
import edu.berkeley.ground.api.models.Edge;
import edu.berkeley.ground.api.models.EdgeFactory;
import edu.berkeley.ground.api.models.EdgeVersion;
import edu.berkeley.ground.api.models.EdgeVersionFactory;
import edu.berkeley.ground.exceptions.GroundException;
import io.dropwizard.jersey.params.NonEmptyStringParam;
import org.hibernate.validator.valuehandling.UnwrapValidatedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.Valid;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.List;

@Path("/edges")
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
    @Path("/{name}")
    public Edge getEdge(@PathParam("name") String name) throws GroundException {
        LOGGER.info("Retrieving edge " + name + ".");
        return this.edgeFactory.retrieveFromDatabase(name);
    }

    @GET
    @Timed
    @Path("/versions/{id}")
    public EdgeVersion getEdgeVersion(@PathParam("id") String id) throws GroundException {
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
    public EdgeVersion createEdgeVersion(@Valid EdgeVersion edgeVersion, @QueryParam("parent") List<String> parentIds) throws GroundException {
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
