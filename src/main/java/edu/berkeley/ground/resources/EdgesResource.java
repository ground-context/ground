package edu.berkeley.ground.resources;

import com.codahale.metrics.annotation.Timed;
import edu.berkeley.ground.api.models.Edge;
import edu.berkeley.ground.api.models.EdgeVersion;
import edu.berkeley.ground.db.DBClient;
import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.exceptions.GroundException;
import io.dropwizard.jersey.params.NonEmptyStringParam;
import org.hibernate.validator.valuehandling.UnwrapValidatedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.Valid;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

@Path("/edges")
@Produces(MediaType.APPLICATION_JSON)
public class EdgesResource {
    private static final Logger LOGGER = LoggerFactory.getLogger(EdgesResource.class);

    private DBClient dbClient;

    public EdgesResource(DBClient dbClient) {
        this.dbClient = dbClient;
    }

    @GET
    @Timed
    @Path("/{name}")
    public Edge getEdge(@PathParam("name") String name) throws GroundException {
        LOGGER.info("Retrieving edge " + name + ".");
        GroundDBConnection connection = this.dbClient.getConnection();

        try {
            Edge edge = Edge.retrieveFromDatabase(connection, name);

            connection.commit();
            return edge;
        } catch (GroundException e) {
            connection.abort();
            throw e;
        }
    }

    @GET
    @Timed
    @Path("/versions/{id}")
    public EdgeVersion getEdgeVersion(@PathParam("id") String id) throws GroundException {
        LOGGER.info("Retrieving edge version " + id + ".");
        GroundDBConnection connection = this.dbClient.getConnection();

        try {
            EdgeVersion edgeVersion = EdgeVersion.retrieveFromDatabase(connection, id);

            connection.commit();
            return edgeVersion;
        } catch (GroundException e) {
            connection.abort();
            throw e;
        }
    }

    @POST
    @Timed
    @Path("/{name}")
    public Edge createEdge(@PathParam("name") String name) throws GroundException {
        LOGGER.info("Creating edge " + name + ".");
        GroundDBConnection connection = this.dbClient.getConnection();

        try {
            Edge edge = Edge.create(connection, name);

            connection.commit();
            return edge;
        } catch (GroundException e) {
            connection.abort();
            throw e;
        }
    }

    @POST
    @Timed
    @Path("/versions")
    public EdgeVersion createEdgeVersion(@Valid EdgeVersion edgeVersion, @QueryParam("parent") @UnwrapValidatedValue NonEmptyStringParam parentId) throws GroundException {
        LOGGER.info("Creating edge version in edge " + edgeVersion.getEdgeId() + ".");
        GroundDBConnection connection = this.dbClient.getConnection();

        try {
            EdgeVersion created =EdgeVersion.create(connection,
                                                    edgeVersion.getTags(),
                                                    edgeVersion.getStructureVersionId(),
                                                    edgeVersion.getReference(),
                                                    edgeVersion.getParameters(),
                                                    edgeVersion.getEdgeId(),
                                                    edgeVersion.getFromId(),
                                                    edgeVersion.getToId(),
                                                    parentId.get());

            connection.commit();
            return created;
        } catch (GroundException e) {
            connection.abort();
            throw e;
        }
    }
}
