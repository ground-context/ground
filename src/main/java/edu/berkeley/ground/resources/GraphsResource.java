package edu.berkeley.ground.resources;

import com.codahale.metrics.annotation.Timed;
import edu.berkeley.ground.api.models.Graph;
import edu.berkeley.ground.api.models.GraphFactory;
import edu.berkeley.ground.api.models.GraphVersion;
import edu.berkeley.ground.api.models.GraphVersionFactory;
import edu.berkeley.ground.db.DBClient;
import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.exceptions.GroundException;
import io.dropwizard.jersey.params.NonEmptyStringParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.Valid;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

@Path("/graphs")
@Produces(MediaType.APPLICATION_JSON)
public class GraphsResource {
    private static final Logger LOGGER = LoggerFactory.getLogger(GraphsResource.class);

    private DBClient dbClient;
    private GraphFactory graphFactory;
    private GraphVersionFactory graphVersionFactory;

    public GraphsResource(DBClient dbClient, GraphFactory graphFactory, GraphVersionFactory graphVersionFactory) {
        this.dbClient = dbClient;
        this.graphFactory = graphFactory;
        this.graphVersionFactory = graphVersionFactory;
    }

    @GET
    @Timed
    @Path("/{name}")
    public Graph getGraph(@PathParam("name") String name) throws GroundException {
        LOGGER.info("Retrieving graph " + name + ".");
        GroundDBConnection connection = this.dbClient.getConnection();

        try {
            Graph graph = this.graphFactory.retrieveFromDatabase(connection, name);

            connection.commit();
            return graph;
        } catch (GroundException e) {
            connection.abort();
            throw e;
        }
    }

    @GET
    @Timed
    @Path("/versions/{id}")
    public GraphVersion getGraphVersion(@PathParam("id") String id) throws GroundException {
        LOGGER.info("Retrieving graph version " + id + ".");
        GroundDBConnection connection = this.dbClient.getConnection();

        try {
            GraphVersion graphVersion = this.graphVersionFactory.retrieveFromDatabase(connection, id);

            connection.commit();
            return graphVersion;
        } catch (GroundException e) {
            connection.abort();
            throw e;
        }
    }

    @POST
    @Timed
    @Path("/{name}")
    public Graph createGraph(@PathParam("name") String name) throws GroundException {
        LOGGER.info("Creating graph " + name + ".");
        GroundDBConnection connection = this.dbClient.getConnection();

        try {
            Graph graph = this.graphFactory.create(connection, name);

            connection.commit();
            return graph;
        } catch (GroundException e) {
            connection.abort();
            throw e;
        }
    }

    @POST
    @Timed
    @Path("/versions")
    public GraphVersion createGraphVersion(@Valid GraphVersion graphVersion, @QueryParam("parent")NonEmptyStringParam parentId) throws GroundException {
        LOGGER.info("Creating graph version in graph " + graphVersion.getGraphId() + ".");
        GroundDBConnection connection = this.dbClient.getConnection();

        try {
            GraphVersion created = this.graphVersionFactory.create(connection,
                                                                   graphVersion.getTags(),
                                                                   graphVersion.getStructureVersionId(),
                                                                   graphVersion.getReference(),
                                                                   graphVersion.getParameters(),
                                                                   graphVersion.getGraphId(),
                                                                   graphVersion.getEdgeVersionIds(),
                                                                   parentId.get());

            connection.commit();
            return created;
        } catch (GroundException e) {
            connection.abort();
            throw e;
        }
    }
}
