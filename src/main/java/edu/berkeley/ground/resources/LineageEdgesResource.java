package edu.berkeley.ground.resources;

import com.codahale.metrics.annotation.Timed;
import edu.berkeley.ground.api.usage.LineageEdge;
import edu.berkeley.ground.api.usage.LineageEdgeFactory;
import edu.berkeley.ground.api.usage.LineageEdgeVersion;
import edu.berkeley.ground.api.usage.LineageEdgeVersionFactory;
import edu.berkeley.ground.db.DBClient;
import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.exceptions.GroundException;
import io.dropwizard.jersey.params.NonEmptyStringParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.Valid;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

@Path("/lineage")
@Produces(MediaType.APPLICATION_JSON)
public class LineageEdgesResource {
    private static final Logger LOGGER = LoggerFactory.getLogger(LineageEdgesResource.class);

    private DBClient dbClient;
    private LineageEdgeFactory lineageEdgeFactory;
    private LineageEdgeVersionFactory lineageEdgeVersionFactory;

    public LineageEdgesResource(DBClient dbClient, LineageEdgeFactory lineageEdgeFactory, LineageEdgeVersionFactory lineageEdgeVersionFactory) {
        this.dbClient = dbClient;
        this.lineageEdgeFactory = lineageEdgeFactory;
        this.lineageEdgeVersionFactory = lineageEdgeVersionFactory;
    }

    @GET
    @Timed
    @Path("/{name}")
    public LineageEdge getLineageEdge(@PathParam("name") String name) throws GroundException {
        LOGGER.info("Retrieving lineage edge " + name + ".");
        GroundDBConnection connection = this.dbClient.getConnection();

        try {
            LineageEdge lineageEdge = this.lineageEdgeFactory.retrieveFromDatabase(connection, name);

            connection.commit();
            return lineageEdge;
        } catch (GroundException e) {
            connection.abort();
            throw e;
        }
    }

    @GET
    @Timed
    @Path("/versions/{id}")
    public LineageEdgeVersion getLineageEdgeVersion(@PathParam("id") String id) throws GroundException {
        LOGGER.info("Retrieving lineage edge version " + id + ".");
        GroundDBConnection connection = this.dbClient.getConnection();

        try {
            LineageEdgeVersion lineageEdgeVersion = this.lineageEdgeVersionFactory.retrieveFromDatabase(connection, id);

            connection.commit();
            return lineageEdgeVersion;
        } catch (GroundException e) {
            connection.abort();
            throw e;
        }
    }

    @POST
    @Timed
    @Path("/{name}")
    public LineageEdge createLineageEdge(@PathParam("name") String name) throws GroundException {
        LOGGER.info("Creating lineage edge " + name + ".");
        GroundDBConnection connection = this.dbClient.getConnection();

        try {
            LineageEdge lineageEdge = this.lineageEdgeFactory.create(connection, name);

            connection.commit();
            return lineageEdge;
        } catch (GroundException e) {
            connection.abort();
            throw e;
        }
    }

    @POST
    @Timed
    @Path("/versions")
    public LineageEdgeVersion createLineageEdgeVersion(@Valid LineageEdgeVersion lineageEdgeVersion, @QueryParam("parent") NonEmptyStringParam parentId) throws GroundException {
        LOGGER.info("Creating lineage edge version in lineage edge " + lineageEdgeVersion.getLineageEdgeId() + ".");
        GroundDBConnection connection = this.dbClient.getConnection();

        try {
            LineageEdgeVersion created = this.lineageEdgeVersionFactory.create(connection,
                                                                               lineageEdgeVersion.getTags(),
                                                                               lineageEdgeVersion.getStructureVersionId(),
                                                                               lineageEdgeVersion.getReference(),
                                                                               lineageEdgeVersion.getParameters(),
                                                                               lineageEdgeVersion.getFromId(),
                                                                               lineageEdgeVersion.getToId(),
                                                                               lineageEdgeVersion.getLineageEdgeId(),
                                                                               parentId.get());

            connection.commit();
            return created;
        } catch (GroundException e) {
            connection.abort();
            throw e;
        }
    }
}
