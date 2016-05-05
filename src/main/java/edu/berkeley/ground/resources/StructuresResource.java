package edu.berkeley.ground.resources;

import com.codahale.metrics.annotation.Timed;
import edu.berkeley.ground.api.models.Structure;
import edu.berkeley.ground.api.models.StructureFactory;
import edu.berkeley.ground.api.models.StructureVersion;
import edu.berkeley.ground.api.models.StructureVersionFactory;
import edu.berkeley.ground.db.DBClient;
import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.exceptions.GroundException;
import io.dropwizard.jersey.params.NonEmptyStringParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.Valid;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

@Path("/structures")
@Produces(MediaType.APPLICATION_JSON)
public class StructuresResource {
    private static final Logger LOGGER = LoggerFactory.getLogger(StructuresResource.class);

    private DBClient dbClient;
    private StructureFactory structureFactory;
    private StructureVersionFactory structureVersionFactory;

    public StructuresResource(DBClient dbClient, StructureFactory structureFactory, StructureVersionFactory structureVersionFactory) {
        this.dbClient = dbClient;
        this.structureFactory = structureFactory;
        this.structureVersionFactory = structureVersionFactory;
    }

    @GET
    @Timed
    @Path("/{name}")
    public Structure getStructure(@PathParam("name") String name) throws GroundException {
        LOGGER.info("Retrieving structure " + name + ".");
        GroundDBConnection connection = this.dbClient.getConnection();

        try {
            Structure structure = this.structureFactory.retrieveFromDatabase(connection, name);

            connection.commit();
            return structure;
        } catch (GroundException e) {
            connection.abort();
            throw e;
        }
    }

    @GET
    @Timed
    @Path("/versions/{id}")
    public StructureVersion getStructureVersion(@PathParam("id") String id) throws GroundException {
        LOGGER.info("Retrieving structure version " + id + ".");
        GroundDBConnection connection = this.dbClient.getConnection();

        try {
            StructureVersion structureVersion = this.structureVersionFactory.retrieveFromDatabase(connection, id);

            connection.commit();
            return structureVersion;
        } catch (GroundException e) {
            connection.abort();
            throw e;
        }
    }

    @POST
    @Timed
    @Path("/{name}")
    public Structure createStructure(@PathParam("name") String name) throws GroundException {
        LOGGER.info("Creating structure " + name + ".");
        GroundDBConnection connection = this.dbClient.getConnection();

        try {
            Structure structure = this.structureFactory.create(connection, name);

            connection.commit();
            return structure;
        } catch (GroundException e) {
            connection.abort();
            throw e;
        }
    }

    @POST
    @Timed
    @Path("/versions")
    public StructureVersion createStructureVersion(@Valid StructureVersion structureVersion, @QueryParam("parent") NonEmptyStringParam parentId) throws GroundException {
        LOGGER.info("Creating structure version in structure " + structureVersion.getStructureId() + ".");
        GroundDBConnection connection = this.dbClient.getConnection();

        try {
            StructureVersion created = this.structureVersionFactory.create(connection,
                                                                           structureVersion.getStructureId(),
                                                                           structureVersion.getAttributes(),
                                                                           parentId.get());

            connection.commit();
            return created;
        } catch (GroundException e) {
            connection.abort();
            throw e;
        }
    }
}
