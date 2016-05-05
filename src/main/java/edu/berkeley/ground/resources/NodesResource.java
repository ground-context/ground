package edu.berkeley.ground.resources;

import com.codahale.metrics.annotation.Timed;
import edu.berkeley.ground.api.models.Node;
import edu.berkeley.ground.api.models.NodeFactory;
import edu.berkeley.ground.api.models.NodeVersion;
import edu.berkeley.ground.api.models.NodeVersionFactory;
import edu.berkeley.ground.db.DBClient;
import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.exceptions.GroundException;
import io.dropwizard.jersey.params.NonEmptyStringParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.Valid;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

@Path("/nodes")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class NodesResource {
    private static final Logger LOGGER = LoggerFactory.getLogger(NodesResource.class);

    private DBClient dbClient;
    private NodeFactory nodeFactory;
    private NodeVersionFactory nodeVersionFactory;

    public NodesResource(DBClient dbClient, NodeFactory nodeFactory, NodeVersionFactory nodeVersionFactory) {
        this.dbClient = dbClient;
        this.nodeFactory = nodeFactory;
        this.nodeVersionFactory = nodeVersionFactory;
    }

    @GET
    @Timed
    @Path("/{name}")
    public Node getNode(@PathParam("name") String name) throws GroundException {
        LOGGER.info("Retrieving node " + name + ".");
        GroundDBConnection connection = this.dbClient.getConnection();

        try {
            Node node =  this.nodeFactory.retrieveFromDatabase(connection, name);

            connection.commit();
            return node;
        } catch (GroundException e) {
            connection.abort();
            throw e;
        }
    }

    @GET
    @Timed
    @Path("/versions/{id}")
    public NodeVersion getNodeVersion(@PathParam("id") String id) throws GroundException {
        LOGGER.info("Retrieving node version " + id + ".");
        GroundDBConnection connection = this.dbClient.getConnection();

        try {
            NodeVersion nodeVersion = this.nodeVersionFactory.retrieveFromDatabase(connection, id);

            connection.commit();
            return nodeVersion;
        } catch (GroundException e) {
            connection.abort();
            throw e;
        }
    }

    @POST
    @Timed
    @Path("/{name}")
    public Node createNode(@PathParam("name") String name) throws GroundException {
        LOGGER.info("Creating node " + name + ".");
        GroundDBConnection connection = this.dbClient.getConnection();

        try {
            Node node = this.nodeFactory.create(connection, name);

            connection.commit();
            return node;
        } catch (GroundException e) {
            connection.abort();
            throw e;
        }
    }

    @POST
    @Timed
    @Path("/versions")
    public NodeVersion createNodeVersion(@Valid NodeVersion nodeVersion, @QueryParam("parent") NonEmptyStringParam parentId) throws GroundException {
        LOGGER.info("Creating node version in node " + nodeVersion.getNodeId() + ".");
        GroundDBConnection connection = this.dbClient.getConnection();

        try {
            NodeVersion created = this.nodeVersionFactory.create(connection,
                                                                 nodeVersion.getTags(),
                                                                 nodeVersion.getStructureVersionId(),
                                                                 nodeVersion.getReference(),
                                                                 nodeVersion.getParameters(),
                                                                 nodeVersion.getNodeId(),
                                                                 parentId.get());

            connection.commit();
            return created;
        } catch (GroundException e) {
            connection.abort();
            throw e;
        }
    }
}
