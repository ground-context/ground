package edu.berkeley.ground.resources;

import com.codahale.metrics.annotation.Timed;
import edu.berkeley.ground.api.models.Node;
import edu.berkeley.ground.api.models.NodeFactory;
import edu.berkeley.ground.api.models.NodeVersion;
import edu.berkeley.ground.api.models.NodeVersionFactory;
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

    private NodeFactory nodeFactory;
    private NodeVersionFactory nodeVersionFactory;

    public NodesResource(NodeFactory nodeFactory, NodeVersionFactory nodeVersionFactory) {
        this.nodeFactory = nodeFactory;
        this.nodeVersionFactory = nodeVersionFactory;
    }

    @GET
    @Timed
    @Path("/{name}")
    public Node getNode(@PathParam("name") String name) throws GroundException {
        LOGGER.info("Retrieving node " + name + ".");
        return this.nodeFactory.retrieveFromDatabase(name);
    }

    @GET
    @Timed
    @Path("/versions/{id}")
    public NodeVersion getNodeVersion(@PathParam("id") String id) throws GroundException {
        LOGGER.info("Retrieving node version " + id + ".");
        return this.nodeVersionFactory.retrieveFromDatabase(id);
    }

    @POST
    @Timed
    @Path("/{name}")
    public Node createNode(@PathParam("name") String name) throws GroundException {
        LOGGER.info("Creating node " + name + ".");
        return this.nodeFactory.create(name);
    }

    @POST
    @Timed
    @Path("/versions")
    public NodeVersion createNodeVersion(@Valid NodeVersion nodeVersion, @QueryParam("parent") NonEmptyStringParam parentId) throws GroundException {
        LOGGER.info("Creating node version in node " + nodeVersion.getNodeId() + ".");
        return this.nodeVersionFactory.create(nodeVersion.getTags(),
                                              nodeVersion.getStructureVersionId(),
                                              nodeVersion.getReference(),
                                              nodeVersion.getParameters(),
                                              nodeVersion.getNodeId(),
                                              parentId.get());
    }
}
