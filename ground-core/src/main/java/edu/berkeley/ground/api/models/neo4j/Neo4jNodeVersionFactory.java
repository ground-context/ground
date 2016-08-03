package edu.berkeley.ground.api.models.neo4j;

import edu.berkeley.ground.api.models.NodeVersion;
import edu.berkeley.ground.api.models.NodeVersionFactory;
import edu.berkeley.ground.api.models.RichVersion;
import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.api.versions.Type;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.Neo4jClient;
import edu.berkeley.ground.db.Neo4jClient.Neo4jConnection;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.util.IdGenerator;
import org.neo4j.driver.v1.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class Neo4jNodeVersionFactory extends NodeVersionFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(Neo4jNodeVersionFactory.class);
    private Neo4jClient dbClient;

    private Neo4jNodeFactory nodeFactory;
    private Neo4jRichVersionFactory richVersionFactory;

    public Neo4jNodeVersionFactory(Neo4jNodeFactory nodeFactory, Neo4jRichVersionFactory richVersionFactory, Neo4jClient dbClient) {
        this.dbClient = dbClient;
        this.nodeFactory = nodeFactory;
        this.richVersionFactory = richVersionFactory;
    }

    public NodeVersion create(Optional<Map<String, Tag>> tags,
                              Optional<String> structureVersionId,
                              Optional<String> reference,
                              Optional<Map<String, String>> parameters,
                              String nodeId,
                              Optional<String> parentId) throws GroundException {

        Neo4jConnection connection = this.dbClient.getConnection();

        try {
            String id = IdGenerator.generateId(nodeId);

            // add the id of the version to the tag
            tags = tags.map(tagsMap ->
                                    tagsMap.values().stream().collect(Collectors.toMap(Tag::getKey, tag -> new Tag(id, tag.getKey(), tag.getValue(), tag.getValueType())))
            );

            List<DbDataContainer> insertions = new ArrayList<>();
            insertions.add(new DbDataContainer("id", Type.STRING, id));
            insertions.add(new DbDataContainer("node_id", Type.STRING, nodeId));

            connection.addVertex("NodeVersion", insertions);
            this.richVersionFactory.insertIntoDatabase(connection, id, tags, structureVersionId, reference, parameters);

            this.nodeFactory.update(connection, nodeId, id, parentId);

            connection.commit();
            LOGGER.info("Created node version " + id + " in node " + nodeId + ".");

            return NodeVersionFactory.construct(id, tags, structureVersionId, reference, parameters, nodeId);
        } catch (GroundException e) {
            connection.abort();

            throw e;
        }
    }

    public NodeVersion retrieveFromDatabase(String id) throws GroundException {
        Neo4jConnection connection = this.dbClient.getConnection();

        try {
            RichVersion version = this.richVersionFactory.retrieveFromDatabase(connection, id);

            List<DbDataContainer> predicates = new ArrayList<>();
            predicates.add(new DbDataContainer("id", Type.STRING, id));

            Record record = connection.getVertex(predicates);
            String nodeId = record.get("node_id").toString();

            connection.commit();
            LOGGER.info("Retrieved node version " + id + " in node " + nodeId + ".");

            return NodeVersionFactory.construct(id, version.getTags(), version.getStructureVersionId(), version.getReference(), version.getParameters(), nodeId);
        } catch (GroundException e) {
            connection.abort();

            throw e;
        }
    }

    public List<String> getTransitiveClosure(String nodeVersionId) throws GroundException {
        Neo4jConnection connection = this.dbClient.getConnection();
        try {
            List<String> result = connection.transitiveClosure(nodeVersionId);

            connection.commit();
            return result;
        } catch (GroundException e) {
            connection.abort();

            throw e;
        }
    }
}
