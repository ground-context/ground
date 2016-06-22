package edu.berkeley.ground.api.models.gremlin;

import edu.berkeley.ground.api.models.RichVersion;
import edu.berkeley.ground.api.models.RichVersionFactory;
import edu.berkeley.ground.api.models.StructureVersion;
import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.api.versions.Type;
import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.GremlinClient.GremlinConnection;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.util.ElasticSearchClient;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.*;

public class GremlinRichVersionFactory extends RichVersionFactory {
    private GremlinStructureVersionFactory structureVersionFactory;
    private GremlinTagFactory tagFactory;
    private ElasticSearchClient elasticSearchClient;

    public GremlinRichVersionFactory(GremlinStructureVersionFactory structureVersionFactory, GremlinTagFactory tagFactory, ElasticSearchClient elasticSearchClient) {
        this.structureVersionFactory = structureVersionFactory;
        this.tagFactory = tagFactory;
        this.elasticSearchClient = elasticSearchClient;
    }

    public void insertIntoDatabase(GroundDBConnection connectionPointer, String id, Optional<Map<String, Tag>>tags, Optional<String> structureVersionId, Optional<String> reference, Optional<Map<String, String>> parameters) throws GroundException {
        GremlinConnection connection = (GremlinConnection) connectionPointer;

        List<DbDataContainer> predicates = new ArrayList<>();
        predicates.add(new DbDataContainer("id", Type.STRING, id));
        Vertex versionVertex = connection.getVertex(predicates);

        if (structureVersionId.isPresent()) {
            StructureVersion structureVersion = this.structureVersionFactory.retrieveFromDatabase(structureVersionId.get());
            RichVersionFactory.checkStructureTags(structureVersion, tags);
        }

        if (parameters.isPresent()) {
            Map<String, String> parametersMap = parameters.get();

            for (String key : parametersMap.keySet()) {
                String value = parametersMap.get(key);

                List<DbDataContainer> insertions = new ArrayList<>();
                insertions.add(new DbDataContainer("id", Type.STRING, id));
                insertions.add(new DbDataContainer("pkey", Type.STRING, key));
                insertions.add(new DbDataContainer("value", Type.STRING, value));

                Vertex parameterVertex = connection.addVertex("RichVersionExternalParameter", insertions);

                insertions.clear();
                connection.addEdge("RichVersionExternalParameterConnection", versionVertex, parameterVertex, insertions);
            }
        }

        if (structureVersionId.isPresent()) {
            versionVertex.property("structure_id", structureVersionId.get());
        }

        if (reference.isPresent()) {
            versionVertex.property("reference", reference.get());
        }

        if (tags.isPresent()) {
            for (String key : tags.get().keySet()) {
                Tag tag = tags.get().get(key);

                List<DbDataContainer> tagInsertion = new ArrayList<>();
                tagInsertion.add(new DbDataContainer("id", Type.STRING, id));
                tagInsertion.add(new DbDataContainer("tkey", Type.STRING, key));
                tagInsertion.add(new DbDataContainer("value", Type.STRING, tag.getValue().map(Object::toString).orElse(null)));
                tagInsertion.add(new DbDataContainer("type", Type.STRING, tag.getValueType().map(Object::toString).orElse(null)));

                Vertex tagVertex = connection.addVertex("Tag", tagInsertion);
                connection.addEdge("TagConnection", versionVertex, tagVertex, new ArrayList<>());
            }

            if (this.elasticSearchClient != null) {
                elasticSearchClient.indexTags(id, tags.get());
            }
        }
    }

    public RichVersion retrieveFromDatabase(GroundDBConnection connectionPointer, String id) throws GroundException {
        GremlinConnection connection = (GremlinConnection) connectionPointer;

        List<DbDataContainer> predicates = new ArrayList<>();
        predicates.add(new DbDataContainer("id", Type.STRING, id));
        Vertex versionVertex = connection.getVertex(predicates);

        List<Vertex> parameterVertices = connection.getAdjacentVerticesByEdgeLabel(versionVertex, "RichVersionExternalParameterConnection");
        Optional<Map<String, String>> parameters;

        if (!parameterVertices.isEmpty()) {
            Map<String, String> parametersMap = new HashMap<>();

            for (Vertex parameter : parameterVertices) {
                parametersMap.put(parameter.property("pkey").value().toString(), parameter.property("value").value().toString());
            }

            parameters = Optional.of(parametersMap);
        } else {
            parameters = Optional.empty();
        }

        Optional<Map<String, Tag>> tags = tagFactory.retrieveFromDatabaseById(connectionPointer, id);

        Optional<String> reference = Optional.ofNullable(versionVertex.property("reference").toString());
        Optional<String> structureVersionId = Optional.ofNullable(versionVertex.property("structureversion_id").value().toString());

        return RichVersionFactory.construct(id, tags, structureVersionId, reference, parameters);
    }
}
