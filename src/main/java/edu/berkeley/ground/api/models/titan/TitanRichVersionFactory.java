package edu.berkeley.ground.api.models.titan;

import com.thinkaurelius.titan.core.TitanVertex;
import edu.berkeley.ground.api.models.RichVersion;
import edu.berkeley.ground.api.models.RichVersionFactory;
import edu.berkeley.ground.api.models.StructureVersion;
import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.api.versions.Type;
import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.TitanClient.TitanConnection;
import edu.berkeley.ground.exceptions.GroundException;

import java.util.*;

public class TitanRichVersionFactory extends RichVersionFactory {
    private TitanStructureVersionFactory structureVersionFactory;
    private TitanTagFactory tagFactory;

    public TitanRichVersionFactory(TitanStructureVersionFactory structureVersionFactory, TitanTagFactory tagFactory) {
        this.structureVersionFactory = structureVersionFactory;
        this.tagFactory = tagFactory;
    }

    public void insertIntoDatabase(GroundDBConnection connectionPointer, String id, Optional<Map<String, Tag>>tags, Optional<String> structureVersionId, Optional<String> reference, Optional<Map<String, String>> parameters) throws GroundException {
        TitanConnection connection = (TitanConnection) connectionPointer;

        List<DbDataContainer> predicates = new ArrayList<>();
        predicates.add(new DbDataContainer("id", Type.STRING, id));
        TitanVertex versionVertex = connection.getVertex(predicates);

        if (structureVersionId.isPresent()) {
            StructureVersion structureVersion = this.structureVersionFactory.retrieveFromDatabase(structureVersionId.get());
            RichVersionFactory.checkStructureTags(connection, structureVersion, tags);
        }

        if (parameters.isPresent()) {
            Map<String, String> parametersMap = parameters.get();

            for (String key : parametersMap.keySet()) {
                String value = parametersMap.get(key);

                List<DbDataContainer> insertions = new ArrayList<>();
                insertions.add(new DbDataContainer("id", Type.STRING, id));
                insertions.add(new DbDataContainer("pkey", Type.STRING, key));
                insertions.add(new DbDataContainer("value", Type.STRING, value));

                TitanVertex parameterVertex = connection.addVertex("RichVersionExternalParameter", insertions);

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
                tagInsertion.add(new DbDataContainer("value", Type.STRING, tag.getValue().map(t -> t.toString()).orElse(null)));
                tagInsertion.add(new DbDataContainer("type", Type.STRING, tag.getValueType().map(t -> t.toString()).orElse(null)));

                TitanVertex tagVertex = connection.addVertex("Tag", tagInsertion);
                connection.addEdge("TagConnection", versionVertex, tagVertex, new ArrayList<>());
            }
        }
    }

    public RichVersion retrieveFromDatabase(GroundDBConnection connectionPointer, String id) throws GroundException {
        TitanConnection connection = (TitanConnection) connectionPointer;

        List<DbDataContainer> predicates = new ArrayList<>();
        predicates.add(new DbDataContainer("id", Type.STRING, id));
        TitanVertex versionVertex = connection.getVertex(predicates);

        List<TitanVertex> parameterVertices = connection.getAdjacentVerticesByEdgeLabel(versionVertex, "RichVersionExternalParameterConnection");
        Optional<Map<String, String>> parameters;

        if (!parameterVertices.isEmpty()) {
            Map<String, String> parametersMap = new HashMap<>();

            for (TitanVertex parameter : parameterVertices) {
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
