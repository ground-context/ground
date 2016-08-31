/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.berkeley.ground.api.models.neo4j;

import edu.berkeley.ground.api.models.RichVersion;
import edu.berkeley.ground.api.models.RichVersionFactory;
import edu.berkeley.ground.api.models.StructureVersion;
import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.api.versions.GroundType;
import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.Neo4jClient;
import edu.berkeley.ground.db.Neo4jClient.Neo4jConnection;
import edu.berkeley.ground.exceptions.GroundException;
import org.neo4j.driver.internal.value.StringValue;
import org.neo4j.driver.v1.Record;

import java.util.*;

public class Neo4jRichVersionFactory extends RichVersionFactory {
    private Neo4jStructureVersionFactory structureVersionFactory;
    private Neo4jTagFactory tagFactory;

    public Neo4jRichVersionFactory(Neo4jStructureVersionFactory structureVersionFactory, Neo4jTagFactory tagFactory) {
        this.structureVersionFactory = structureVersionFactory;
        this.tagFactory = tagFactory;
    }

    public void insertIntoDatabase(GroundDBConnection connectionPointer, String id, Optional<Map<String, Tag>> tags, Optional<String> structureVersionId, Optional<String> reference, Optional<Map<String, String>> parameters) throws GroundException {
        Neo4jConnection connection = (Neo4jConnection) connectionPointer;

        if (structureVersionId.isPresent()) {
            StructureVersion structureVersion = this.structureVersionFactory.retrieveFromDatabase(structureVersionId.get());
            RichVersionFactory.checkStructureTags(structureVersion, tags);
        }

        if (parameters.isPresent()) {
            Map<String, String> parametersMap = parameters.get();

            for (String key : parametersMap.keySet()) {
                String value = parametersMap.get(key);

                List<DbDataContainer> insertions = new ArrayList<>();
                insertions.add(new DbDataContainer("id", GroundType.STRING, id));
                insertions.add(new DbDataContainer("pkey", GroundType.STRING, key));
                insertions.add(new DbDataContainer("value", GroundType.STRING, value));

                connection.addVertexAndEdge("RichVersionExternalParameter", insertions, "RichVersionExternalParameterConnection", id, new ArrayList<>());

                insertions.clear();
            }
        }

        if (structureVersionId.isPresent()) {
            connection.setProperty(id, "structure_id", structureVersionId.get(), true);
        }

        if (reference.isPresent()) {
            connection.setProperty(id, "reference", reference.get(), true);
        }

        if (tags.isPresent()) {
            for (String key : tags.get().keySet()) {
                Tag tag = tags.get().get(key);

                List<DbDataContainer> tagInsertion = new ArrayList<>();
                tagInsertion.add(new DbDataContainer("id", GroundType.STRING, id));
                tagInsertion.add(new DbDataContainer("tkey", GroundType.STRING, key));
                tagInsertion.add(new DbDataContainer("value", GroundType.STRING, tag.getValue().map(Object::toString).orElse(null)));
                tagInsertion.add(new DbDataContainer("type", GroundType.STRING, tag.getValueType().map(Object::toString).orElse(null)));

                connection.addVertexAndEdge("Tag", tagInsertion, "TagConnection", id, new ArrayList<>());
            }
        }
    }

    public RichVersion retrieveFromDatabase(GroundDBConnection connectionPointer, String id) throws GroundException {
        Neo4jConnection connection = (Neo4jConnection) connectionPointer;

        List<DbDataContainer> predicates = new ArrayList<>();
        predicates.add(new DbDataContainer("id", GroundType.STRING, id));
        Record record = connection.getVertex(predicates);

        List<String> returnFields = new ArrayList<>();
        returnFields.add("pkey");
        returnFields.add("value");

        List<Record> parameterVertices = connection.getAdjacentVerticesByEdgeLabel(id, "RichVersionExternalParameterConnection", returnFields);
        Optional<Map<String, String>> parameters;

        if (!parameterVertices.isEmpty()) {
            Map<String, String> parametersMap = new HashMap<>();

            for (Record parameter : parameterVertices) {
                parametersMap.put(Neo4jClient.getStringFromValue((StringValue) parameter.get("pkey")), Neo4jClient.getStringFromValue((StringValue) parameter.get("value")));
            }

            parameters = Optional.of(parametersMap);
        } else {
            parameters = Optional.empty();
        }

        Optional<Map<String, Tag>> tags = tagFactory.retrieveFromDatabaseById(connectionPointer, id);

        Optional<String> reference = Optional.ofNullable(record.get("reference").toString());
        Optional<String> structureVersionId = Optional.ofNullable(record.get("structure_id").toString());

        return RichVersionFactory.construct(id, tags, structureVersionId, reference, parameters);
    }
}
