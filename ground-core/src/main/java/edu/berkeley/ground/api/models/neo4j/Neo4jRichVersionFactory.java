/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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
import edu.berkeley.ground.exceptions.EmptyResultException;
import edu.berkeley.ground.exceptions.GroundException;

import org.neo4j.driver.internal.value.NullValue;
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

  public void insertIntoDatabase(GroundDBConnection connectionPointer,
                                 long id,
                                 Map<String, Tag> tags,
                                 long structureVersionId,
                                 String reference,
                                 Map<String, String> referenceParameters
  ) throws GroundException {
    Neo4jConnection connection = (Neo4jConnection) connectionPointer;

    if (structureVersionId != -1) {
      StructureVersion structureVersion = this.structureVersionFactory.retrieveFromDatabase(structureVersionId);
      RichVersionFactory.checkStructureTags(structureVersion, tags);
    }

    for (String key : referenceParameters.keySet()) {
      String value = referenceParameters.get(key);

      List<DbDataContainer> insertions = new ArrayList<>();
      insertions.add(new DbDataContainer("richversion_id", GroundType.LONG, id));
      insertions.add(new DbDataContainer("pkey", GroundType.STRING, key));
      insertions.add(new DbDataContainer("value", GroundType.STRING, value));

      connection.addVertexAndEdge("RichVersionExternalParameter", insertions, "RichVersionExternalParameterConnection", id, new ArrayList<>());

      insertions.clear();
    }

    if (structureVersionId != -1) {
      connection.setProperty(id, "structure_id", structureVersionId, false);
    }

    if (reference != null) {
      connection.setProperty(id, "reference", reference, true);
    }

    for (String key : tags.keySet()) {
      Tag tag = tags.get(key);

      List<DbDataContainer> tagInsertion = new ArrayList<>();
      tagInsertion.add(new DbDataContainer("richversion_id", GroundType.LONG, id));
      tagInsertion.add(new DbDataContainer("tkey", GroundType.STRING, key));

      if (tag.getValue() != null) {
        tagInsertion.add(new DbDataContainer("value", GroundType.STRING, tag.getValue().toString()));
        tagInsertion.add(new DbDataContainer("type", GroundType.STRING, tag.getValueType().toString()));
      } else {
        tagInsertion.add(new DbDataContainer("value", GroundType.STRING, null));
        tagInsertion.add(new DbDataContainer("type", GroundType.STRING, null));
      }

      connection.addVertexAndEdge("Tag", tagInsertion, "TagConnection", id, new ArrayList<>());
      tagInsertion.clear();
    }
  }

  public RichVersion retrieveFromDatabase(GroundDBConnection connectionPointer, long id) throws GroundException {
    Neo4jConnection connection = (Neo4jConnection) connectionPointer;

    List<DbDataContainer> predicates = new ArrayList<>();
    predicates.add(new DbDataContainer("id", GroundType.LONG, id));
    Record record;

    try {
      record = connection.getVertex(predicates);
    } catch (EmptyResultException eer) {
      throw new GroundException("No RichVersion found with id " + id + ".");
    }

    List<String> returnFields = new ArrayList<>();
    returnFields.add("pkey");
    returnFields.add("value");

    List<Record> parameterVertices = connection.getAdjacentVerticesByEdgeLabel("RichVersionExternalParameterConnection", id, returnFields);
    Map<String, String> referenceParameters = new HashMap<>();

    if (!parameterVertices.isEmpty()) {
      for (Record parameter : parameterVertices) {
        referenceParameters.put(Neo4jClient.getStringFromValue((StringValue) parameter.get("pkey")), Neo4jClient.getStringFromValue((StringValue) parameter.get("value")));
      }
    }

    Map<String, Tag> tags = tagFactory.retrieveFromDatabaseById(connectionPointer, id);

    String reference;
    if (record.get("v").asNode().get("reference") instanceof NullValue) {
      reference = null;
    } else {
      reference = Neo4jClient.getStringFromValue((StringValue) record.get("v").asNode()
          .get("reference"));
    }

    long structureVersionId;
    if (record.get("v").asNode().get("structure_id") instanceof NullValue) {
      structureVersionId = -1;
    } else {
      structureVersionId = record.get("v").asNode().get("structure_id").asLong();
    }

    return RichVersionFactory.construct(id, tags, structureVersionId, reference, referenceParameters);
  }
}
