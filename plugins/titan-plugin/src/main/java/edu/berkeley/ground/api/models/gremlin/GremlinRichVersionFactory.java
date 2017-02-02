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

package edu.berkeley.ground.api.models.gremlin;

import edu.berkeley.ground.api.models.RichVersion;
import edu.berkeley.ground.api.models.RichVersionFactory;
import edu.berkeley.ground.api.models.StructureVersion;
import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.api.versions.GroundType;
import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.GremlinClient.GremlinConnection;
import edu.berkeley.ground.exceptions.EmptyResultException;
import edu.berkeley.ground.exceptions.GroundException;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.*;

public class GremlinRichVersionFactory extends RichVersionFactory {
  private GremlinStructureVersionFactory structureVersionFactory;
  private GremlinTagFactory tagFactory;

  public GremlinRichVersionFactory(GremlinStructureVersionFactory structureVersionFactory,
                                   GremlinTagFactory tagFactory) {
    this.structureVersionFactory = structureVersionFactory;
    this.tagFactory = tagFactory;
  }

  public void insertIntoDatabase(GroundDBConnection connectionPointer,
                                 String id,
                                 Map<String, Tag> tags,
                                 String structureVersionId,
                                 String reference,
                                 Map<String, String> referenceParameters
  ) throws GroundException {
    GremlinConnection connection = (GremlinConnection) connectionPointer;

    List<DbDataContainer> predicates = new ArrayList<>();
    predicates.add(new DbDataContainer("id", GroundType.STRING, id));

    Vertex versionVertex = null;
    try {
      versionVertex = connection.getVertex(predicates);
    } catch (EmptyResultException eer) {
      throw new GroundException("No RichVersion found with id " + id + ".");
    }

    if (structureVersionId != null) {
      StructureVersion structureVersion = this.structureVersionFactory.retrieveFromDatabase(structureVersionId);
      RichVersionFactory.checkStructureTags(structureVersion, tags);
    }

    if (!referenceParameters.isEmpty()) {
      Map<String, String> referenceParametersMap = referenceParameters;

      for (String key : referenceParametersMap.keySet()) {
        String value = referenceParametersMap.get(key);

        List<DbDataContainer> insertions = new ArrayList<>();
        insertions.add(new DbDataContainer("richversion_id", GroundType.STRING, id));
        insertions.add(new DbDataContainer("pkey", GroundType.STRING, key));
        insertions.add(new DbDataContainer("value", GroundType.STRING, value));

        Vertex parameterVertex = connection.addVertex("RichVersionExternalParameter", insertions);

        insertions.clear();
        connection.addEdge("RichVersionExternalParameterConnection", versionVertex, parameterVertex, insertions);
      }
    }

    if (structureVersionId != null) {
      versionVertex.property("structureversion_id", structureVersionId);
    }

    if (reference != null) {
      versionVertex.property("reference", reference);
    }

    if (!tags.isEmpty()) {
      for (String key : tags.keySet()) {
        Tag tag = tags.get(key);

        List<DbDataContainer> tagInsertion = new ArrayList<>();
        tagInsertion.add(new DbDataContainer("richversion_id", GroundType.STRING, id));
        tagInsertion.add(new DbDataContainer("tkey", GroundType.STRING, key));

        if (tag.getValue() != null) {
          tagInsertion.add(new DbDataContainer("value", GroundType.STRING, tag.getValue().toString()));
          tagInsertion.add(new DbDataContainer("type", GroundType.STRING, tag.getValueType().toString()));
        }

        Vertex tagVertex = connection.addVertex("Tag", tagInsertion);
        connection.addEdge("TagConnection", versionVertex, tagVertex, new ArrayList<>());
      }
    }
  }

  public RichVersion retrieveFromDatabase(GroundDBConnection connectionPointer, String id) throws GroundException {
    GremlinConnection connection = (GremlinConnection) connectionPointer;

    List<DbDataContainer> predicates = new ArrayList<>();
    predicates.add(new DbDataContainer("id", GroundType.STRING, id));

    Vertex versionVertex = null;
    try {
      versionVertex = connection.getVertex(predicates);
    } catch (EmptyResultException eer) {
      throw new GroundException("No RichVersion found with id " + id + ".");
    }

    List<Vertex> parameterVertices = connection.getAdjacentVerticesByEdgeLabel(versionVertex, "RichVersionExternalParameterConnection");
    Map<String, String> referenceParameters = new HashMap<>();

    for (Vertex parameter : parameterVertices) {
      referenceParameters.put(parameter.property("pkey").value().toString(), parameter.property("value").value().toString());
    }

    Map<String, Tag> tags = tagFactory.retrieveFromDatabaseById(connectionPointer, id);

    String reference = null;
    if (versionVertex.property("reference").isPresent()) {
      reference = versionVertex.property("reference").value().toString();
    }


    String structureVersionId = null;
    if (versionVertex.property("structureversion_id").isPresent()) {
      structureVersionId = versionVertex.property("structureversion_id").value().toString();
    }

    return RichVersionFactory.construct(id, tags, structureVersionId, reference, referenceParameters);
  }
}
