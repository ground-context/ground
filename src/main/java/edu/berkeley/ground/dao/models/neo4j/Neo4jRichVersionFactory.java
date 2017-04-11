/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.berkeley.ground.dao.models.neo4j;

import edu.berkeley.ground.dao.models.RichVersionFactory;
import edu.berkeley.ground.dao.versions.neo4j.Neo4jVersionFactory;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.Neo4jClient;
import edu.berkeley.ground.exceptions.GroundDbException;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.exceptions.GroundVersionNotFoundException;
import edu.berkeley.ground.model.models.RichVersion;
import edu.berkeley.ground.model.models.StructureVersion;
import edu.berkeley.ground.model.models.Tag;
import edu.berkeley.ground.model.versions.GroundType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.internal.value.NullValue;
import org.neo4j.driver.internal.value.StringValue;
import org.neo4j.driver.v1.Record;

public abstract  class Neo4jRichVersionFactory<T extends RichVersion>
    extends Neo4jVersionFactory<T>
    implements RichVersionFactory<T> {
  private final Neo4jClient dbClient;
  private final Neo4jStructureVersionFactory structureVersionFactory;
  private final Neo4jTagFactory tagFactory;

  /**
   * Constructor for the Neo4j rich version factory.
   *
   * @param dbClient the Neo4j client
   * @param structureVersionFactory the singleton Neo4jStructureVerisonFactory
   * @param tagFactory the singleton Neo4jTagFactory
   */
  public Neo4jRichVersionFactory(Neo4jClient dbClient,
                                 Neo4jStructureVersionFactory structureVersionFactory,
                                 Neo4jTagFactory tagFactory) {
    this.dbClient = dbClient;
    this.structureVersionFactory = structureVersionFactory;
    this.tagFactory = tagFactory;
  }

  /**
   * Persist rich version data in the database.
   *
   * @param id the id of the rich version
   * @param tags tags associated with this version
   * @param structureVersionId the id of the StructureVersion associated with this version
   * @param reference an optional external reference
   * @param referenceParameters access parameters for the reference
   * @throws GroundException an error while persisting data
   */
  @Override
  public void insertIntoDatabase(long id,
                                 Map<String, Tag> tags,
                                 long structureVersionId,
                                 String reference,
                                 Map<String, String> referenceParameters
  ) throws GroundException {
    if (structureVersionId != -1) {
      StructureVersion structureVersion =
          this.structureVersionFactory.retrieveFromDatabase(structureVersionId);
      RichVersionFactory.checkStructureTags(structureVersion, tags);
    }

    for (String key : referenceParameters.keySet()) {
      String value = referenceParameters.get(key);

      List<DbDataContainer> insertions = new ArrayList<>();
      insertions.add(new DbDataContainer("rich_version_id", GroundType.LONG, id));
      insertions.add(new DbDataContainer("pkey", GroundType.STRING, key));
      insertions.add(new DbDataContainer("value", GroundType.STRING, value));

      this.dbClient.addVertexAndEdge("RichVersionExternalParameter", insertions,
          "RichVersionExternalParameterConnection", id, new ArrayList<>());

      insertions.clear();
    }

    if (structureVersionId != -1) {
      this.dbClient.setProperty(id, "structure_id", structureVersionId, false);
    }

    if (reference != null) {
      this.dbClient.setProperty(id, "reference", reference, true);
    }

    for (String key : tags.keySet()) {
      Tag tag = tags.get(key);

      List<DbDataContainer> tagInsertion = new ArrayList<>();
      tagInsertion.add(new DbDataContainer("rich_version_id", GroundType.LONG, id));
      tagInsertion.add(new DbDataContainer("tkey", GroundType.STRING, key));

      if (tag.getValue() != null) {
        tagInsertion.add(new DbDataContainer("value", GroundType.STRING,
            tag.getValue().toString()));
        tagInsertion.add(new DbDataContainer("type", GroundType.STRING,
            tag.getValueType().toString()));
      } else {
        tagInsertion.add(new DbDataContainer("value", GroundType.STRING, null));
        tagInsertion.add(new DbDataContainer("type", GroundType.STRING, null));
      }

      this.dbClient.addVertexAndEdge("RichVersionTag", tagInsertion, "RichVersionTagConnection",
          id, new ArrayList<>());
      tagInsertion.clear();
    }
  }

  /**
   * Retrieve rich version data from the database.
   *
   * @param id the id of the rich version
   * @return the retrieved rich version
   * @throws GroundException either the rich version didn't exist or couldn't be retrieved
   */
  public RichVersion retrieveRichVersionData(long id) throws GroundException {
    List<DbDataContainer> predicates = new ArrayList<>();
    predicates.add(new DbDataContainer("id", GroundType.LONG, id));

    Record record = this.dbClient.getVertex(predicates);

    if (record == null) {
      throw new GroundVersionNotFoundException(this.getType(), id);
    }

    List<String> returnFields = new ArrayList<>();
    returnFields.add("pkey");
    returnFields.add("value");

    List<Record> parameterVertices = this.dbClient.getAdjacentVerticesByEdgeLabel(
        "RichVersionExternalParameterConnection", id, returnFields);
    Map<String, String> referenceParameters = new HashMap<>();

    if (!parameterVertices.isEmpty()) {
      for (Record parameter : parameterVertices) {
        referenceParameters.put(Neo4jClient.getStringFromValue((StringValue) parameter.get("pkey")),
            Neo4jClient.getStringFromValue((StringValue) parameter.get("value")));
      }
    }

    Map<String, Tag> tags = tagFactory.retrieveFromDatabaseByVersionId(id);

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

    return new RichVersion(id, tags, structureVersionId, reference, referenceParameters);
  }
}
