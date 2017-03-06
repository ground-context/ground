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

package edu.berkeley.ground.api.models.postgres;

import edu.berkeley.ground.api.models.RichVersion;
import edu.berkeley.ground.api.models.RichVersionFactory;
import edu.berkeley.ground.api.models.StructureVersion;
import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.api.versions.GroundType;
import edu.berkeley.ground.api.versions.postgres.PostgresVersionFactory;
import edu.berkeley.ground.db.DBClient;
import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.PostgresClient.PostgresConnection;
import edu.berkeley.ground.db.QueryResults;
import edu.berkeley.ground.exceptions.EmptyResultException;
import edu.berkeley.ground.exceptions.GroundException;

import java.util.*;

public class PostgresRichVersionFactory extends RichVersionFactory {
  private PostgresVersionFactory versionFactory;
  private PostgresStructureVersionFactory structureVersionFactory;
  private PostgresTagFactory tagFactory;

  public PostgresRichVersionFactory(PostgresVersionFactory versionFactory,
                                    PostgresStructureVersionFactory structureVersionFactory,
                                    PostgresTagFactory tagFactory) {

    this.versionFactory = versionFactory;
    this.structureVersionFactory = structureVersionFactory;
    this.tagFactory = tagFactory;
  }

  public void insertIntoDatabase(GroundDBConnection connectionPointer,
                                 long id,
                                 Map<String, Tag> tags,
                                 long structureVersionId,
                                 String reference,
                                 Map<String, String> referenceParameters) throws GroundException {
    PostgresConnection connection = (PostgresConnection) connectionPointer;

    this.versionFactory.insertIntoDatabase(connection, id);

    if (structureVersionId != -1) {
      StructureVersion structureVersion = this.structureVersionFactory.retrieveFromDatabase(structureVersionId);
      RichVersionFactory.checkStructureTags(structureVersion, tags);
    }

    List<DbDataContainer> insertions = new ArrayList<>();
    insertions.add(new DbDataContainer("id", GroundType.LONG, id));
    insertions.add(new DbDataContainer("structure_version_id", GroundType.LONG, structureVersionId));
    insertions.add(new DbDataContainer("reference", GroundType.STRING, reference));

    connection.insert("rich_version", insertions);

    for (String key : tags.keySet()) {
      Tag tag = tags.get(key);

      List<DbDataContainer> tagInsertion = new ArrayList<>();
      tagInsertion.add(new DbDataContainer("rich_version_id", GroundType.LONG, id));
      tagInsertion.add(new DbDataContainer("key", GroundType.STRING, key));

      if (tag.getValue() != null) {
        tagInsertion.add(new DbDataContainer("value", GroundType.STRING, tag.getValue().toString()));
        tagInsertion.add(new DbDataContainer("type", GroundType.STRING, tag.getValueType().toString()));
      } else {
        tagInsertion.add(new DbDataContainer("value", GroundType.STRING, null));
        tagInsertion.add(new DbDataContainer("type", GroundType.STRING, null));
      }

      connection.insert("rich_version_tag", tagInsertion);
    }

    for (String key : referenceParameters.keySet()) {
      List<DbDataContainer> parameterInsertion = new ArrayList<>();

      parameterInsertion.add(new DbDataContainer("rich_version_id", GroundType.LONG, id));
      parameterInsertion.add(new DbDataContainer("key", GroundType.STRING, key));
      parameterInsertion.add(new DbDataContainer("value", GroundType.STRING, referenceParameters.get(key)));

      connection.insert("rich_version_external_parameter", parameterInsertion);
    }
  }

  public RichVersion retrieveFromDatabase(GroundDBConnection connectionPointer, long id) throws GroundException {
    PostgresConnection connection = (PostgresConnection) connectionPointer;

    List<DbDataContainer> predicates = new ArrayList<>();
    predicates.add(new DbDataContainer("id", GroundType.LONG, id));

    QueryResults resultSet;
    try {
      resultSet = connection.equalitySelect("rich_version", DBClient.SELECT_STAR, predicates);
    } catch (EmptyResultException eer) {
      throw new GroundException("No RichVersion found with id " + id + ".");
    }

    List<DbDataContainer> parameterPredicates = new ArrayList<>();
    parameterPredicates.add(new DbDataContainer("rich_version_id", GroundType.LONG, id));
    Map<String, String> referenceParameters = new HashMap<>();

    try {
      QueryResults parameterSet = connection.equalitySelect("rich_version_external_parameter", DBClient.SELECT_STAR, parameterPredicates);

      do {
        referenceParameters.put(parameterSet.getString(2), parameterSet.getString(3));
      } while (parameterSet.next());
    } catch (EmptyResultException eer) {
      // do nothing; there are no referenceParameters
    }

    Map<String, Tag> tags = tagFactory.retrieveFromDatabaseByVersionId(connection, id);

    String reference = resultSet.getString(3);
    long structureVersionId = resultSet.getLong(2);
    structureVersionId = structureVersionId == 0 ? -1 : structureVersionId;

    return RichVersionFactory.construct(id, tags, structureVersionId, reference, referenceParameters);
  }
}
