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

package dao.models.cassandra;

import dao.models.RichVersionFactory;
import dao.versions.cassandra.CassandraVersionFactory;
import db.CassandraClient;
import db.CassandraResults;
import db.DbClient;
import db.DbDataContainer;
import exceptions.GroundException;
import exceptions.GroundVersionNotFoundException;
import models.models.RichVersion;
import models.models.StructureVersion;
import models.models.Tag;
import models.versions.GroundType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class CassandraRichVersionFactory<T extends RichVersion>
    extends CassandraVersionFactory<T>
    implements RichVersionFactory<T> {

  private final CassandraClient dbClient;
  private final CassandraStructureVersionFactory structureVersionFactory;
  private final CassandraTagFactory tagFactory;

  /**
   * Constructor for the Cassandra rich version factory.
   *
   * @param dbClient the Cassandra client
   * @param structureVersionFactory the singleton CassandraStructureVerisonFactory
   * @param tagFactory the singleton CassandraTagFactory
   */
  public CassandraRichVersionFactory(CassandraClient dbClient,
                                     CassandraStructureVersionFactory structureVersionFactory,
                                     CassandraTagFactory tagFactory) {

    super(dbClient);

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
                                 Map<String, String> referenceParameters) throws GroundException {
    super.insertIntoDatabase(id);

    if (structureVersionId != -1) {
      StructureVersion structureVersion = this.structureVersionFactory
          .retrieveFromDatabase(structureVersionId);

      RichVersionFactory.checkStructureTags(structureVersion, tags);
    }

    List<DbDataContainer> insertions = new ArrayList<>();
    insertions.add(new DbDataContainer("id", GroundType.LONG, id));
    insertions.add(new DbDataContainer("structure_version_id", GroundType.LONG,
        structureVersionId));
    insertions.add(new DbDataContainer("reference", GroundType.STRING, reference));

    this.dbClient.insert("rich_version", insertions);

    for (String key : tags.keySet()) {
      Tag tag = tags.get(key);

      List<DbDataContainer> tagInsertion = new ArrayList<>();
      tagInsertion.add(new DbDataContainer("rich_version_id", GroundType.LONG, id));
      tagInsertion.add(new DbDataContainer("key", GroundType.STRING, key));

      if (tag.getValue() != null) {
        tagInsertion.add(new DbDataContainer("value", GroundType.STRING,
            tag.getValue().toString()));
        tagInsertion.add(new DbDataContainer("type", GroundType.STRING,
            tag.getValueType().toString()));
      } else {
        tagInsertion.add(new DbDataContainer("value", GroundType.STRING, null));
        tagInsertion.add(new DbDataContainer("type", GroundType.STRING, null));
      }

      this.dbClient.insert("rich_version_tag", tagInsertion);
    }

    for (String key : referenceParameters.keySet()) {
      List<DbDataContainer> parameterInsertion = new ArrayList<>();
      parameterInsertion.add(new DbDataContainer("rich_version_id", GroundType.LONG, id));
      parameterInsertion.add(new DbDataContainer("key", GroundType.STRING, key));
      parameterInsertion.add(new DbDataContainer("value", GroundType.STRING,
          referenceParameters.get(key)));

      this.dbClient.insert("rich_version_external_parameter", parameterInsertion);
    }
  }

  /**
   * Retrieve rich version data from the database.
   *
   * @param id the id of the rich version
   * @return the retrieved rich version
   * @throws GroundException either the rich version didn't exist or couldn't be retrieved
   */
  protected RichVersion retrieveRichVersionData(long id) throws GroundException {
    List<DbDataContainer> predicates = new ArrayList<>();
    predicates.add(new DbDataContainer("id", GroundType.LONG, id));

    CassandraResults resultSet = this.dbClient.equalitySelect("rich_version",
        DbClient.SELECT_STAR,
        predicates);
    if (resultSet.isEmpty()) {
      throw new GroundVersionNotFoundException(RichVersion.class, id);
    }

    List<DbDataContainer> parameterPredicates = new ArrayList<>();
    parameterPredicates.add(new DbDataContainer("rich_version_id", GroundType.LONG, id));
    Map<String, String> referenceParameters = new HashMap<>();
    CassandraResults parameterSet = this.dbClient.equalitySelect("rich_version_external_parameter",
        DbClient.SELECT_STAR, parameterPredicates);

    if (!parameterSet.isEmpty()) {
      do {
        referenceParameters.put(parameterSet.getString("key"), parameterSet.getString("value"));
      } while (parameterSet.next());
    }

    Map<String, Tag> tags = this.tagFactory.retrieveFromDatabaseByVersionId(id);

    String reference = resultSet.getString("reference");
    long structureVersionId = resultSet.getLong("structure_version_id");

    return new RichVersion(id, tags, structureVersionId, reference, referenceParameters);
  }
}
