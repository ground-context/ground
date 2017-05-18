/**
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * <p>
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.berkeley.ground.postgres.dao.core;

import com.fasterxml.jackson.databind.JsonNode;
import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.factory.core.RichVersionFactory;
import edu.berkeley.ground.common.model.core.RichVersion;
import edu.berkeley.ground.common.model.core.StructureVersion;
import edu.berkeley.ground.common.model.version.GroundType;
import edu.berkeley.ground.common.model.version.Tag;
import edu.berkeley.ground.common.utils.IdGenerator;
import edu.berkeley.ground.postgres.dao.version.VersionDao;
import edu.berkeley.ground.postgres.utils.PostgresStatements;
import edu.berkeley.ground.postgres.utils.PostgresUtils;
import play.db.Database;
import play.libs.Json;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class RichVersionDao<T extends RichVersion> extends VersionDao<T> implements RichVersionFactory<T> {

  public RichVersionDao() {
  }

  public RichVersionDao(Database dbSource, IdGenerator idGenerator) {
    super(dbSource, idGenerator);
  }

  @Override
  public T create(T RichVersion, List<Long> parentIds) throws GroundException {
    return null;
  }

  @Override
  public RichVersion retrieveFromDatabase(long id) throws GroundException {
    String sql = String.format("select * from rich_version where id=%d", id);
    JsonNode json = Json.parse(PostgresUtils.executeQueryToJson(dbSource, sql));
    if (json.size() == 0) {
      throw new GroundException(String.format("Rich Version with id %d does not exist.", id));
    }
    return Json.fromJson(json.get(0), RichVersion.class);
  }

  static Map<String, Tag> addIdToTags(long id, Map<String, Tag> tags) throws GroundException {

    Function<Tag, Tag> addId =
      (Tag t) -> {
        try {
          return new Tag(id, t.getKey(), t.getValue(), t.getValueType());
        } catch (GroundException e) {
          throw new RuntimeException(e);
        }
      };

    return tags.values().stream().collect(Collectors.toMap(Tag::getKey, addId));
  }

  /**
   * Validate that the given Tags satisfy the StructureVersion's requirements.
   *
   * @param structureVersion the StructureVersion to check against
   * @param tags             the provided tags
   */
  static void checkStructureTags(StructureVersion structureVersion, Map<String, Tag> tags)
    throws GroundException {

    Map<String, GroundType> structureVersionAttributes = structureVersion.getAttributes();

    if (tags.isEmpty()) {
      throw new GroundException("No tags were specified");
    }

    for (String key : structureVersionAttributes.keySet()) {
      if (!tags.keySet().contains(key)) {
        // check if such a tag exists
        throw new GroundException("No tag with key " + key + " was specified.");
      } else if (tags.get(key).getValueType() == null) {
        // check that value type is specified
        throw new GroundException("Tag with key " + key + " did not have a value.");
      } else if (!tags.get(key).getValueType().equals(structureVersionAttributes.get(key))) {
        // check that the value type is the same
        throw new GroundException(
          "Tag with key "
            + key
            + " did not have a value of the correct type: expected ["
            + structureVersionAttributes.get(key)
            + "] but found ["
            + tags.get(key).getValueType()
            + "].");
      }
    }
  }

  @Override
  public PostgresStatements insert(final T richVersion) throws GroundException {
    Long structureVersionId;
    if (richVersion.getStructureVersionId() == -1) {
      structureVersionId = null;
    } else {
      structureVersionId = richVersion.getStructureVersionId();
      checkStructureTags(new StructureVersionDao(dbSource, idGenerator)
        .retrieveFromDatabase(richVersion.getStructureVersionId()), richVersion.getTags());
    }
    PostgresStatements statements = super.insert(richVersion);
    statements.append(
      String.format(
        "insert into rich_version (id, structure_version_id, reference) values (%d, %d, \'%s\')",
        richVersion.getId(), structureVersionId, richVersion.getReference()));
    final Map<String, Tag> tags = richVersion.getTags();
    if (tags != null) {
      for (String key : tags.keySet()) {
        Tag tag = tags.get(key);

        if (tag.getValue() != null) {
          statements.append(
            String.format(
              "insert into rich_version_tag (rich_version_id, key, value, type) values (%d, \'%s\', \'%s\', \'%s\')",
              richVersion.getId(), key, tag.getValue().toString(), tag.getValueType().toString()));
        } else {
          statements.append(
            String.format(
              "insert into rich_version_tag (rich_version_id, key, value, type) values (%d, \'%s\', \'%s\', \'%s\')",
              richVersion.getId(), key, null, null));
        }
      }
    }
    return statements;
  }

}
