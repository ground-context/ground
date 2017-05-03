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
package edu.berkeley.ground.postgres.dao;

import com.fasterxml.jackson.databind.JsonNode;
import edu.berkeley.ground.lib.exception.GroundException;
import edu.berkeley.ground.lib.factory.version.ItemFactory;
import edu.berkeley.ground.lib.model.version.Item;
import edu.berkeley.ground.lib.model.version.Tag;
import edu.berkeley.ground.postgres.utils.IdGenerator;
import edu.berkeley.ground.postgres.utils.PostgresUtils;
import edu.berkeley.ground.lib.model.core.StructureVersion;
import edu.berkeley.ground.lib.model.core.RichVersion;
import edu.berkeley.ground.lib.factory.core.RichVersionFactory;
import edu.berkeley.ground.lib.model.version.GroundType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import edu.berkeley.ground.lib.model.version.Tag;
import java.util.function.Function;
import java.util.stream.Collectors;

import play.libs.Json;
import play.db.Database;

public class RichVersionDao<T extends RichVersion> implements RichVersionFactory<T> {
  public void create(final Database dbSource, final T richVersion, final IdGenerator idGenerator) throws GroundException {
    final List<String> sqlList = createSqlList(richVersion);
    PostgresUtils.executeSqlList(dbSource, sqlList);
  }

  @Override
  public T retrieveFromDatabase(Database dbSource, long id) throws GroundException{
      return null;
  }

  @Override
  public void insertIntoDatabase(long id,
      RichVersion richVersion)
      throws GroundException {
        
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
   * @param tags the provided tags
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

  public List<String> createSqlList(final T richVersion) throws GroundException {
    final List<String> sqlList = new ArrayList<>();
    sqlList.add(
      String.format(
        "insert into rich_version (id, structure_version_id, reference) values (%d, %d, %s)",
        richVersion.getId(), richVersion.getStructureVersionId(), richVersion.getReference()));
    final Map<String, Tag> tags = richVersion.getTags();
    if (tags != null) {
      for (String key : tags.keySet()) {
        Tag tag = tags.get(key);

        if (tag.getValue() != null) {
          sqlList.add(
            String.format(
              "insert into rich_version_tag (rich_version_id, key, value, type) values (%d, %s, %s, %s)",
              richVersion.getId(), key, tag.getValue().toString(), tag.getValueType().toString()));
        } else {
          sqlList.add(
            String.format(
              "insert into rich_version_tag (rich_version_id, key, value, type) values (%d, %s, %s, %s)",
              richVersion.getId(), key, null, null));
        }
      }
    }
    return sqlList;
  }



}