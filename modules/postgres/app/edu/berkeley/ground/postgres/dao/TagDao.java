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

import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.factory.version.TagFactory;
import edu.berkeley.ground.common.model.version.Tag;

import java.util.*;

import edu.berkeley.ground.common.utils.IdGenerator;
import edu.berkeley.ground.postgres.utils.PostgresUtils;
import play.db.Database;

public class TagDao implements TagFactory {

  public final void create(final Database dbSource, final Tag tag, final IdGenerator idGenerator) throws GroundException {
    long uniqueId = idGenerator.generateItemId();
    Tag newTag = new Tag(uniqueId, tag.getKey(), tag.getValue(), tag.getValueType());
    try {
      PostgresUtils.executeSqlList(dbSource, createSqlList(newTag));
    } catch (Exception e) {
      throw new GroundException(e);
    }
  }

  public List<String> createSqlList(final Tag tag) {
    List<String> sqlList = new ArrayList<>();
    sqlList.add(String.format("insert into item_tag (item_id, key, value, type) values (%d, '%s', '%s', '%s')",
      tag.getId(), tag.getKey(), tag.getValue(), tag.getValueType()));
    return sqlList;
  }

  @Override
  public List<Long> getVersionIdsByTag(String tag) throws GroundException {
    return null;
  }

  @Override
  public List<Long> getItemIdsByTag(String tag) throws GroundException {
    return null;
  }
}
