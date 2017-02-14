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

package edu.berkeley.ground.api.models.cassandra;

import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.api.models.TagFactory;
import edu.berkeley.ground.api.versions.GroundType;
import edu.berkeley.ground.db.CassandraClient.CassandraConnection;
import edu.berkeley.ground.db.DBClient;
import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.QueryResults;
import edu.berkeley.ground.exceptions.EmptyResultException;
import edu.berkeley.ground.exceptions.GroundException;

import java.util.*;

public class CassandraTagFactory extends TagFactory {
  public Map<String, Tag> retrieveFromDatabaseById(GroundDBConnection connectionPointer, long id) throws GroundException {
    CassandraConnection connection = (CassandraConnection) connectionPointer;

    List<DbDataContainer> predicates = new ArrayList<>();
    predicates.add(new DbDataContainer("rich_version_id", GroundType.LONG, id));

    Map<String, Tag> result = new HashMap<>();

    QueryResults resultSet;
    try {
      resultSet = connection.equalitySelect("tag", DBClient.SELECT_STAR, predicates);
    } catch (EmptyResultException eer) {
      // this means that there are no tags
      return result;
    }

    while (resultSet.next()) {
      String key = resultSet.getString("key");

      // these methods will return null if the input is null, so there's no need to check
      GroundType type = GroundType.fromString(resultSet.getString("type"));

      String valueString = resultSet.getString("value");
      Object value = GroundType.stringToType(valueString, type);

      result.put(key, new Tag(id, key, value, type));
    }

    return result;
  }

  public List<Long> getIdsByTag(GroundDBConnection connectionPointer, String tag) throws GroundException {
    CassandraConnection connection = (CassandraConnection) connectionPointer;
    List<Long> result = new ArrayList<>();

    List<DbDataContainer> predicates = new ArrayList<>();
    predicates.add(new DbDataContainer("key", GroundType.STRING, tag));

    List<String> projections = new ArrayList<>();
    projections.add("rich_version_id");

    QueryResults resultSet;
    try {
      resultSet = connection.equalitySelect("tag", projections, predicates);
    } catch (EmptyResultException eer) {
      // this means that there are no tags
      return result;
    }

    while (resultSet.next()) {
      result.add(resultSet.getLong(0));
    }

    return result;
  }
}
