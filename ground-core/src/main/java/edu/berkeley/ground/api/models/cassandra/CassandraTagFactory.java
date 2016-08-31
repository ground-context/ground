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

package edu.berkeley.ground.api.models.cassandra;

import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.api.models.TagFactory;
import edu.berkeley.ground.api.versions.GroundType;
import edu.berkeley.ground.db.CassandraClient.CassandraConnection;
import edu.berkeley.ground.db.DBClient;
import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.QueryResults;
import edu.berkeley.ground.exceptions.GroundException;

import java.util.*;

public class CassandraTagFactory extends TagFactory {
    public Optional<Map<String, Tag>> retrieveFromDatabaseById(GroundDBConnection connectionPointer, String id) throws GroundException {
        CassandraConnection connection = (CassandraConnection) connectionPointer;

        List<DbDataContainer> predicates = new ArrayList<>();
        predicates.add(new DbDataContainer("richversion_id", GroundType.STRING, id));

        QueryResults resultSet = connection.equalitySelect("Tags", DBClient.SELECT_STAR, predicates);
        Map<String, Tag> result = new HashMap<>();

        do {
            String key = resultSet.getString(1);
            Optional<GroundType> type = Optional.ofNullable(GroundType.fromString(resultSet.getString(3)));

            String valueString = resultSet.getString(2);
            Optional<Object> value = type.map(t -> GroundType.stringToType(valueString, t));

            result.put(key, new Tag(id, key, value, type));
        } while (resultSet.next());

        if (result.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(result);
        }
    }
}
