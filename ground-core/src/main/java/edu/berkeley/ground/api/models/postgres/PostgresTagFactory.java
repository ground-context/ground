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

package edu.berkeley.ground.api.models.postgres;

import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.api.models.TagFactory;
import edu.berkeley.ground.api.versions.GroundType;
import edu.berkeley.ground.db.DBClient;
import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.PostgresClient.PostgresConnection;
import edu.berkeley.ground.db.PostgresResults;
import edu.berkeley.ground.db.QueryResults;
import edu.berkeley.ground.exceptions.EmptyResultException;
import edu.berkeley.ground.exceptions.GroundException;

import java.util.*;

public class PostgresTagFactory extends TagFactory {
    public Map<String, Tag> retrieveFromDatabaseById(GroundDBConnection connectionPointer, String id) throws GroundException {
        PostgresConnection connection = (PostgresConnection) connectionPointer;

        List<DbDataContainer> predicates = new ArrayList<>();
        predicates.add(new DbDataContainer("richversion_id", GroundType.STRING, id));

        Map<String, Tag> result = new HashMap<>();

        QueryResults resultSet = null;
        try {
            resultSet = connection.equalitySelect("Tags", DBClient.SELECT_STAR, predicates);
        } catch (EmptyResultException eer) {
            return new HashMap<>();
        }

        do {
            String key = resultSet.getString(2);
            GroundType type = GroundType.fromString(resultSet.getString(4));

            String valueString = resultSet.getString(3);
            Object value = GroundType.stringToType(valueString, type);

            result.put(key, new Tag(id, key, value, type));
        } while (resultSet.next());

        return result;
    }

    public List<String> getIdsByTag(GroundDBConnection connectionPointer, String tag) throws GroundException {
        PostgresConnection connection = (PostgresConnection) connectionPointer;

        List<DbDataContainer> predicates = new ArrayList<>();
        predicates.add(new DbDataContainer("key", GroundType.STRING, tag));

        List<String> projections = new ArrayList<>();
        projections.add("richversion_id");

        QueryResults resultSet = null;
        try {
            resultSet = connection.equalitySelect("Tags", DBClient.SELECT_STAR, predicates);
        } catch (EmptyResultException eer) {
            return new ArrayList<>();
        }

        List<String> result = new ArrayList<>();

        while (resultSet.next()) {
            result.add(resultSet.getString(1));
        }

        return result;
    }
}
