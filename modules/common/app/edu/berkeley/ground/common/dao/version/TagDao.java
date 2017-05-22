/**
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.berkeley.ground.common.dao.version;

import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.model.version.Tag;
import edu.berkeley.ground.common.util.DbStatements;
import java.util.List;
import java.util.Map;

public interface TagDao {

  List<Long> getVersionIdsByTag(String tag) throws GroundException;

  List<Long> getItemIdsByTag(String tag) throws GroundException;

  Map<String, Tag> retrieveFromDatabaseByVersionId(long id) throws GroundException;

  Map<String, Tag> retrieveFromDatabaseByItemId(long id) throws GroundException;

  DbStatements insertItemTag(final Tag tag);

  DbStatements insertRichVersionTag(final Tag tag);
}
