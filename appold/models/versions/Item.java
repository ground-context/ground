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
package models.versions;

import edu.berkeley.ground.model.version.Tag;
import java.util.Map;

public class Item<T extends Version> {

  private final long id;

  private final Map<String, Tag> tags;

  public Item(long id, Map<String, Tag> tags) {
    this.id = id;
    this.tags = tags;
  }

  public long getId() {
    return this.id;
  }

  public Map<String, Tag> getTags() {
    return this.tags;
  }
}
