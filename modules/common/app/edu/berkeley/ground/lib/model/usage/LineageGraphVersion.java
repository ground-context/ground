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

package edu.berkeley.ground.lib.model.usage;

import java.util.List;
import java.util.Map;

import edu.berkeley.ground.lib.model.core.RichVersion;
import edu.berkeley.ground.lib.model.version.Tag;

public class LineageGraphVersion extends RichVersion {
  // the id of the LineageGraph that contains this version
  private final long lineageGraphId;

  // the list of ids of LineageEdgeVersions that are in this LineageGraph
  private final List<Long> lineageEdgeVersionIds;

  /**
   * Create a lineage graph version.
   *
   * @param id
   *          the id of the lineage graph version
   * @param tags
   *          the tags associated with this version
   * @param structureVersionId
   *          the id of the StructureVersion associated with this version
   * @param reference
   *          an optional external reference
   * @param referenceParameters
   *          the access parameters for the reference
   * @param lineageGraphId
   *          the id of the lineage graph containing this version
   * @param lineageEdgeVersionIds
   *          the ids of the lineage edges in this lineage graph version
   */

  public LineageGraphVersion(long id, Map<String, Tag> tags, long structureVersionId,
      String reference, Map<String, String> referenceParameters, long lineageGraphId,
      List<Long> lineageEdgeVersionIds) {

    super(id, tags, structureVersionId, reference, referenceParameters);

    this.lineageGraphId = lineageGraphId;
    this.lineageEdgeVersionIds = lineageEdgeVersionIds;
  }

  public long getLineageGraphId() {
    return this.lineageGraphId;
  }

  public List<Long> getLineageEdgeVersionIds() {
    return this.lineageEdgeVersionIds;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof LineageGraphVersion)) {
      return false;
    }

    LineageGraphVersion otherLineageGraphVersion = (LineageGraphVersion) other;

    return this.lineageGraphId == otherLineageGraphVersion.lineageGraphId
        && this.lineageEdgeVersionIds.equals(otherLineageGraphVersion.lineageEdgeVersionIds)
        && super.equals(other);
  }
}
