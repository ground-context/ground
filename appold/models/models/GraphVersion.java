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

package models.models;




import java.util.List;
import java.util.Map;

public class GraphVersion extends RichVersion {
  // the id of the Graph that contains this Version
  private final long graphId;

  // the list of ids of EdgeVersions in this GraphVersion
  private final List<Long> edgeVersionIds;

  /**
   * Create a new graph version.
   *
   * @param id the id of this graph version
   * @param tags the tags associated with this graph version
   * @param structureVersionId the id of the StructureVersion associated with this graph version
   * @param reference an optional external reference
   * @param referenceParameters the access parameters of the reference
   * @param graphId the id of the graph containing this version
   * @param edgeVersionIds the list of edge versions in this graph version
   */
  
  public GraphVersion(("id") long id,
                      ("tags") Map<String, Tag> tags,
                      ("structureVersionId") long structureVersionId,
                      ("reference") String reference,
                      ("referenceParameters") Map<String, String> referenceParameters,
                      ("graphId") long graphId,
                      ("edgeVersionIds") List<Long> edgeVersionIds) {

    super(id, tags, structureVersionId, reference, referenceParameters);

    this.graphId = graphId;
    this.edgeVersionIds = edgeVersionIds;
  }

  
  public long getGraphId() {
    return this.graphId;
  }

  
  public List<Long> getEdgeVersionIds() {
    return this.edgeVersionIds;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof GraphVersion)) {
      return false;
    }

    GraphVersion otherGraphVersion = (GraphVersion) other;

    return this.graphId == otherGraphVersion.graphId
        && this.edgeVersionIds.equals(otherGraphVersion.edgeVersionIds)
        && super.equals(other);
  }
}
