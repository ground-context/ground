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

package util;

import dao.models.EdgeFactory;
import dao.models.EdgeVersionFactory;
import dao.models.GraphFactory;
import dao.models.GraphVersionFactory;
import dao.models.NodeFactory;
import dao.models.NodeVersionFactory;
import dao.models.StructureFactory;
import dao.models.StructureVersionFactory;
import dao.usage.LineageEdgeFactory;
import dao.usage.LineageEdgeVersionFactory;
import dao.usage.LineageGraphFactory;
import dao.usage.LineageGraphVersionFactory;
import db.DbClient;

public interface FactoryGenerator {
  EdgeFactory getEdgeFactory();

  EdgeVersionFactory getEdgeVersionFactory();

  GraphFactory getGraphFactory();

  GraphVersionFactory getGraphVersionFactory();

  NodeFactory getNodeFactory();

  NodeVersionFactory getNodeVersionFactory();

  LineageEdgeFactory getLineageEdgeFactory();

  LineageEdgeVersionFactory getLineageEdgeVersionFactory();

  StructureFactory getStructureFactory();

  StructureVersionFactory getStructureVersionFactory();

  LineageGraphFactory getLineageGraphFactory();

  LineageGraphVersionFactory getLineageGraphVersionFactory();

  DbClient getDbClient();
}
