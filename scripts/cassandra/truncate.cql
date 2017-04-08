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

truncate lineage_graph_version_edge;
truncate lineage_graph_version;
truncate lineage_graph;
truncate lineage_edge_version;
truncate lineage_edge;
truncate principal;
truncate graph_version_edge;
truncate graph_version;
truncate edge_version;
truncate node_version;
truncate graph;
truncate node;
truncate edge;
truncate rich_version_tag;
truncate rich_version_external_parameter;
truncate rich_version;
truncate structure_version_attribute;
truncate structure_version;
truncate structure;
truncate version_history_dag;
truncate item_tag;
truncate item;
truncate version_successor;
truncate version;

insert into version(id) values (0);
