
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--    http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.


TRUNCATE lineage_graph_version_edge;
TRUNCATE lineage_graph_version;
TRUNCATE lineage_graph;
TRUNCATE lineage_edge_version;
TRUNCATE lineage_edge;
TRUNCATE principal;
TRUNCATE graph_version_edge;
TRUNCATE graph_version;
TRUNCATE edge_version;
TRUNCATE node_version;
TRUNCATE graph;
TRUNCATE node;
TRUNCATE edge;
TRUNCATE rich_version_tag;
TRUNCATE rich_version_external_parameter;
TRUNCATE rich_version;
TRUNCATE structure_version_attribute;
TRUNCATE structure_version;
TRUNCATE structure;
TRUNCATE version_history_dag;
TRUNCATE item_tag;
TRUNCATE item;
TRUNCATE version_successor;
TRUNCATE version;

INSERT into version(id) values (0);
