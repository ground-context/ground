use ground;

drop table lineageedgeversions;
drop table lineageedges;
drop table principals;
drop table workflows;
drop table graphversionedges;
drop table graphversions;
drop table edgeversions;
drop table nodeversions;
drop table graphs;
drop table nodes;
drop table edges;
drop table tags;
drop table richversionexternalparameters;
drop table richversions;
drop table structureversionitems;
drop table structureversions;
drop table structures;
drop table versionhistorydags;
drop table items;
drop table versionsuccessors;
drop table versions;

/* VERSIONS */

create table Versions (id varchar primary key);

create table VersionSuccessors (
    successor_id varchar primary key,
    vfrom varchar, 
    vto varchar
);

create table Items (
    id varchar primary key
);

create table VersionHistoryDAGs (
    item_id varchar,
    successor_id varchar,
    primary key(item_id, successor_id)
);

/* MODELS */

create table Structures (
    item_id varchar,
    name varchar,
    primary key(item_id, name)
);

create table StructureVersions (
    id varchar primary key,
    structure_id varchar
);

create table StructureVersionItems (
    svid varchar,
    key varchar,
    type varchar,
    primary key(svid, key)
);

create table RichVersions (
    id varchar primary key,
    structure_id varchar,
    reference varchar
);

create table RichVersionExternalParameters (
    richversion_id varchar,
    key varchar,
    value varchar,
    primary key (richversion_id, key)
);

create table Tags (
    richversion_id varchar,
    key varchar,
    value varchar,
    type varchar,
    primary key(richversion_id, key)
);

create table Edges (
    item_id varchar,
    name varchar,
    primary key(item_id, name)
);

create table Nodes (
    item_id varchar,
    name varchar,
    primary key(item_id, name)
);

create table Graphs (
    item_id varchar,
    name varchar,
    primary key(item_id, name)
);

create table NodeVersions (
    id varchar primary key,
    node_id varchar,
);

create table EdgeVersions (
    id varchar primary key,
    edge_id varchar,
    endpoint_one varchar,
    endpoint_two varchar,
);

create table GraphVersions (
    id varchar primary key,
    graph_id varchar,
);

create table GraphVersionEdges (
    gvid varchar,
    evid varchar,
    primary key(gvid, evid)
);

/* USAGE */

create table Workflows (
    graph_id varchar,
    name varchar,
    primary key(graph_id, name)
);

create table Principals (
    node_id varchar,
    name varchar,
    primary key(node_id, name)
);

create table LineageEdges (
    item_id varchar,
    name varchar,
    primary key(item_id, name)
);

create table LineageEdgeVersions (
    id varchar primary key,
    lineageedge_id varchar,
    endpoint_one varchar,
    endpoint_two varchar,
    workflow_id varchar,
    principal_id varchar,
);

/* CREATE EMPTY VERSION */

insert into Versions(id) values ('EMPTY');
