/* VERSIONS */

create table Versions (
    id varchar not null primary key
);

create table VersionSuccessors (
    successor_id varchar not null primary key,
    vfrom varchar not null references Versions(id),
    vto varchar not null references Versions(id),
    unique (vfrom, vto)
);

create table Items (
    id varchar not null primary key
);

create table VersionHistoryDAGs (
    item_id varchar not null references Items(id),
    successor_id varchar not null references VersionSuccessors(successor_id),
    primary key(item_id, successor_id)
);


/* MODELS */
create type DataType as enum ('integer', 'string', 'boolean');


create table Structures (
    item_id varchar not null primary key references Items(id),
    name varchar not null unique
);

create table StructureVersions (
    id varchar not null primary key references Versions(id),
    structure_id varchar not null references Structures(item_id)
);

create table StructureVersionItems (
    svid varchar not null references StructureVersions(id),
    key varchar not null,
    type varchar not null,
    primary key(svid, key)
);

create table RichVersions (
    id varchar not null primary key references Versions(id),
    structure_id varchar references StructureVersions(id),
    reference varchar
);

create table RichVersionExternalParameters (
    richversion_id varchar not null references RichVersions(id),
    key varchar not null,
    value varchar not null,
    primary key (richversion_id, key)
);

create table Tags (
    richversion_id varchar references RichVersions(id),
    key varchar not null,
    value varchar,
    type DataType,
    primary key(richversion_id, key)
);

create table Edges (
    item_id varchar not null primary key references Items(id),
    name varchar not null unique
);

create table Nodes (
    item_id varchar not null primary key references Items(id),
    name varchar not null unique
);

create table Graphs (
    item_id varchar not null primary key references Items(id),
    name varchar not null unique
);

create table NodeVersions (
    id varchar not null primary key references RichVersions(id),
    node_id varchar not null references Nodes(item_id)
);

create table EdgeVersions (
    id varchar not null primary key references RichVersions(id),
    edge_id varchar not null references Edges(item_id),
    endpoint_one varchar not null references NodeVersions(id),
    endpoint_two varchar not null references NodeVersions(id)
);

create table GraphVersions (
    id varchar not null primary key references RichVersions(id),
    graph_id varchar not null references Graphs(item_id)
);

create table GraphVersionEdges (
    gvid varchar not null references GraphVersions(id),
    evid varchar not null references EdgeVersions(id),
    primary key(gvid, evid)
);

/* USAGE */

create table Workflows (
    graph_id varchar not null primary key references Graphs(item_id),
    name varchar not null unique references Graphs(name)
);

create table Principals (
    node_id varchar not null primary key references Nodes(item_id),
    name varchar not null unique references Nodes(name)
);

create table LineageEdges (
    item_id varchar not null primary key references Items(id),
    name varchar not null unique
);

create table LineageEdgeVersions (
    id varchar not null primary key references RichVersions(id),
    lineageedge_id varchar not null references LineageEdges(item_id),
    endpoint_one varchar not null references RichVersions(id),
    endpoint_two varchar not null references RichVersions(id),
    workflow_id varchar references GraphVersions(id),
    principal_id varchar references NodeVersions(id)
);

/* CREATE EMPTY VERSION */

insert into Versions(id) values ('EMPTY');

/* CREATE FUNCTION FOR ITERATION */
create function reachable(vstart varchar) returns table(dest varchar) as $$ 
  declare cnt integer;
  declare prev_cnt integer;

  begin
    create temp table paths as select endpoint_one, endpoint_two from edgeversions where endpoint_one = vstart;
    select into cnt count(*) from paths;

    while prev_cnt != cnt loop
      create temp table new_paths as select endpoint_two, endpoint_two from paths, edgeversions where endpoint_two = endpoint_one;
      alter table paths rename to old_paths;
      create table paths as select * from new_paths union select * from old_paths;
    end loop;
    return query select endpoint_two from paths;
  end;
$$ language plpgsql;

