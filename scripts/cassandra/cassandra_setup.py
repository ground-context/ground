from cassandra.cluster import Cluster
import sys

assert (len(sys.argv) == 2)
dbname = sys.argv[1]

cluster = Cluster()
session = cluster.connect(dbname)

print ("Connection succeeded. Dropping tables...")

try:
    session.execute("drop table lineageedgeversions;")
    session.execute("drop table lineageedges;")
    session.execute("drop table principals;")
    session.execute("drop table workflows;")
    session.execute("drop table graphversionedges;")
    session.execute("drop table graphversions;")
    session.execute("drop table edgeversions;")
    session.execute("drop table nodeversions;")
    session.execute("drop table graphs;")
    session.execute("drop table nodes;")
    session.execute("drop table edges;")
    session.execute("drop table tags;")
    session.execute("drop table richversionexternalparameters")
    session.execute("drop table richversions;")
    session.execute("drop table structureversionitems;")
    session.execute("drop table structureversions;")
    session.execute("drop table structures;")
    session.execute("drop table versionhistorydags;")
    session.execute("drop table items;")
    session.execute("drop table versionsuccessors;")
    session.execute("drop table versions;")
except:
    print("Dropping tables failed.")

print "Creating tables..."

statements = open("cassandra.sql", 'r').read()

for statement in statements.split(';'):
    if statement.strip() != '':
        session.execute(statement + ';')

print "Table creation succeeded."
