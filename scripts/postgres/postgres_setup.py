import psycopg2
import sys

assert (len(sys.argv) == 2)
dbname = sys.argv[1]

conn = psycopg2.connect("dbname=%s user=ground password=metadata" % (dbname))
conn.autocommit = True
cursor = conn.cursor()
print ("Connection succeeded. Dropping tables...")

try:
    cursor.execute("drop table lineageedgeversions;")
    cursor.execute("drop table lineageedges;")
    cursor.execute("drop table principals;")
    cursor.execute("drop table workflows;")
    cursor.execute("drop table graphversionedges;")
    cursor.execute("drop table graphversions;")
    cursor.execute("drop table edgeversions;")
    cursor.execute("drop table nodeversions;")
    cursor.execute("drop table graphs;")
    cursor.execute("drop table nodes;")
    cursor.execute("drop table edges;")
    cursor.execute("drop table tags;")
    cursor.execute("drop table richversionexternalparameters")
    cursor.execute("drop table richversions;")
    cursor.execute("drop table structureversionitems;")
    cursor.execute("drop table structureversions;")
    cursor.execute("drop table structures;")
    cursor.execute("drop type datatype;")
    cursor.execute("drop table versionhistorydags;")
    cursor.execute("drop table items;")
    cursor.execute("drop table versionsuccessors;")
    cursor.execute("drop table versions;")
except:
    print "Dropping tables failed."

print "Creating tables..."

cursor.execute(open("postgres.sql", 'r').read())

print "Table creation succeeded."
