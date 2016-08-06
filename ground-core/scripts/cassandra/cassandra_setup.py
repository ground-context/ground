import sys, os

assert (len(sys.argv) == 2)
dbname = sys.argv[1]

command_string = "cqlsh -k " + str(dbname) + " -f drop_cassandra.sql"
os.system(command_string)

command_string = "cqlsh -k " + str(dbname) + " -f cassandra.sql"
os.system(command_string)
