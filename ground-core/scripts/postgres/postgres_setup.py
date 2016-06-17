import os, sys

assert (len(sys.argv) == 3)
user = sys.argv[1]
dbname = sys.argv[2]


delete_string = "psql -U " + str(user) + " -d " + str(dbname) + " -f drop_postgres.sql"
create_string = "psql -U " + str(user) + " -d " + str(dbname) + " -f postgres.sql"

os.system(delete_string)
os.system(create_string)
