import os

os.system("cqlsh -k titan -e 'drop keyspace titan;'")
os.system(os.environ['TITAN_HOME'] + "/bin/gremlin.sh set_schema.groovy")
